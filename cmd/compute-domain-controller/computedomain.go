/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/featuregates"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/metrics"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
	nvlisters "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/listers/resource/v1beta1"
)

type GetComputeDomainFunc func(uid string) (*nvapi.ComputeDomain, error)
type ListComputeDomainsFunc func() ([]*nvapi.ComputeDomain, error)
type UpdateComputeDomainStatusFunc func(ctx context.Context, cd *nvapi.ComputeDomain) (*nvapi.ComputeDomain, error)

const (
	// informerResyncPeriod defines how often the informer will resync its cache
	// with the API server. This helps ensure eventual consistency.
	informerResyncPeriod = 10 * time.Minute

	// mutationCacheTTL defines how long mutation cache entries remain valid.
	// This should be long enough for the informer cache to catch up but
	// not so long that stale entries cause issues.
	mutationCacheTTL = time.Hour

	computeDomainLabelKey           = "resource.nvidia.com/computeDomain"
	computeDomainCliqueLabelKey     = "resource.nvidia.com/computeDomain.cliqueID"
	computeDomainFinalizer          = computeDomainLabelKey
	computeDomainProtocolAnnotation = nvapi.ComputeDomainCliqueProtocolAnnotation

	computeDomainDefaultChannelDeviceClass = "compute-domain-default-channel.nvidia.com"
	computeDomainChannelDeviceClass        = "compute-domain-channel.nvidia.com"
	computeDomainDaemonDeviceClass         = "compute-domain-daemon.nvidia.com"

	computeDomainResourceClaimTemplateTargetLabelKey = "resource.nvidia.com/computeDomainTarget"
	computeDomainResourceClaimTemplateTargetDaemon   = "Daemon"
	computeDomainResourceClaimTemplateTargetWorkload = "Workload"
)

type ComputeDomainManager struct {
	config        *ManagerConfig
	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc

	factory       nvinformers.SharedInformerFactory
	informer      cache.SharedIndexInformer
	lister        nvlisters.ComputeDomainLister
	mutationCache cache.MutationCache

	// daemonSetManager and nodeManager are nil when the driver is configured
	// for host-managed IMEX (see pkg/imex): in that mode the driver never
	// creates DaemonSets or ComputeDomain node labels, so this machinery
	// (including the DaemonSet manager's nested ComputeDomainClique/status
	// tracking) is never constructed or started at all, rather than merely
	// left unused.
	daemonSetManager             *MultiNamespaceDaemonSetManager
	resourceClaimTemplateManager *WorkloadResourceClaimTemplateManager
	nodeManager                  *NodeManager
}

// NewComputeDomainManager creates a new ComputeDomainManager.
func NewComputeDomainManager(config *ManagerConfig) *ComputeDomainManager {
	factory := nvinformers.NewSharedInformerFactory(config.clientsets.Nvidia, informerResyncPeriod)
	informer := factory.Resource().V1beta1().ComputeDomains().Informer()
	lister := nvlisters.NewComputeDomainLister(informer.GetIndexer())

	m := &ComputeDomainManager{
		config:   config,
		factory:  factory,
		informer: informer,
		lister:   lister,
	}

	if !config.imexConfig.EffectiveHostManaged() {
		m.daemonSetManager = NewMultiNamespaceDaemonSetManager(config, m.Get, m.List, m.UpdateStatus)
		m.nodeManager = NewNodeManager(config, m.Get)
	}
	m.resourceClaimTemplateManager = NewWorkloadResourceClaimTemplateManager(config, m.Get)

	return m
}

// Start starts a ComputeDomainManager.
func (m *ComputeDomainManager) Start(ctx context.Context) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel

	defer func() {
		if rerr != nil {
			if err := m.Stop(); err != nil {
				klog.Errorf("error stopping ComputeDomain manager: %v", err)
			}
		}
	}()

	err := m.informer.AddIndexers(cache.Indexers{
		"uid": uidIndexer[*nvapi.ComputeDomain],
	})
	if err != nil {
		return fmt.Errorf("error adding indexer for UIDs: %w", err)
	}

	// Create mutation cache to track ComputeDomain updates
	// This reduces conflicts when multiple managers update the same ComputeDomain concurrently
	m.mutationCache = cache.NewIntegerResourceVersionMutationCache(
		klog.Background(),
		m.informer.GetStore(),
		m.informer.GetIndexer(),
		mutationCacheTTL,
		true,
	)

	_, err = m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.config.workQueue.Enqueue(obj, m.onAddOrUpdate)
		},
		UpdateFunc: func(oldObj, newObj any) {
			m.config.workQueue.Enqueue(newObj, m.onAddOrUpdate)
		},
	})
	if err != nil {
		return fmt.Errorf("error adding event handlers for ComputeDomain informer: %w", err)
	}

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.factory.Start(ctx.Done())
	}()

	if !cache.WaitForCacheSync(ctx.Done(), m.informer.HasSynced) {
		return fmt.Errorf("informer cache sync for ComputeDomains failed")
	}

	if m.daemonSetManager != nil {
		if err := m.daemonSetManager.Start(ctx); err != nil {
			return fmt.Errorf("error starting DaemonSet manager: %w", err)
		}
	}

	if err := m.resourceClaimTemplateManager.Start(ctx); err != nil {
		return fmt.Errorf("error creating ResourceClaim manager: %w", err)
	}

	if m.nodeManager != nil {
		if err := m.nodeManager.Start(ctx); err != nil {
			return fmt.Errorf("error starting Node manager: %w", err)
		}
	}

	return nil
}

func (m *ComputeDomainManager) Stop() error {
	if m.daemonSetManager != nil {
		if err := m.daemonSetManager.Stop(); err != nil {
			return fmt.Errorf("error stopping DaemonSet manager: %w", err)
		}
	}
	if err := m.resourceClaimTemplateManager.Stop(); err != nil {
		return fmt.Errorf("error stopping ResourceClaimTemplate manager: %w", err)
	}
	if m.nodeManager != nil {
		if err := m.nodeManager.Stop(); err != nil {
			return fmt.Errorf("error stopping Node manager: %w", err)
		}
	}
	if m.cancelContext != nil {
		m.cancelContext()
	}
	m.waitGroup.Wait()
	return nil
}

// Get gets a ComputeDomain with a specific UID from the mutation cache.
func (m *ComputeDomainManager) Get(uid string) (*nvapi.ComputeDomain, error) {
	cds, err := m.mutationCache.ByIndex("uid", uid)
	if err != nil {
		return nil, fmt.Errorf("error retrieving ComputeDomain by UID: %w", err)
	}
	if len(cds) == 0 {
		return nil, nil
	}
	if len(cds) != 1 {
		return nil, fmt.Errorf("multiple ComputeDomains with the same UID")
	}
	cd, ok := cds[0].(*nvapi.ComputeDomain)
	if !ok {
		return nil, fmt.Errorf("failed to cast to ComputeDomain")
	}
	return cd, nil
}

// List returns all ComputeDomains from the informer cache.
func (m *ComputeDomainManager) List() ([]*nvapi.ComputeDomain, error) {
	return m.lister.List(labels.Everything())
}

// UpdateStatus updates a ComputeDomain's status and caches the result in the mutation cache.
func (m *ComputeDomainManager) UpdateStatus(ctx context.Context, cd *nvapi.ComputeDomain) (*nvapi.ComputeDomain, error) {
	// Recalculate global status based on current state
	cd.Status.Status = m.calculateGlobalStatus(cd)

	metrics.ObserveComputeDomainStatus(string(cd.UID), cd.Status.Status)

	updatedCD, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomains(cd.Namespace).UpdateStatus(ctx, cd, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	m.mutationCache.Mutation(updatedCD)

	return updatedCD, nil
}

// RemoveFinalizer removes the finalizer from a ComputeDomain.
func (m *ComputeDomainManager) RemoveFinalizer(ctx context.Context, uid string) error {
	cd, err := m.Get(uid)
	if err != nil {
		return fmt.Errorf("error retrieving ComputeDomain: %w", err)
	}
	if cd == nil {
		return nil
	}

	if cd.GetDeletionTimestamp() == nil {
		return fmt.Errorf("attempting to remove finalizer before ComputeDomain marked for deletion")
	}

	newCD := cd.DeepCopy()
	newCD.Finalizers = []string{}
	for _, f := range cd.Finalizers {
		if f != computeDomainFinalizer {
			newCD.Finalizers = append(newCD.Finalizers, f)
		}
	}
	if len(cd.Finalizers) == len(newCD.Finalizers) {
		return nil
	}

	if _, err = m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomains(cd.Namespace).Update(ctx, newCD, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("error updating ComputeDomain: %w", err)
	}

	return nil
}

func (m *ComputeDomainManager) DeleteSnapshots(ctx context.Context, cdUID string) error {
	namespaces := append([]string{m.config.driverNamespace}, m.config.additionalNamespaces...)
	seenNamespaces := make(map[string]struct{}, len(namespaces))
	var snapshots []nvapi.ComputeDomainCliqueSnapshot
	for _, namespace := range namespaces {
		if _, seen := seenNamespaces[namespace]; seen {
			continue
		}
		seenNamespaces[namespace] = struct{}{}
		listed, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels.SelectorFromSet(labels.Set{computeDomainLabelKey: cdUID}).String(),
		})
		if err != nil {
			return err
		}
		snapshots = append(snapshots, listed.Items...)
	}
	// Validate every namespace before deleting any object or global reservation.
	for i := range snapshots {
		snapshot := &snapshots[i]
		if slices.Contains(snapshot.Finalizers, nvapi.ComputeDomainCliqueSnapshotFinalizer) &&
			(snapshot.Status.Generation > 0 || snapshotEverPublished(snapshot)) {
			return fmt.Errorf("snapshot %s/%s retains published index tombstones; verified IMEX fence is required before deletion", snapshot.Namespace, snapshot.Name)
		}
	}
	for i := range snapshots {
		snapshot := &snapshots[i]
		if slices.Contains(snapshot.Finalizers, nvapi.ComputeDomainCliqueSnapshotFinalizer) {
			withoutFence := snapshot.DeepCopy()
			withoutFence.Finalizers = slices.DeleteFunc(withoutFence.Finalizers, func(finalizer string) bool {
				return finalizer == nvapi.ComputeDomainCliqueSnapshotFinalizer
			})
			_, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(snapshot.Namespace).Update(ctx, withoutFence, metav1.UpdateOptions{})
			observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationFinalizerRemove, err)
			if err != nil {
				return err
			}
		}
		err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(snapshot.Namespace).Delete(ctx, snapshot.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &snapshot.UID}})
		observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationDelete, err)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	reservations, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{computeDomainLabelKey: cdUID}).String(),
	})
	if err != nil {
		return err
	}
	for i := range reservations.Items {
		reservation := &reservations.Items[i]
		if reservation.Spec.ComputeDomainUID != types.UID(cdUID) {
			return fmt.Errorf("physical clique reservation %s has mismatched ComputeDomain UID", reservation.Name)
		}
		// Strict v1 has no fence verifier. Retain the cluster-scoped physical
		// reservation even when generation zero never activated: availability is
		// preferable to unsafe cross-stream reuse, and an operator-controlled
		// recovery protocol can add an explicit evidence-bearing transition later.
	}
	return nil
}

// hostManaged reports whether the driver is configured for host-managed IMEX
// (see pkg/imex), in which case the driver never creates per-ComputeDomain
// DaemonSets, daemon ResourceClaimTemplates, or ComputeDomain node labels.
func (m *ComputeDomainManager) hostManaged() bool {
	return m.config.imexConfig.EffectiveHostManaged()
}

func (m *ComputeDomainManager) calculateGlobalStatus(cd *nvapi.ComputeDomain) string {
	// In host-managed IMEX mode the controller does not track per-node daemon
	// readiness (there are no driver-managed daemons), so Ready means only
	// that the ComputeDomain was admitted and its workload
	// ResourceClaimTemplate exists.
	if m.hostManaged() {
		return nvapi.ComputeDomainStatusReady
	}

	// Mark the ComputeDomain as not ready if not enough nodes are present in the nodes list.
	if len(cd.Status.Nodes) < cd.Spec.NumNodes {
		return nvapi.ComputeDomainStatusNotReady
	}

	// If any of the individual nodes is not ready, return NotReady.
	for _, n := range cd.Status.Nodes {
		if n.Status == nvapi.ComputeDomainStatusNotReady {
			return nvapi.ComputeDomainStatusNotReady
		}
	}

	return nvapi.ComputeDomainStatusReady
}

func (m *ComputeDomainManager) updateGlobalStatus(ctx context.Context, cd *nvapi.ComputeDomain) error {
	newCD := cd.DeepCopy()
	newStatus := m.calculateGlobalStatus(newCD)

	if newCD.Status.Status == newStatus {
		return nil
	}

	newCD.Status.Status = newStatus
	if _, err := m.UpdateStatus(ctx, newCD); err != nil {
		return fmt.Errorf("error updating ComputeDomain status: %w", err)
	}
	return nil
}

func (m *ComputeDomainManager) addFinalizer(ctx context.Context, cd *nvapi.ComputeDomain) error {
	newCD := cd.DeepCopy()
	changed := false
	if !slices.Contains(cd.Finalizers, computeDomainFinalizer) && cd.Annotations[computeDomainProtocolAnnotation] != "" {
		return fmt.Errorf("persisted ComputeDomain clique protocol is controller-owned and cannot predate the controller finalizer")
	}
	if newCD.Annotations == nil {
		newCD.Annotations = make(map[string]string)
	}
	if _, exists := newCD.Annotations[computeDomainProtocolAnnotation]; !exists {
		protocol, err := selectComputeDomainCliqueProtocol(
			cd,
			featuregates.Enabled(featuregates.ControllerOwnedCDCliques),
			m.config.controllerOwnedCDCliquesAvailable,
		)
		if err != nil {
			return err
		}
		newCD.Annotations[computeDomainProtocolAnnotation] = string(protocol)
		changed = true
	}
	if !slices.Contains(newCD.Finalizers, computeDomainFinalizer) {
		newCD.Finalizers = append(newCD.Finalizers, computeDomainFinalizer)
		changed = true
	}
	if !changed {
		return nil
	}
	updated, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomains(cd.Namespace).Update(ctx, newCD, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error updating ComputeDomain: %w", err)
	}
	m.mutationCache.Mutation(updated)

	return nil
}

func selectComputeDomainCliqueProtocol(cd *nvapi.ComputeDomain, controllerEnabled, snapshotAPIAvailable bool) (nvapi.ComputeDomainCliqueProtocol, error) {
	requested := nvapi.ComputeDomainCliqueProtocol(cd.Annotations[nvapi.ComputeDomainCliqueRequestedProtocolAnnotation])
	if requested != "" {
		if err := nvapi.ValidateComputeDomainCliqueProtocol(requested); err != nil {
			return "", fmt.Errorf("invalid requested ComputeDomain clique protocol: %w", err)
		}
	}

	// A marker-less object which already has our finalizer predates protocol
	// selection and must remain legacy. Only a newly admitted object can opt
	// into controller-v1.
	if slices.Contains(cd.Finalizers, computeDomainFinalizer) || requested != nvapi.ComputeDomainCliqueProtocolControllerV1 {
		return nvapi.ComputeDomainCliqueProtocolLegacyV1, nil
	}
	if cd.Spec.NumNodes <= 0 {
		return "", fmt.Errorf("controller-v1 requires spec.numNodes to declare the complete expected Node set")
	}
	if !controllerEnabled || !snapshotAPIAvailable {
		return "", fmt.Errorf("controller-v1 was requested but the ControllerOwnedCDCliques feature gate and snapshot API are not both available")
	}
	return nvapi.ComputeDomainCliqueProtocolControllerV1, nil
}

func computeDomainCliqueProtocol(cd *nvapi.ComputeDomain) (nvapi.ComputeDomainCliqueProtocol, error) {
	protocol := nvapi.ComputeDomainCliqueProtocol(cd.Annotations[computeDomainProtocolAnnotation])
	if err := nvapi.ValidateComputeDomainCliqueProtocol(protocol); err != nil {
		return "", err
	}
	return nvapi.EffectiveComputeDomainCliqueProtocol(protocol), nil
}

func (m *ComputeDomainManager) onAddOrUpdate(ctx context.Context, obj any) error {
	cd, ok := obj.(*nvapi.ComputeDomain)
	if !ok {
		return fmt.Errorf("failed to cast to ComputeDomain")
	}

	klog.V(2).Infof("Processing added or updated ComputeDomain: %s/%s/%s", cd.Namespace, cd.Name, cd.UID)

	cd, err := m.Get(string(cd.UID))
	if err != nil {
		return fmt.Errorf("error getting ComputeDomain: %w", err)
	}
	if cd == nil {
		return nil
	}

	// Host-managed IMEX reconciles a ComputeDomain
	// with a much smaller set of objects (no DaemonSet, no node labels)
	// so branch out early
	if m.hostManaged() {
		return m.onAddOrUpdateHostManaged(ctx, cd)
	}
	return m.onAddOrUpdateDriverManaged(ctx, cd)
}

// onAddOrUpdateDriverManaged reconciles a ComputeDomain under the default,
// driver-managed model: the controller owns a per-ComputeDomain DaemonSet,
// its daemon ResourceClaimTemplate, and ComputeDomain node labels, in
// addition to the workload ResourceClaimTemplate.
func (m *ComputeDomainManager) onAddOrUpdateDriverManaged(ctx context.Context, cd *nvapi.ComputeDomain) error {
	if cd.GetDeletionTimestamp() != nil {
		protocol, protocolErr := computeDomainCliqueProtocol(cd)
		if protocolErr != nil {
			return fmt.Errorf("invalid ComputeDomain clique protocol during deletion: %w", protocolErr)
		}
		if err := m.resourceClaimTemplateManager.Delete(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error deleting ResourceClaimTemplate: %w", err)
		}

		if err := m.daemonSetManager.Delete(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error deleting DaemonSet: %w", err)
		}

		if err := m.nodeManager.RemoveComputeDomainLabels(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error removing ComputeDomain node labels: %w", err)
		}

		if err := m.resourceClaimTemplateManager.RemoveFinalizer(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error removing finalizer on ResourceClaimTemplate: %w", err)
		}

		if err := m.resourceClaimTemplateManager.AssertRemoved(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error asserting removal of ResourceClaimTemplate: %w", err)
		}

		if err := m.daemonSetManager.RemoveFinalizer(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error removing finalizer on DaemonSet: %w", err)
		}

		if err := m.daemonSetManager.AssertRemoved(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error asserting removal of DaemonSet: %w", err)
		}

		if protocol == nvapi.ComputeDomainCliqueProtocolControllerV1 {
			if err := m.DeleteSnapshots(ctx, string(cd.UID)); err != nil {
				// Strict v1 policy: Kubernetes object disappearance is not a
				// runtime fence. Preserve tombstones and the ComputeDomain finalizer
				// until a future verified-fence or audited recovery path clears them.
				return fmt.Errorf("controller-owned clique retirement blocked: %w", err)
			}
		}

		if err := m.RemoveFinalizer(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error removing finalizer: %w", err)
		}

		metrics.ForgetComputeDomain(string(cd.UID))
		return nil
	}

	// Add the finalizer.
	if err := m.addFinalizer(ctx, cd); err != nil {
		return fmt.Errorf("error adding finalizer: %w", err)
	}
	// Protocol and finalizer are persisted together. Wait for the informer
	// round-trip before creating objects so every artifact receives exactly the
	// same immutable marker.
	if _, exists := cd.Annotations[computeDomainProtocolAnnotation]; !exists {
		return nil
	}
	protocol, err := computeDomainCliqueProtocol(cd)
	if err != nil {
		return fmt.Errorf("invalid ComputeDomain clique protocol: %w", err)
	}
	if protocol == nvapi.ComputeDomainCliqueProtocolControllerV1 && !m.config.controllerOwnedCDCliquesAvailable {
		return fmt.Errorf("controller-v1 requested but ComputeDomainCliqueSnapshot API is unavailable")
	}

	// Do not wait for the next periodic label cleanup to happen.
	m.nodeManager.RemoveStaleComputeDomainLabelsAsync(ctx)

	// Create the DaemonsetManager.
	if _, err := m.daemonSetManager.Create(ctx, cd); err != nil {
		return fmt.Errorf("error creating DaemonSet: %w", err)
	}

	// Create the ResourceClaimTemplateManager.
	if _, err := m.resourceClaimTemplateManager.Create(ctx, cd.Namespace, cd.Spec.Channel.ResourceClaimTemplate.Name, cd); err != nil {
		return fmt.Errorf("error creating ResourceClaimTemplate '%s/%s': %w", cd.Namespace, cd.Spec.Channel.ResourceClaimTemplate.Name, err)
	}

	// Change the global Status to reflect the number of ComputeDomain daemons connected.
	if err := m.updateGlobalStatus(ctx, cd); err != nil {
		return fmt.Errorf("error updating global status on ComputeDoimain '%s/%s': %w", cd.Namespace, cd.Name, err)
	}

	return nil
}

// onAddOrUpdateHostManaged reconciles a ComputeDomain under host-managed
// IMEX: the cluster admin owns the host nvidia-imex daemon lifecycle, so
// the controller only manages the workload ResourceClaimTemplate and the
// ComputeDomain finalizer.
func (m *ComputeDomainManager) onAddOrUpdateHostManaged(ctx context.Context, cd *nvapi.ComputeDomain) error {
	if cd.GetDeletionTimestamp() != nil {
		if err := m.resourceClaimTemplateManager.Delete(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error deleting ResourceClaimTemplate: %w", err)
		}

		if err := m.resourceClaimTemplateManager.RemoveFinalizer(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error removing finalizer on ResourceClaimTemplate: %w", err)
		}

		if err := m.resourceClaimTemplateManager.AssertRemoved(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error asserting removal of ResourceClaimTemplate: %w", err)
		}

		if err := m.RemoveFinalizer(ctx, string(cd.UID)); err != nil {
			return fmt.Errorf("error removing finalizer: %w", err)
		}

		metrics.ForgetComputeDomain(string(cd.UID))
		return nil
	}

	// Add the finalizer.
	if err := m.addFinalizer(ctx, cd); err != nil {
		return fmt.Errorf("error adding finalizer: %w", err)
	}

	// Create the workload ResourceClaimTemplate. Its manager adds and tracks
	// its own finalizer on the template.
	if _, err := m.resourceClaimTemplateManager.Create(ctx, cd.Namespace, cd.Spec.Channel.ResourceClaimTemplate.Name, cd); err != nil {
		return fmt.Errorf("error creating ResourceClaimTemplate '%s/%s': %w", cd.Namespace, cd.Spec.Channel.ResourceClaimTemplate.Name, err)
	}

	// Mark the ComputeDomain Ready. Under host-managed IMEX this only means
	// the ComputeDomain was admitted and the workload ResourceClaimTemplate
	// exists; it says nothing about host nvidia-imex health.
	if err := m.updateGlobalStatus(ctx, cd); err != nil {
		return fmt.Errorf("error updating global status on ComputeDomain '%s/%s': %w", cd.Namespace, cd.Name, err)
	}

	return nil
}
