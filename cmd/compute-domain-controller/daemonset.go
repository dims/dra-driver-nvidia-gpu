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
	"bytes"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"text/template"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/featuregates"
)

var DaemonSetTemplatePath = "/templates/compute-domain-daemon.tmpl.yaml"

type DaemonSetTemplateData struct {
	Namespace                 string
	Name                      string
	Finalizer                 string
	ComputeDomainLabelKey     string
	ComputeDomainLabelValue   types.UID
	ResourceClaimTemplateName string
	ImageName                 string
	MaxNodesPerIMEXDomain     int
	FeatureGates              map[string]bool
	LogVerbosity              int
	ImagePullSecretNames      []string
	ServiceAccountName        string
	Protocol                  nvapi.ComputeDomainCliqueProtocol
}

type DaemonSetManager struct {
	sync.Mutex

	config           *ManagerConfig
	waitGroup        sync.WaitGroup
	cancelContext    context.CancelFunc
	getComputeDomain GetComputeDomainFunc

	factory       informers.SharedInformerFactory
	informer      cache.SharedIndexInformer
	mutationCache cache.MutationCache

	resourceClaimTemplateManager *DaemonSetResourceClaimTemplateManager
	cdStatusManager              *ComputeDomainStatusManager
	cleanupManager               *CleanupManager[*appsv1.DaemonSet]
}

func NewDaemonSetManager(config *ManagerConfig, getComputeDomain GetComputeDomainFunc, listComputeDomains ListComputeDomainsFunc, updateComputeDomainStatus UpdateComputeDomainStatusFunc) *DaemonSetManager {
	labelSelector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      computeDomainLabelKey,
				Operator: metav1.LabelSelectorOpExists,
			},
		},
	}

	factory := informers.NewSharedInformerFactoryWithOptions(
		config.clientsets.Core,
		informerResyncPeriod,
		informers.WithNamespace(config.driverNamespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = metav1.FormatLabelSelector(labelSelector)
		}),
	)

	informer := factory.Apps().V1().DaemonSets().Informer()

	m := &DaemonSetManager{
		config:           config,
		getComputeDomain: getComputeDomain,
		factory:          factory,
		informer:         informer,
	}
	m.resourceClaimTemplateManager = NewDaemonSetResourceClaimTemplateManager(config, getComputeDomain)

	// Create ComputeDomainStatusManager to sync node info to CD status
	// - When feature gate ON: syncs from CDCliques + non-fabric-attached pods
	// - When feature gate OFF: syncs from non-fabric-attached pods + handles deletions
	m.cdStatusManager = NewComputeDomainStatusManager(config, listComputeDomains, updateComputeDomainStatus)

	m.cleanupManager = NewCleanupManager[*appsv1.DaemonSet](informer, getComputeDomain, m.cleanup)

	return m
}

func (m *DaemonSetManager) Start(ctx context.Context) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel

	defer func() {
		if rerr != nil {
			if err := m.Stop(); err != nil {
				klog.Errorf("error stopping DaemonSet manager: %v", err)
			}
		}
	}()

	if err := addComputeDomainLabelIndexer[*appsv1.DaemonSet](m.informer); err != nil {
		return fmt.Errorf("error adding indexer for MultiNodeEnvironment label: %w", err)
	}

	m.mutationCache = cache.NewIntegerResourceVersionMutationCache(
		klog.Background(),
		m.informer.GetStore(),
		m.informer.GetIndexer(),
		mutationCacheTTL,
		true,
	)

	_, err := m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.config.workQueue.Enqueue(obj, m.onAddOrUpdate)
		},
		UpdateFunc: func(objOld, objNew any) {
			m.config.workQueue.Enqueue(objNew, m.onAddOrUpdate)
		},
	})
	if err != nil {
		return fmt.Errorf("error adding event handlers for DaemonSet informer: %w", err)
	}

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.factory.Start(ctx.Done())
	}()

	if !cache.WaitForCacheSync(ctx.Done(), m.informer.HasSynced) {
		return fmt.Errorf("informer cache sync for DaemonSet failed")
	}

	if err := m.resourceClaimTemplateManager.Start(ctx); err != nil {
		return fmt.Errorf("error starting ResourceClaimTemplate manager: %w", err)
	}

	if err := m.cdStatusManager.Start(ctx); err != nil {
		return fmt.Errorf("error starting ComputeDomain status manager: %w", err)
	}

	if err := m.cleanupManager.Start(ctx); err != nil {
		return fmt.Errorf("error starting cleanup manager: %w", err)
	}

	return nil
}

func (m *DaemonSetManager) Stop() error {
	if err := m.cdStatusManager.Stop(); err != nil {
		klog.Errorf("error stopping ComputeDomain status manager: %v", err)
	}
	if err := m.resourceClaimTemplateManager.Stop(); err != nil {
		return fmt.Errorf("error stopping ResourceClaimTemplate manager: %w", err)
	}
	if m.cancelContext != nil {
		m.cancelContext()
	}
	m.waitGroup.Wait()
	return nil
}

func (m *DaemonSetManager) Create(ctx context.Context, cd *nvapi.ComputeDomain) (*appsv1.DaemonSet, error) {
	ds, err := getByComputeDomainUID[*appsv1.DaemonSet](ctx, m.mutationCache, string(cd.UID))
	if err != nil {
		return nil, fmt.Errorf("error retrieving DaemonSet: %w", err)
	}
	if len(ds) > 1 {
		return nil, fmt.Errorf("more than one DaemonSet found with same ComputeDomain UID")
	}
	if len(ds) == 1 {
		if _, err := m.resourceClaimTemplateManager.Create(ctx, cd); err != nil {
			return nil, fmt.Errorf("validate existing daemon ResourceClaimTemplate: %w", err)
		}
		if err := validateExistingDaemonSet(ds[0], cd, m.config); err != nil {
			return nil, err
		}
		return ds[0], nil
	}

	rct, err := m.resourceClaimTemplateManager.Create(ctx, cd)
	if err != nil {
		return nil, fmt.Errorf("error creating ResourceClaimTemplate: %w", err)
	}

	daemonSet, err := expectedDaemonSet(cd, rct.Name, m.config)
	if err != nil {
		return nil, err
	}

	d, err := m.config.clientsets.Core.AppsV1().DaemonSets(daemonSet.Namespace).Create(ctx, daemonSet, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("error creating DaemonSet: %w", err)
	}

	// Add the newly created DaemonSet to the mutation cache
	// This ensures subsequent calls will see it immediately
	m.mutationCache.Mutation(d)

	return d, nil
}

func validateExistingDaemonSet(ds *appsv1.DaemonSet, cd *nvapi.ComputeDomain, config *ManagerConfig) error {
	expected, err := expectedDaemonSet(cd, fmt.Sprintf("computedomain-daemon-%s", cd.UID), config)
	if err != nil {
		return err
	}
	// Existing per-CD DaemonSets are intentionally not rewritten on a controller
	// upgrade. Preserve that brownfield behavior for rollout-only fields while
	// comparing every identity/protocol field, probe, command, claim, and
	// downward-API binding exactly. In particular, ControllerOwnedCDCliques is
	// an admission-default gate and may be disabled while a persisted
	// controller-v1 domain continues running.
	normalizeMutableDaemonRolloutFields(expected, ds)
	if ds.Namespace != expected.Namespace || ds.Name != expected.Name ||
		!slices.Equal(ds.Finalizers, expected.Finalizers) ||
		!reflect.DeepEqual(ds.Labels, expected.Labels) ||
		!reflect.DeepEqual(ds.Spec.Selector, expected.Spec.Selector) ||
		!reflect.DeepEqual(ds.Spec.Template, expected.Spec.Template) {
		return fmt.Errorf("refusing to adopt DaemonSet %s/%s because its canonical Pod template or identity does not match ComputeDomain UID %q", ds.Namespace, ds.Name, cd.UID)
	}
	return nil
}

func normalizeMutableDaemonRolloutFields(expected, existing *appsv1.DaemonSet) {
	expected.Spec.Template.Spec.ImagePullSecrets = slices.Clone(existing.Spec.Template.Spec.ImagePullSecrets)
	if len(expected.Spec.Template.Spec.Containers) != 1 || len(existing.Spec.Template.Spec.Containers) != 1 {
		return
	}
	expectedContainer := &expected.Spec.Template.Spec.Containers[0]
	existingContainer := &existing.Spec.Template.Spec.Containers[0]
	expectedContainer.Image = existingContainer.Image
	expectedContainer.ImagePullPolicy = existingContainer.ImagePullPolicy
	if len(expectedContainer.Env) != len(existingContainer.Env) {
		return
	}
	for i := range expectedContainer.Env {
		if expectedContainer.Env[i].Name != existingContainer.Env[i].Name {
			continue
		}
		switch expectedContainer.Env[i].Name {
		case "FEATURE_GATES", "LOG_VERBOSITY":
			expectedContainer.Env[i] = existingContainer.Env[i]
		}
	}
}

func expectedDaemonSet(cd *nvapi.ComputeDomain, resourceClaimTemplateName string, config *ManagerConfig) (*appsv1.DaemonSet, error) {
	protocol, err := computeDomainCliqueProtocol(cd)
	if err != nil {
		return nil, fmt.Errorf("invalid ComputeDomain clique protocol: %w", err)
	}
	templateData := DaemonSetTemplateData{
		Namespace:                 config.driverNamespace,
		Name:                      fmt.Sprintf("computedomain-daemon-%s", cd.UID),
		Finalizer:                 computeDomainFinalizer,
		ComputeDomainLabelKey:     computeDomainLabelKey,
		ComputeDomainLabelValue:   cd.UID,
		ResourceClaimTemplateName: resourceClaimTemplateName,
		ImageName:                 config.imageName,
		MaxNodesPerIMEXDomain:     config.maxNodesPerIMEXDomain,
		FeatureGates:              featuregates.ToMap(),
		LogVerbosity:              config.logVerbosityCDDaemon,
		ImagePullSecretNames:      config.imagePullSecretNames,
		Protocol:                  protocol,
		ServiceAccountName:        "compute-domain-daemon-service-account",
	}
	if protocol == nvapi.ComputeDomainCliqueProtocolControllerV1 {
		templateData.ServiceAccountName = "compute-domain-daemon-reader-service-account"
	}
	tmpl, err := template.ParseFiles(DaemonSetTemplatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DaemonSet template: %w", err)
	}
	var daemonSetYAML bytes.Buffer
	if err := tmpl.Execute(&daemonSetYAML, templateData); err != nil {
		return nil, fmt.Errorf("failed to execute DaemonSet template: %w", err)
	}
	var unstructuredObj unstructured.Unstructured
	if err := yaml.Unmarshal(daemonSetYAML.Bytes(), &unstructuredObj); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DaemonSet YAML: %w", err)
	}
	var daemonSet appsv1.DaemonSet
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.UnstructuredContent(), &daemonSet); err != nil {
		return nil, fmt.Errorf("failed to convert DaemonSet to typed object: %w", err)
	}
	// Informer objects contain defaults applied by the API server. Normalize the
	// locally rendered expected PodTemplate before doing an exact comparison;
	// client-go's external scheme intentionally does not register API-server
	// defaulting functions.
	applyDaemonPodTemplateDefaults(&daemonSet.Spec.Template)
	return &daemonSet, nil
}

func applyDaemonPodTemplateDefaults(template *corev1.PodTemplateSpec) {
	spec := &template.Spec
	if spec.DeprecatedServiceAccount == "" {
		spec.DeprecatedServiceAccount = spec.ServiceAccountName
	}
	if spec.DNSPolicy == "" {
		spec.DNSPolicy = corev1.DNSClusterFirst
	}
	if spec.RestartPolicy == "" {
		spec.RestartPolicy = corev1.RestartPolicyAlways
	}
	if spec.SecurityContext == nil {
		spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	if spec.TerminationGracePeriodSeconds == nil {
		seconds := int64(corev1.DefaultTerminationGracePeriodSeconds)
		spec.TerminationGracePeriodSeconds = &seconds
	}
	if spec.SchedulerName == "" {
		spec.SchedulerName = corev1.DefaultSchedulerName
	}
	for i := range spec.Containers {
		container := &spec.Containers[i]
		if container.ImagePullPolicy == "" {
			container.ImagePullPolicy = corev1.PullIfNotPresent
			lastSlash := strings.LastIndex(container.Image, "/")
			lastColon := strings.LastIndex(container.Image, ":")
			if lastColon > lastSlash && container.Image[lastColon+1:] == "latest" {
				container.ImagePullPolicy = corev1.PullAlways
			}
		}
		if container.TerminationMessagePath == "" {
			container.TerminationMessagePath = corev1.TerminationMessagePathDefault
		}
		if container.TerminationMessagePolicy == "" {
			container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}
		for j := range container.Env {
			if container.Env[j].ValueFrom != nil && container.Env[j].ValueFrom.FieldRef != nil && container.Env[j].ValueFrom.FieldRef.APIVersion == "" {
				container.Env[j].ValueFrom.FieldRef.APIVersion = "v1"
			}
		}
	}
}

func (m *DaemonSetManager) Get(ctx context.Context, cdUID string) (*appsv1.DaemonSet, error) {
	ds, err := getByComputeDomainUID[*appsv1.DaemonSet](ctx, m.mutationCache, cdUID)
	if err != nil {
		return nil, fmt.Errorf("error retrieving DaemonSet: %w", err)
	}
	if len(ds) > 1 {
		return nil, fmt.Errorf("more than one DaemonSet found with same ComputeDomain UID")
	}
	if len(ds) == 0 {
		return nil, nil
	}
	return ds[0], nil
}

func (m *DaemonSetManager) Delete(ctx context.Context, cdUID string) error {
	ds, err := getByComputeDomainUID[*appsv1.DaemonSet](ctx, m.mutationCache, cdUID)
	if err != nil {
		return fmt.Errorf("error retrieving DaemonSet: %w", err)
	}
	if len(ds) > 1 {
		return fmt.Errorf("more than one DaemonSet found with same ComputeDomain UID")
	}
	if len(ds) == 0 {
		return nil
	}

	d := ds[0]

	if err := m.resourceClaimTemplateManager.Delete(ctx, cdUID); err != nil {
		return fmt.Errorf("error deleting ResourceClaimTemplate: %w", err)
	}

	if d.GetDeletionTimestamp() != nil {
		return nil
	}

	err = m.config.clientsets.Core.AppsV1().DaemonSets(d.Namespace).Delete(ctx, d.Name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("erroring deleting DaemonSet: %w", err)
	}

	return nil
}

func (m *DaemonSetManager) RemoveFinalizer(ctx context.Context, cdUID string) error {
	if err := m.resourceClaimTemplateManager.RemoveFinalizer(ctx, cdUID); err != nil {
		return fmt.Errorf("error removing finalizer on ResourceClaimTemplate: %w", err)
	}
	if err := m.removeFinalizer(ctx, cdUID); err != nil {
		return fmt.Errorf("error removing finalizer on DaemonSet: %w", err)
	}
	return nil
}

func (m *DaemonSetManager) AssertRemoved(ctx context.Context, cdUID string) error {
	if err := m.resourceClaimTemplateManager.AssertRemoved(ctx, cdUID); err != nil {
		return fmt.Errorf("error asserting ResourceClaimTemplate removed: %w", err)
	}
	if err := m.assertRemoved(ctx, cdUID); err != nil {
		return fmt.Errorf("error asserting DaemonSet removal: %w", err)
	}
	return nil
}

func (m *DaemonSetManager) removeFinalizer(ctx context.Context, cdUID string) error {
	ds, err := getByComputeDomainUID[*appsv1.DaemonSet](ctx, m.mutationCache, cdUID)
	if err != nil {
		return fmt.Errorf("error retrieving DaemonSet: %w", err)
	}
	if len(ds) > 1 {
		return fmt.Errorf("more than one DaemonSet found with same ComputeDomain UID")
	}
	if len(ds) == 0 {
		return nil
	}

	d := ds[0]

	if d.GetDeletionTimestamp() == nil {
		return fmt.Errorf("attempting to remove finalizer before DaemonSet marked for deletion")
	}

	newD := d.DeepCopy()
	newD.Finalizers = []string{}
	for _, f := range d.Finalizers {
		if f != computeDomainFinalizer {
			newD.Finalizers = append(newD.Finalizers, f)
		}
	}
	if len(d.Finalizers) == len(newD.Finalizers) {
		return nil
	}

	if _, err := m.config.clientsets.Core.AppsV1().DaemonSets(d.Namespace).Update(ctx, newD, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("error updating DaemonSet: %w", err)
	}

	// Update mutation cache after successful update
	m.mutationCache.Mutation(newD)

	return nil
}

func (m *DaemonSetManager) assertRemoved(ctx context.Context, cdUID string) error {
	ds, err := getByComputeDomainUID[*appsv1.DaemonSet](ctx, m.informer.GetIndexer(), cdUID)
	if err != nil {
		return fmt.Errorf("error retrieving DaemonSet: %w", err)
	}
	if len(ds) != 0 {
		return fmt.Errorf("still exists")
	}
	return nil
}

func (m *DaemonSetManager) onAddOrUpdate(ctx context.Context, obj any) error {
	d, ok := obj.(*appsv1.DaemonSet)
	if !ok {
		return fmt.Errorf("failed to cast to DaemonSet")
	}

	klog.V(2).Infof("Processing added or updated DaemonSet: %s/%s", d.Namespace, d.Name)

	cd, err := m.getComputeDomain(d.Labels[computeDomainLabelKey])
	if err != nil {
		return fmt.Errorf("error getting ComputeDomain: %w", err)
	}
	if cd == nil {
		return nil
	}

	if int(d.Status.NumberReady) != cd.Spec.NumNodes {
		return nil
	}

	newCD := cd.DeepCopy()
	newCD.Status.Status = nvapi.ComputeDomainStatusReady
	if _, err = m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomains(newCD.Namespace).UpdateStatus(ctx, newCD, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("error updating nodes in ComputeDomain status: %w", err)
	}

	return nil
}

func (m *DaemonSetManager) cleanup(ctx context.Context, cdUID string) error {
	if err := m.Delete(ctx, cdUID); err != nil {
		return fmt.Errorf("error deleting DaemonSet: %w", err)
	}
	if err := m.RemoveFinalizer(ctx, cdUID); err != nil {
		return fmt.Errorf("error removing DaemonSet finalizer: %w", err)
	}
	return nil
}
