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
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/metrics"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
	nvlisters "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/listers/resource/v1beta1"
)

const (
	snapshotQueueName                          = "compute-domain-clique-snapshots"
	snapshotDebounceWindow                     = 500 * time.Millisecond
	snapshotHardDeadline                       = 30 * time.Second
	snapshotWriteBarrierGetDeadline            = 5 * time.Second
	gpuCliqueNodeLabelKey                      = "nvidia.com/gpu.clique"
	computeDomainCliqueStartupAnnotationKey    = "resource.nvidia.com/computeDomainCliqueStartupID"
	computeDomainCliqueCapabilityAnnotationKey = "resource.nvidia.com/computeDomainCliqueProtocolCapability"
	computeDomainUIDIndex                      = "computeDomainUID"
	computeDomainCliqueIndex                   = "computeDomainClique"
	podComputeDomainNodeIndex                  = "computeDomainNode"
	workloadPodNodeIndex                       = "workloadNode"
	workloadPodClaimIndex                      = "workloadClaim"
	workloadPodTemplateIndex                   = "workloadClaimTemplate"
	workloadPodUIDIndex                        = "workloadPodUID"
	computeDomainAttestationAnnotationKey      = "resource.nvidia.com/computeDomainAttestation"
	controllerOwnedCliqueIsolationLabelKey     = "resource.nvidia.com/controllerOwnedComputeDomain"
	reservationActivationTrackingAnnotationKey = "resource.nvidia.com/reservationActivationTracking"
	reservationActivationTrackingStatusV1      = "status-v1"
	physicalCliqueIndex                        = "physicalClique"
)

type snapshotWriteBarrier struct {
	resourceVersion string
	updatedAt       time.Time
}

// ControllerOwnedCliqueManager is the only writer of controller-v1
// assignments and membership snapshots. Its queue key is the snapshot's
// canonical namespace/name, so one noisy clique cannot create duplicate work
// items and multiple workers can still make progress across independent
// cliques.
type ControllerOwnedCliqueManager struct {
	config *ManagerConfig

	workloadCoreFactory     informers.SharedInformerFactory
	namespacedCoreFactory   informers.SharedInformerFactory
	computeDomainFactory    nvinformers.SharedInformerFactory
	nvidiaFactory           nvinformers.SharedInformerFactory
	computeDomainInformer   cache.SharedIndexInformer
	podInformer             cache.SharedIndexInformer
	workloadPodInformer     cache.SharedIndexInformer
	resourceClaimInformer   cache.SharedIndexInformer
	claimTemplateInformer   cache.SharedIndexInformer
	reservationInformer     cache.SharedIndexInformer
	attestationNodeInformer cache.SharedIndexInformer
	nodeInformer            cache.SharedIndexInformer
	daemonSetInformer       cache.SharedIndexInformer
	snapshotInformer        cache.SharedIndexInformer
	nodeLister              corelisters.NodeLister
	attestationNodeLister   corelisters.NodeLister
	resourceClaimLister     resourcelisters.ResourceClaimLister
	claimTemplateLister     resourcelisters.ResourceClaimTemplateLister
	reservationLister       nvlisters.ComputeDomainCliqueReservationLister
	daemonSetLister         appslisters.DaemonSetLister
	nodeStateMu             sync.RWMutex
	nodeStates              map[string]selectedNodeState
	domainNodeSummaries     map[string]selectedNodeSummary
	barrierMu               sync.Mutex
	writeBarriers           map[string]snapshotWriteBarrier
	batchMu                 sync.Mutex
	batchStarted            map[string]time.Time
	batchLastChanged        map[string]time.Time
	batchSignature          map[string]string
	pendingMu               sync.Mutex
	pendingScopes           map[string]snapshotScope
	reservationMu           sync.RWMutex
	validatedReservations   map[string]nvapi.ComputeDomainCliqueReservationSpec
	validatedActivations    map[string]types.UID
	reservationLocksMu      sync.Mutex
	reservationLocks        map[string]*sync.Mutex
	liveAttestationCheck    func(*corev1.Node) bool
	formationWarningsMu     sync.Mutex
	formationWarnings       map[string]struct{}
	formationEventSink      func(context.Context, *nvapi.ComputeDomain, string, string) error

	queue            workqueue.TypedRateLimitingInterface[string]
	attestationQueue workqueue.TypedRateLimitingInterface[string]
	waitGroup        sync.WaitGroup
	cancel           context.CancelFunc
}

type snapshotScope struct {
	computeDomainUID string
	cliqueID         string
}

// selectedNodeState is the small routing and expected-set projection kept for
// each selected Node. Reconciliation still reads the authoritative Node
// objects from the informer index; this projection only prevents every clique
// from repeatedly listing every Node in a multi-clique ComputeDomain.
type selectedNodeState struct {
	computeDomainUID string
	cliqueID         string
	topologyReady    bool
}

type selectedNodeSummary struct {
	selected      int
	topologyReady int
}

func observeCliqueAPIAction(resource, operation string, err error) {
	result := metrics.CliqueAPIResultSuccess
	if apierrors.IsAlreadyExists(err) {
		result = metrics.CliqueAPIResultAlreadyExists
	} else if apierrors.IsNotFound(err) {
		result = metrics.CliqueAPIResultNotFound
	} else if apierrors.IsConflict(err) {
		result = metrics.CliqueAPIResultConflict
	} else if apierrors.IsTooManyRequests(err) {
		result = metrics.CliqueAPIResultThrottled
	} else if apierrors.IsForbidden(err) {
		result = metrics.CliqueAPIResultForbidden
	} else if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || errors.Is(err, context.DeadlineExceeded) {
		result = metrics.CliqueAPIResultTimeout
	} else if err != nil {
		result = metrics.CliqueAPIResultError
	}
	mutated := err == nil && operation != metrics.CliqueAPIOperationGet && operation != metrics.CliqueAPIOperationWriteBarrierGet
	metrics.ObserveCliqueAPIAction(
		string(nvapi.ComputeDomainCliqueProtocolControllerV1),
		resource,
		operation,
		result,
		mutated,
	)
}

func NewControllerOwnedCliqueManager(config *ManagerConfig) *ControllerOwnedCliqueManager {
	selector := &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
		Key: computeDomainLabelKey, Operator: metav1.LabelSelectorOpExists,
	}}}
	// Workload Pods and their claims cannot be filtered by the routing label:
	// those are the facts from which the controller derives authorization.
	workloadCoreFactory := informers.NewSharedInformerFactory(config.clientsets.Core, 0)
	sharedNodeInformer := workloadCoreFactory.Core().V1().Nodes().Informer()
	namespacedCoreFactory := informers.NewSharedInformerFactoryWithOptions(
		config.clientsets.Core,
		informerResyncPeriod,
		informers.WithNamespace(config.driverNamespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = metav1.FormatLabelSelector(selector)
		}),
	)
	nvidiaFactory := nvinformers.NewSharedInformerFactoryWithOptions(
		config.clientsets.Nvidia,
		informerResyncPeriod,
		nvinformers.WithNamespace(config.driverNamespace),
	)
	computeDomainFactory := nvinformers.NewSharedInformerFactory(config.clientsets.Nvidia, informerResyncPeriod)
	resourceClaimInformer := workloadCoreFactory.Resource().V1().ResourceClaims().Informer()
	claimTemplateInformer := workloadCoreFactory.Resource().V1().ResourceClaimTemplates().Informer()
	reservationInformer := computeDomainFactory.Resource().V1beta1().ComputeDomainCliqueReservations().Informer()

	m := &ControllerOwnedCliqueManager{
		config:                  config,
		workloadCoreFactory:     workloadCoreFactory,
		namespacedCoreFactory:   namespacedCoreFactory,
		computeDomainFactory:    computeDomainFactory,
		nvidiaFactory:           nvidiaFactory,
		computeDomainInformer:   computeDomainFactory.Resource().V1beta1().ComputeDomains().Informer(),
		podInformer:             namespacedCoreFactory.Core().V1().Pods().Informer(),
		workloadPodInformer:     workloadCoreFactory.Core().V1().Pods().Informer(),
		resourceClaimInformer:   resourceClaimInformer,
		claimTemplateInformer:   claimTemplateInformer,
		reservationInformer:     reservationInformer,
		attestationNodeInformer: sharedNodeInformer,
		nodeInformer:            sharedNodeInformer,
		daemonSetInformer:       namespacedCoreFactory.Apps().V1().DaemonSets().Informer(),
		snapshotInformer:        nvidiaFactory.Resource().V1beta1().ComputeDomainCliqueSnapshots().Informer(),
		nodeLister:              corelisters.NewNodeLister(sharedNodeInformer.GetIndexer()),
		attestationNodeLister:   workloadCoreFactory.Core().V1().Nodes().Lister(),
		resourceClaimLister:     resourcelisters.NewResourceClaimLister(resourceClaimInformer.GetIndexer()),
		claimTemplateLister:     resourcelisters.NewResourceClaimTemplateLister(claimTemplateInformer.GetIndexer()),
		reservationLister:       nvlisters.NewComputeDomainCliqueReservationLister(reservationInformer.GetIndexer()),
		daemonSetLister:         namespacedCoreFactory.Apps().V1().DaemonSets().Lister(),
		nodeStates:              make(map[string]selectedNodeState),
		domainNodeSummaries:     make(map[string]selectedNodeSummary),
		writeBarriers:           make(map[string]snapshotWriteBarrier),
		batchStarted:            make(map[string]time.Time),
		batchLastChanged:        make(map[string]time.Time),
		batchSignature:          make(map[string]string),
		pendingScopes:           make(map[string]snapshotScope),
		validatedReservations:   make(map[string]nvapi.ComputeDomainCliqueReservationSpec),
		validatedActivations:    make(map[string]types.UID),
		reservationLocks:        make(map[string]*sync.Mutex),
		formationWarnings:       make(map[string]struct{}),
		formationEventSink:      config.formationEventSink,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: snapshotQueueName},
		),
		attestationQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "compute-domain-node-attestations"},
		),
	}
	return m
}

func (m *ControllerOwnedCliqueManager) addInformerIndexes() error {
	if err := m.computeDomainInformer.AddIndexers(cache.Indexers{
		"uid": func(obj any) ([]string, error) {
			cd, ok := obj.(*nvapi.ComputeDomain)
			if !ok {
				return nil, fmt.Errorf("expected ComputeDomain, got %T", obj)
			}
			return []string{string(cd.UID)}, nil
		},
	}); err != nil {
		return fmt.Errorf("adding ComputeDomain UID index: %w", err)
	}
	if err := m.snapshotInformer.AddIndexers(cache.Indexers{
		computeDomainUIDIndex: func(obj any) ([]string, error) {
			snapshot, ok := obj.(*nvapi.ComputeDomainCliqueSnapshot)
			if !ok {
				return nil, fmt.Errorf("expected ComputeDomainCliqueSnapshot, got %T", obj)
			}
			return []string{string(snapshot.Spec.ComputeDomainUID)}, nil
		},
		"cliqueID": func(obj any) ([]string, error) {
			snapshot, ok := obj.(*nvapi.ComputeDomainCliqueSnapshot)
			if !ok {
				return nil, fmt.Errorf("expected ComputeDomainCliqueSnapshot, got %T", obj)
			}
			return []string{snapshot.Spec.CliqueID}, nil
		},
	}); err != nil {
		return fmt.Errorf("adding snapshot ComputeDomain UID index: %w", err)
	}
	if err := m.nodeInformer.AddIndexers(cache.Indexers{
		computeDomainCliqueIndex: nodeComputeDomainCliqueIndexKeys,
	}); err != nil {
		return fmt.Errorf("adding Node clique indexes: %w", err)
	}
	if err := m.podInformer.AddIndexers(cache.Indexers{
		podComputeDomainNodeIndex: podComputeDomainNodeIndexKeys,
	}); err != nil {
		return fmt.Errorf("adding Pod ComputeDomain/Node index: %w", err)
	}
	if err := m.workloadPodInformer.AddIndexers(cache.Indexers{
		workloadPodNodeIndex:     workloadPodNodeIndexKeys,
		workloadPodClaimIndex:    workloadPodClaimIndexKeys,
		workloadPodTemplateIndex: workloadPodTemplateIndexKeys,
		workloadPodUIDIndex:      workloadPodUIDIndexKeys,
	}); err != nil {
		return fmt.Errorf("adding workload Pod attestation indexes: %w", err)
	}
	if err := m.attestationNodeInformer.AddIndexers(cache.Indexers{
		physicalCliqueIndex: func(obj any) ([]string, error) {
			node, ok := obj.(*corev1.Node)
			if !ok {
				return nil, fmt.Errorf("expected Node, got %T", obj)
			}
			keys := []string{}
			if cliqueID := node.Labels[gpuCliqueNodeLabelKey]; cliqueID != "" {
				keys = append(keys, cliqueID)
			}
			if startupCliqueID := node.Annotations[computeDomainCliqueStartupAnnotationKey]; startupCliqueID != "" {
				keys = append(keys, startupCliqueID)
			}
			slices.Sort(keys)
			return slices.Compact(keys), nil
		},
	}); err != nil {
		return fmt.Errorf("adding physical clique Node index: %w", err)
	}
	return nil
}

func (m *ControllerOwnedCliqueManager) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	if err := m.addInformerIndexes(); err != nil {
		return err
	}

	handler := cache.ResourceEventHandlerFuncs{AddFunc: m.enqueueObject, UpdateFunc: func(_, current any) { m.enqueueObject(current) }, DeleteFunc: m.enqueueObject}
	for _, informer := range []cache.SharedIndexInformer{m.podInformer, m.daemonSetInformer, m.snapshotInformer} {
		if _, err := informer.AddEventHandler(handler); err != nil {
			return fmt.Errorf("adding controller-owned clique event handler: %w", err)
		}
	}
	if _, err := m.workloadPodInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: m.enqueueWorkloadPod,
		UpdateFunc: func(previous, current any) {
			m.enqueueWorkloadPod(previous)
			m.enqueueWorkloadPod(current)
		},
		DeleteFunc: m.enqueueWorkloadPod,
	}); err != nil {
		return fmt.Errorf("adding workload Pod attestation event handler: %w", err)
	}
	if _, err := m.resourceClaimInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: m.enqueueResourceClaim,
		UpdateFunc: func(previous, current any) {
			m.enqueueResourceClaim(previous)
			m.enqueueResourceClaim(current)
		},
		DeleteFunc: m.enqueueResourceClaim,
	}); err != nil {
		return fmt.Errorf("adding ResourceClaim attestation event handler: %w", err)
	}
	if _, err := m.claimTemplateInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: m.enqueueClaimTemplate,
		UpdateFunc: func(previous, current any) {
			m.enqueueClaimTemplate(previous)
			m.enqueueClaimTemplate(current)
		},
		DeleteFunc: m.enqueueClaimTemplate,
	}); err != nil {
		return fmt.Errorf("adding ResourceClaimTemplate attestation event handler: %w", err)
	}
	if _, err := m.attestationNodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(current any) { m.enqueueAttestationNodeChange(nil, current) },
		UpdateFunc: m.enqueueAttestationNodeChange,
		DeleteFunc: func(previous any) { m.enqueueAttestationNodeChange(previous, nil) },
	}); err != nil {
		return fmt.Errorf("adding Node attestation event handler: %w", err)
	}
	if _, err := m.nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(current any) { m.handleNodeEvent(nil, current) },
		UpdateFunc: m.handleNodeEvent,
		DeleteFunc: func(previous any) { m.handleNodeEvent(previous, nil) },
	}); err != nil {
		return fmt.Errorf("adding controller-owned clique Node event handler: %w", err)
	}
	if _, err := m.computeDomainInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, current any) {
			if cd, ok := current.(*nvapi.ComputeDomain); ok && cd.DeletionTimestamp != nil {
				m.enqueueExistingForComputeDomain(string(cd.UID))
			}
		},
	}); err != nil {
		return fmt.Errorf("adding ComputeDomain event handler: %w", err)
	}

	m.workloadCoreFactory.Start(ctx.Done())
	m.namespacedCoreFactory.Start(ctx.Done())
	m.computeDomainFactory.Start(ctx.Done())
	m.nvidiaFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), m.podInformer.HasSynced, m.workloadPodInformer.HasSynced,
		m.resourceClaimInformer.HasSynced, m.claimTemplateInformer.HasSynced,
		m.reservationInformer.HasSynced,
		m.attestationNodeInformer.HasSynced,
		m.nodeInformer.HasSynced, m.daemonSetInformer.HasSynced, m.computeDomainInformer.HasSynced, m.snapshotInformer.HasSynced) {
		return fmt.Errorf("controller-owned clique informer cache sync failed")
	}
	// Informer handlers are asynchronous relative to HasSynced. Rebuild this
	// small aggregate from the authoritative cache before workers begin, then
	// let ordinary handlers maintain it incrementally.
	m.rebuildSelectedNodeState()

	for range 4 {
		m.waitGroup.Add(1)
		go func() {
			defer m.waitGroup.Done()
			m.runWorker(ctx)
		}()
	}
	for range 2 {
		m.waitGroup.Add(1)
		go func() {
			defer m.waitGroup.Done()
			m.runAttestationWorker(ctx)
		}()
	}
	return nil
}

func (m *ControllerOwnedCliqueManager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.queue.ShutDown()
	m.attestationQueue.ShutDown()
	m.waitGroup.Wait()
}

func (m *ControllerOwnedCliqueManager) enqueueObject(obj any) {
	switch object := obj.(type) {
	case *corev1.Pod:
		m.enqueuePod(object)
	case cache.DeletedFinalStateUnknown:
		m.enqueueObject(object.Obj)
	case *appsv1.DaemonSet:
		cdUID := object.Labels[computeDomainLabelKey]
		if cdUID != "" {
			m.enqueueExistingForComputeDomain(cdUID)
		}
	case *nvapi.ComputeDomainCliqueSnapshot:
		m.queue.Add(object.Namespace + "/" + object.Name)
	}
}

func nodeComputeDomainCliqueIndexKeys(obj any) ([]string, error) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil, fmt.Errorf("expected Node, got %T", obj)
	}
	uid, cliqueID := node.Labels[computeDomainLabelKey], node.Labels[gpuCliqueNodeLabelKey]
	if uid == "" || cliqueID == "" || !validNodeAttestation(node, nvapi.ComputeDomainCliqueProtocolControllerV1) {
		return nil, nil
	}
	return []string{uid + "\x00" + cliqueID}, nil
}

func podComputeDomainNodeIndexKeys(obj any) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("expected Pod, got %T", obj)
	}
	uid := pod.Labels[computeDomainLabelKey]
	if uid == "" || pod.Spec.NodeName == "" {
		return nil, nil
	}
	return []string{uid + "\x00" + pod.Spec.NodeName}, nil
}

func selectedNodeStateFor(node *corev1.Node) (selectedNodeState, bool) {
	if node == nil {
		return selectedNodeState{}, false
	}
	state := selectedNodeState{
		computeDomainUID: node.Labels[computeDomainLabelKey],
		cliqueID:         node.Labels[gpuCliqueNodeLabelKey],
	}
	if state.computeDomainUID == "" {
		return selectedNodeState{}, false
	}
	if !validNodeAttestation(node, nvapi.ComputeDomainCliqueProtocolControllerV1) {
		return selectedNodeState{}, false
	}
	state.topologyReady = state.cliqueID != "" &&
		node.Annotations[computeDomainCliqueStartupAnnotationKey] == state.cliqueID &&
		node.Annotations[computeDomainCliqueCapabilityAnnotationKey] == string(nvapi.ComputeDomainCliqueProtocolControllerV1)
	return state, true
}

func (m *ControllerOwnedCliqueManager) enqueueNodeState(state selectedNodeState) {
	if state.computeDomainUID == "" || state.cliqueID == "" {
		return
	}
	key := m.config.driverNamespace + "/" + snapshotName(state.computeDomainUID, state.cliqueID)
	m.pendingMu.Lock()
	m.pendingScopes[key] = snapshotScope{computeDomainUID: state.computeDomainUID, cliqueID: state.cliqueID}
	m.pendingMu.Unlock()
	m.queue.Add(key)
}

func addSelectedNodeSummary(summary selectedNodeSummary, state selectedNodeState) selectedNodeSummary {
	summary.selected++
	if state.topologyReady {
		summary.topologyReady++
	}
	return summary
}

func removeSelectedNodeSummary(summary selectedNodeSummary, state selectedNodeState) selectedNodeSummary {
	summary.selected--
	if state.topologyReady {
		summary.topologyReady--
	}
	return summary
}

func expectedSetReadyForSummary(summary selectedNodeSummary, expected int) bool {
	return expected > 0 && summary.selected == expected && summary.topologyReady == expected
}

func (m *ControllerOwnedCliqueManager) expectedNodeCount(cdUID string) (int, bool) {
	objects, err := m.computeDomainInformer.GetIndexer().ByIndex("uid", cdUID)
	if err != nil || len(objects) != 1 {
		return 0, false
	}
	cd, ok := objects[0].(*nvapi.ComputeDomain)
	if !ok || cd.DeletionTimestamp != nil || cd.Spec.NumNodes <= 0 {
		return 0, false
	}
	return cd.Spec.NumNodes, true
}

func (m *ControllerOwnedCliqueManager) handleNodeEvent(previous, current any) {
	unwrap := func(obj any) *corev1.Node {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		node, _ := obj.(*corev1.Node)
		return node
	}
	previousNode, currentNode := unwrap(previous), unwrap(current)
	var name string
	if currentNode != nil {
		name = currentNode.Name
	} else if previousNode != nil {
		name = previousNode.Name
	} else {
		return
	}
	m.nodeStateMu.Lock()
	trackedPrevious, tracked := m.nodeStates[name]
	previousState, previousSelected := selectedNodeStateFor(previousNode)
	if tracked {
		previousState, previousSelected = trackedPrevious, true
	}
	currentState, currentSelected := selectedNodeStateFor(currentNode)
	affected := make(map[string]selectedNodeSummary, 2)
	if previousSelected {
		affected[previousState.computeDomainUID] = m.domainNodeSummaries[previousState.computeDomainUID]
	}
	if currentSelected {
		affected[currentState.computeDomainUID] = m.domainNodeSummaries[currentState.computeDomainUID]
	}
	if tracked {
		summary := removeSelectedNodeSummary(m.domainNodeSummaries[trackedPrevious.computeDomainUID], trackedPrevious)
		if summary.selected == 0 {
			delete(m.domainNodeSummaries, trackedPrevious.computeDomainUID)
		} else {
			m.domainNodeSummaries[trackedPrevious.computeDomainUID] = summary
		}
		delete(m.nodeStates, name)
	}
	if currentSelected {
		m.nodeStates[name] = currentState
		m.domainNodeSummaries[currentState.computeDomainUID] = addSelectedNodeSummary(m.domainNodeSummaries[currentState.computeDomainUID], currentState)
	}
	after := make(map[string]selectedNodeSummary, len(affected))
	for cdUID := range affected {
		after[cdUID] = m.domainNodeSummaries[cdUID]
	}
	m.nodeStateMu.Unlock()

	// Only the old and new clique receive an ordinary Node event. A broadcast
	// to all cliques is necessary only when the domain-wide expected-set barrier
	// transitions, not for every Node annotation/condition update.
	if previousSelected {
		m.enqueueNodeState(previousState)
	}
	if currentSelected {
		m.enqueueNodeState(currentState)
	}
	for cdUID, before := range affected {
		expected, found := m.expectedNodeCount(cdUID)
		if found && expectedSetReadyForSummary(before, expected) != expectedSetReadyForSummary(after[cdUID], expected) {
			m.enqueueExistingForComputeDomain(cdUID)
		}
	}
}

func (m *ControllerOwnedCliqueManager) rebuildSelectedNodeState() {
	m.nodeStateMu.Lock()
	defer m.nodeStateMu.Unlock()
	m.nodeStates = make(map[string]selectedNodeState)
	m.domainNodeSummaries = make(map[string]selectedNodeSummary)
	for _, obj := range m.nodeInformer.GetStore().List() {
		node, ok := obj.(*corev1.Node)
		if !ok {
			continue
		}
		state, selected := selectedNodeStateFor(node)
		if !selected {
			continue
		}
		m.nodeStates[node.Name] = state
		m.domainNodeSummaries[state.computeDomainUID] = addSelectedNodeSummary(m.domainNodeSummaries[state.computeDomainUID], state)
	}
}

func (m *ControllerOwnedCliqueManager) expectedSetReady(cdUID string, expected int) bool {
	m.nodeStateMu.RLock()
	summary := m.domainNodeSummaries[cdUID]
	m.nodeStateMu.RUnlock()
	return expectedSetReadyForSummary(summary, expected)
}

func (m *ControllerOwnedCliqueManager) enqueuePod(pod *corev1.Pod) {
	cdUID := pod.Labels[computeDomainLabelKey]
	if cdUID == "" || pod.Spec.NodeName == "" {
		return
	}
	node, err := m.nodeLister.Get(pod.Spec.NodeName)
	if err != nil {
		return
	}
	cliqueID := node.Labels[gpuCliqueNodeLabelKey]
	if cliqueID == "" {
		return
	}
	key := pod.Namespace + "/" + snapshotName(cdUID, cliqueID)
	m.pendingMu.Lock()
	m.pendingScopes[key] = snapshotScope{computeDomainUID: cdUID, cliqueID: cliqueID}
	m.pendingMu.Unlock()
	m.queue.Add(key)
}

func (m *ControllerOwnedCliqueManager) enqueueExistingForComputeDomain(cdUID string) {
	objects, err := m.snapshotInformer.GetIndexer().ByIndex(computeDomainUIDIndex, cdUID)
	if err != nil {
		return
	}
	for _, object := range objects {
		snapshot, ok := object.(*nvapi.ComputeDomainCliqueSnapshot)
		if !ok {
			continue
		}
		m.queue.Add(snapshot.Namespace + "/" + snapshot.Name)
	}
}

func (m *ControllerOwnedCliqueManager) runWorker(ctx context.Context) {
	for {
		key, shutdown := m.queue.Get()
		if shutdown {
			return
		}
		started := time.Now()
		err := m.reconcile(ctx, key)
		if err != nil {
			metrics.ObserveCliqueReconcile(string(nvapi.ComputeDomainCliqueProtocolControllerV1), "error", time.Since(started))
			klog.Errorf("reconciling controller-owned clique %s: %v", key, err)
			m.queue.AddRateLimited(key)
		} else {
			metrics.ObserveCliqueReconcile(string(nvapi.ComputeDomainCliqueProtocolControllerV1), "success", time.Since(started))
			m.queue.Forget(key)
		}
		m.queue.Done(key)
	}
}

func (m *ControllerOwnedCliqueManager) recordWrite(key, rv string) {
	m.barrierMu.Lock()
	defer m.barrierMu.Unlock()
	m.writeBarriers[key] = snapshotWriteBarrier{resourceVersion: rv, updatedAt: time.Now()}
}

func (m *ControllerOwnedCliqueManager) informerObservedLastWrite(ctx context.Context, key, observedRV string) (bool, *nvapi.ComputeDomainCliqueSnapshot, error) {
	m.barrierMu.Lock()
	barrier, found := m.writeBarriers[key]
	m.barrierMu.Unlock()
	if !found {
		return true, nil, nil
	}
	comparison, err := compareResourceVersion(observedRV, barrier.resourceVersion)
	if err != nil {
		return false, nil, fmt.Errorf("compare snapshot resource versions: %w", err)
	}
	if comparison < 0 {
		// Informer lag is normal, but an unbounded self-wait can permanently
		// strand reconciliation after a watch interruption. After a bounded
		// wait, verify the exact object with a live GET. The GET is per-key and
		// only on this exceptional path, not on every reconciliation.
		if time.Since(barrier.updatedAt) < snapshotWriteBarrierGetDeadline {
			return false, nil, nil
		}
		namespace, name, splitErr := cache.SplitMetaNamespaceKey(key)
		if splitErr != nil {
			return false, nil, splitErr
		}
		current, getErr := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(namespace).Get(ctx, name, metav1.GetOptions{})
		observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationWriteBarrierGet, getErr)
		if getErr != nil {
			return false, nil, fmt.Errorf("verify stalled snapshot write barrier: %w", getErr)
		}
		comparison, err = compareResourceVersion(current.ResourceVersion, barrier.resourceVersion)
		if err != nil {
			return false, nil, fmt.Errorf("compare live snapshot resource versions: %w", err)
		}
		if comparison < 0 {
			return false, nil, nil
		}
		m.barrierMu.Lock()
		delete(m.writeBarriers, key)
		m.barrierMu.Unlock()
		return true, current, nil
	}
	m.barrierMu.Lock()
	delete(m.writeBarriers, key)
	m.barrierMu.Unlock()
	return true, nil, nil
}

// compareResourceVersion compares the apiserver's decimal resource versions
// without converting them to a fixed-width integer. It is intentionally
// equivalent to apimachinery/pkg/util/resourceversion.CompareResourceVersion;
// this repository's vendor tree predates that package despite the pinned
// module version exporting it.
func compareResourceVersion(a, b string) (int, error) {
	wellFormed := func(rv string) bool {
		if rv == "" || rv[0] == '0' {
			return false
		}
		for i := range rv {
			if rv[i] < '0' || rv[i] > '9' {
				return false
			}
		}
		return true
	}
	if !wellFormed(a) || !wellFormed(b) {
		return 0, fmt.Errorf("resource versions must be positive decimal integers: %q, %q", a, b)
	}
	if len(a) < len(b) {
		return -1, nil
	}
	if len(a) > len(b) {
		return 1, nil
	}
	return strings.Compare(a, b), nil
}

func (m *ControllerOwnedCliqueManager) reconcile(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	obj, exists, err := m.snapshotInformer.GetIndexer().GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return m.createSnapshotForPodSet(ctx, namespace, name)
	}
	snapshot, ok := obj.(*nvapi.ComputeDomainCliqueSnapshot)
	if !ok {
		return fmt.Errorf("unexpected snapshot cache object %T", obj)
	}
	if snapshot.Spec.Protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 {
		return fmt.Errorf("snapshot has unsupported protocol %q", snapshot.Spec.Protocol)
	}
	if ready, current, err := m.informerObservedLastWrite(ctx, key, snapshot.ResourceVersion); err != nil {
		return err
	} else if !ready {
		m.queue.AddAfter(key, 50*time.Millisecond)
		return nil
	} else if current != nil {
		snapshot = current
	}
	return m.updateSnapshot(ctx, snapshot)
}

func (m *ControllerOwnedCliqueManager) createSnapshotForPodSet(ctx context.Context, namespace, name string) error {
	m.pendingMu.Lock()
	scope, found := m.pendingScopes[namespace+"/"+name]
	m.pendingMu.Unlock()
	if !found {
		return nil
	}
	cdUID := scope.computeDomainUID
	cliqueID := scope.cliqueID
	// ComputeDomains are namespaced and selected by an informer UID index
	// because the daemon lives in the driver namespace, which may differ from
	// the workload. This avoids a cluster-wide live LIST per new clique.
	objects, err := m.computeDomainInformer.GetIndexer().ByIndex("uid", cdUID)
	if err != nil {
		return err
	}
	if len(objects) != 1 {
		return nil
	}
	owner, ok := objects[0].(*nvapi.ComputeDomain)
	if !ok {
		return fmt.Errorf("unexpected ComputeDomain cache object %T", objects[0])
	}
	if owner == nil || owner.DeletionTimestamp != nil {
		return nil
	}
	protocol, err := computeDomainCliqueProtocol(owner)
	if err != nil || protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 {
		return err
	}
	ds, err := m.daemonSetFor(namespace, owner)
	if err != nil {
		return err
	}
	if ds == nil || ds.DeletionTimestamp != nil {
		return nil
	}
	// Reserve the physical clique before the namespaced snapshot exists. The
	// API-server-atomic singleton closes controller-v1/controller-v1 races. The
	// separate whole-clique isolation boundary prevents legacy formation; the
	// legacy check and this Create are not themselves one atomic transaction.
	// Strict v1 deliberately retains this
	// reservation even if formation never reaches generation one: object
	// absence is not proof that no old runtime acquired the topology.
	if err := m.reservePhysicalClique(ctx, owner, cliqueID, nvapi.ComputeDomainCliqueProtocolControllerV1); err != nil {
		return err
	}
	controller := true
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			Labels: map[string]string{computeDomainLabelKey: cdUID, computeDomainCliqueLabelKey: cliqueID},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "DaemonSet",
				Name: ds.Name, UID: ds.UID, Controller: &controller,
			}},
		},
		Spec: nvapi.ComputeDomainCliqueSnapshotSpec{
			ComputeDomainUID:       owner.UID,
			ComputeDomainName:      owner.Name,
			ComputeDomainNamespace: owner.Namespace,
			CliqueID:               cliqueID,
			DaemonSetName:          ds.Name,
			DaemonSetUID:           ds.UID,
			Capacity:               m.config.maxNodesPerIMEXDomain,
			Protocol:               nvapi.ComputeDomainCliqueProtocolControllerV1,
		},
	}
	created, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(namespace).Create(ctx, snapshot, metav1.CreateOptions{})
	observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationCreate, err)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	if err == nil {
		m.recordWrite(namespace+"/"+name, created.ResourceVersion)
		m.pendingMu.Lock()
		delete(m.pendingScopes, namespace+"/"+name)
		m.pendingMu.Unlock()
	}
	return err
}

func physicalCliqueReservationName(cliqueID string) string {
	digest := sha256.Sum256([]byte(cliqueID))
	return "clique-" + hex.EncodeToString(digest[:])
}

func (m *ControllerOwnedCliqueManager) reservePhysicalClique(ctx context.Context, owner *nvapi.ComputeDomain, cliqueID string, protocol nvapi.ComputeDomainCliqueProtocol) error {
	reservation := &nvapi.ComputeDomainCliqueReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name:        physicalCliqueReservationName(cliqueID),
			Labels:      map[string]string{computeDomainLabelKey: string(owner.UID)},
			Annotations: map[string]string{reservationActivationTrackingAnnotationKey: reservationActivationTrackingStatusV1},
		},
		Spec: nvapi.ComputeDomainCliqueReservationSpec{
			CliqueID:               cliqueID,
			ComputeDomainUID:       owner.UID,
			ComputeDomainName:      owner.Name,
			ComputeDomainNamespace: owner.Namespace,
			Protocol:               protocol,
		},
	}
	if protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 {
		return fmt.Errorf("physical clique reservations are controller-v1 only, got %q", protocol)
	}
	// Serialize only callers for this physical clique. Different cliques keep
	// reconciling concurrently, while the normal first-formation path performs
	// one Create instead of racing into an avoidable AlreadyExists plus GET.
	lock := m.reservationLock(reservation.Name)
	lock.Lock()
	defer lock.Unlock()
	if m.reservationMatchesMemo(reservation.Name, reservation.Spec) {
		return nil
	}
	if m.reservationLister != nil {
		existing, err := m.reservationLister.Get(reservation.Name)
		if err == nil {
			return m.validateAndRememberReservation(existing, reservation.Spec)
		}
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("read cached physical clique reservation %q: %w", reservation.Name, err)
		}
	}
	created, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Create(ctx, reservation, metav1.CreateOptions{})
	observeCliqueAPIAction(metrics.CliqueAPIResourceReservation, metrics.CliqueAPIOperationCreate, err)
	if err == nil {
		return m.validateAndRememberReservation(created, reservation.Spec)
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("reserve physical clique %q: %w", cliqueID, err)
	}
	existing, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(ctx, reservation.Name, metav1.GetOptions{})
	observeCliqueAPIAction(metrics.CliqueAPIResourceReservation, metrics.CliqueAPIOperationGet, err)
	if err != nil {
		return fmt.Errorf("read existing physical clique reservation %q: %w", reservation.Name, err)
	}
	return m.validateAndRememberReservation(existing, reservation.Spec)
}

func (m *ControllerOwnedCliqueManager) reservationLock(name string) *sync.Mutex {
	m.reservationLocksMu.Lock()
	defer m.reservationLocksMu.Unlock()
	lock := m.reservationLocks[name]
	if lock == nil {
		lock = &sync.Mutex{}
		m.reservationLocks[name] = lock
	}
	return lock
}

func (m *ControllerOwnedCliqueManager) reservationMatchesMemo(name string, expected nvapi.ComputeDomainCliqueReservationSpec) bool {
	m.reservationMu.RLock()
	defer m.reservationMu.RUnlock()
	actual, found := m.validatedReservations[name]
	return found && actual == expected
}

func (m *ControllerOwnedCliqueManager) validateAndRememberReservation(reservation *nvapi.ComputeDomainCliqueReservation, expected nvapi.ComputeDomainCliqueReservationSpec) error {
	if reservation.Spec != expected {
		return fmt.Errorf("physical clique %q is still reserved by unfenced ComputeDomain %s/%s UID %q", expected.CliqueID, reservation.Spec.ComputeDomainNamespace, reservation.Spec.ComputeDomainName, reservation.Spec.ComputeDomainUID)
	}
	m.reservationMu.Lock()
	if previous, found := m.validatedReservations[reservation.Name]; found && previous != expected {
		// A successfully released reservation name can be reused by a new
		// ComputeDomain in the same controller process. Never let the former
		// stream's activation memo retain the successor's Node route before the
		// successor has published generation one.
		delete(m.validatedActivations, reservation.Name)
	}
	m.validatedReservations[reservation.Name] = reservation.Spec
	m.reservationMu.Unlock()
	return nil
}

func (m *ControllerOwnedCliqueManager) forgetReleasedReservation(name string) {
	m.reservationMu.Lock()
	delete(m.validatedReservations, name)
	delete(m.validatedActivations, name)
	m.reservationMu.Unlock()
}

func snapshotName(cdUID, cliqueID string) string {
	// Clique IDs may contain dots, UUID separators and other characters which
	// are legal label values but awkward in a DNS-subdomain object name. The
	// bounded digest also keeps names below the Kubernetes 253-byte limit.
	digest := sha256.Sum256([]byte(cliqueID))
	return strings.ToLower(fmt.Sprintf("%s.%s", cdUID, hex.EncodeToString(digest[:8])))
}

func selectedNodesForClique(indexer cache.Indexer, cdUID, cliqueID string) ([]*corev1.Node, error) {
	objects, err := indexer.ByIndex(computeDomainCliqueIndex, cdUID+"\x00"+cliqueID)
	if err != nil {
		return nil, err
	}
	nodes := make([]*corev1.Node, 0, len(objects))
	for _, object := range objects {
		node, ok := object.(*corev1.Node)
		if !ok {
			return nil, fmt.Errorf("unexpected clique Node cache object %T", object)
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func candidatePodsForNodes(indexer cache.Indexer, cdUID string, nodes []*corev1.Node) ([]*corev1.Pod, error) {
	pods := make([]*corev1.Pod, 0, len(nodes))
	for _, node := range nodes {
		objects, err := indexer.ByIndex(podComputeDomainNodeIndex, cdUID+"\x00"+node.Name)
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			pod, ok := object.(*corev1.Pod)
			if !ok {
				return nil, fmt.Errorf("unexpected daemon Pod cache object %T", object)
			}
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

func (m *ControllerOwnedCliqueManager) updateSnapshot(ctx context.Context, snapshot *nvapi.ComputeDomainCliqueSnapshot) error {
	if snapshot.DeletionTimestamp != nil {
		if snapshot.Status.Generation == 0 && !snapshotEverPublished(snapshot) && slices.Contains(snapshot.Finalizers, nvapi.ComputeDomainCliqueSnapshotFinalizer) {
			updated := snapshot.DeepCopy()
			updated.Finalizers = slices.DeleteFunc(updated.Finalizers, func(finalizer string) bool {
				return finalizer == nvapi.ComputeDomainCliqueSnapshotFinalizer
			})
			result, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(snapshot.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
			observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationFinalizerRemove, err)
			if err == nil {
				m.recordWrite(snapshot.Namespace+"/"+snapshot.Name, result.ResourceVersion)
			}
			return err
		}
		// An ever-published allocation remains a tombstone until the retirement
		// state machine verifies exact process-exit receipts and marks it Fenced.
		// Do not remove the finalizer merely because owners disappeared.
		return nil
	}
	// Deletion retirement owns these terminal phases. Normal membership
	// reconciliation must never turn a Retiring/Fenced snapshot back Active.
	if snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhaseRetiring || snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhaseFenced {
		return nil
	}
	if snapshot.Status.Generation > 0 {
		ready, err := m.ensureReservationActivation(ctx, snapshot)
		if err != nil {
			return err
		}
		if !ready {
			return nil
		}
	}
	owners, err := m.computeDomainInformer.GetIndexer().ByIndex("uid", string(snapshot.Spec.ComputeDomainUID))
	if err != nil {
		return err
	}
	if len(owners) != 1 {
		return fmt.Errorf("expected one live ComputeDomain for UID %q, found %d", snapshot.Spec.ComputeDomainUID, len(owners))
	}
	owner, ok := owners[0].(*nvapi.ComputeDomain)
	if !ok {
		return fmt.Errorf("unexpected ComputeDomain cache object %T", owners[0])
	}
	if owner.DeletionTimestamp != nil || owner.Spec.NumNodes <= 0 {
		return fmt.Errorf("controller-v1 ComputeDomain is deleting or lacks a positive expected Node count")
	}
	expectedSetReady := m.expectedSetReady(string(snapshot.Spec.ComputeDomainUID), owner.Spec.NumNodes)
	ds, err := m.daemonSetFor(snapshot.Namespace, owner)
	if err != nil {
		return err
	}
	if ds == nil {
		return nil
	}
	if ds.Name != snapshot.Spec.DaemonSetName || ds.UID != snapshot.Spec.DaemonSetUID || ds.DeletionTimestamp != nil {
		return fmt.Errorf("snapshot DaemonSet identity is absent, deleting, or changed")
	}
	if err := validateExistingSnapshot(snapshot, owner, ds, m.config); err != nil {
		return err
	}

	handoffBlocked := false
	selectedNodes, err := selectedNodesForClique(m.nodeInformer.GetIndexer(), string(snapshot.Spec.ComputeDomainUID), snapshot.Spec.CliqueID)
	if err != nil {
		return err
	}
	pods, err := candidatePodsForNodes(m.podInformer.GetIndexer(), string(snapshot.Spec.ComputeDomainUID), selectedNodes)
	if err != nil {
		return err
	}
	members := make([]nvapi.ComputeDomainCliqueMember, 0, len(pods))
	// Do not durably reserve a partial arrival set. Waiting for the declared
	// domain-wide expected set makes allocation independent of scheduler and
	// informer event order while DaemonSet Pods continue starting in parallel.
	if snapshot.Status.Generation == 0 && !expectedSetReady {
		m.queue.AddAfter(snapshot.Namespace+"/"+snapshot.Name, snapshotDebounceWindow)
		return nil
	}
	assignments, allocationBlocked, err := allocateSelectedNodes(snapshot.Status.Assignments, selectedNodes, snapshot.Spec.Capacity)
	if err != nil {
		return err
	}
	assignmentByNode := make(map[types.UID]int, len(assignments))
	for i := range assignments {
		assignmentByNode[assignments[i].NodeUID] = i
	}
	publishedBootIDs := make(map[string]string, len(snapshot.Status.Members))
	for i := range snapshot.Status.Members {
		member := &snapshot.Status.Members[i]
		publishedBootIDs[string(member.NodeUID)+"\x00"+string(member.PodUID)] = member.NodeBootID
	}
	selectedByName := make(map[string]*corev1.Node, len(selectedNodes))
	selectedUIDs := make(map[types.UID]struct{}, len(selectedNodes))
	for _, node := range selectedNodes {
		selectedByName[node.Name] = node
		selectedUIDs[node.UID] = struct{}{}
	}

	// At most one Pod incarnation may represent one Node. An incumbent that was
	// ever published stays authoritative; overlapping/replacement Pods cannot
	// self-assert a handoff and instead quarantine the slot.
	podsByNode := make(map[string][]*corev1.Pod, len(pods))
	for _, pod := range pods {
		if !eligibleSnapshotPod(pod, ds) {
			continue
		}
		node, selected := selectedByName[pod.Spec.NodeName]
		if !selected || node.Labels[gpuCliqueNodeLabelKey] != snapshot.Spec.CliqueID {
			continue
		}
		podsByNode[node.Name] = append(podsByNode[node.Name], pod)
	}
	for _, node := range selectedNodes {
		if node.Annotations[computeDomainCliqueStartupAnnotationKey] != snapshot.Spec.CliqueID ||
			node.Annotations[computeDomainCliqueCapabilityAnnotationKey] != string(nvapi.ComputeDomainCliqueProtocolControllerV1) {
			handoffBlocked = true
			continue
		}
		assignmentPosition, exists := assignmentByNode[node.UID]
		if !exists {
			continue
		}
		assignment := &assignments[assignmentPosition]
		candidates := podsByNode[node.Name]
		var pod *corev1.Pod
		if assignment.CurrentPodUID != "" {
			for _, candidate := range candidates {
				if candidate.UID == assignment.CurrentPodUID {
					pod = candidate
					break
				}
			}
		}
		if pod == nil && !assignment.EverPublished && len(candidates) == 1 {
			pod = candidates[0]
			assignment.CurrentPodUID = pod.UID
		}
		if pod == nil && assignment.CurrentPodUID == "" && len(candidates) == 1 {
			pod = candidates[0]
			assignment.CurrentPodUID = pod.UID
		}
		if pod == nil {
			if assignment.EverPublished {
				assignment.State = nvapi.ComputeDomainCliqueAssignmentStateQuarantined
				handoffBlocked = true
			} else if len(candidates) == 0 {
				assignment.CurrentPodUID = ""
			}
			continue
		}
		assignment.State = nvapi.ComputeDomainCliqueAssignmentStateBound
		activationBootID := node.Status.NodeInfo.BootID
		if assignment.EverPublished && assignment.CurrentPodUID == pod.UID {
			// NodeBootID is the activation epoch for the exact published Pod,
			// not a projection of the Node's latest boot. A Node reboot may
			// restart a container in-place while Kubernetes retains the Pod
			// UID. Preserve the original epoch so that the restarted daemon or
			// a later replacement can prove that the old runtime was fenced by
			// a real reboot. Updating it here would erase the only durable
			// distinction between NodeReboot evidence and an unsafe same-boot
			// replacement.
			if publishedBootID, found := publishedBootIDs[string(node.UID)+"\x00"+string(pod.UID)]; found {
				activationBootID = publishedBootID
			}
		}
		members = append(members, nvapi.ComputeDomainCliqueMember{
			Index: assignment.Index, NodeName: node.Name, NodeUID: node.UID,
			NodeBootID: activationBootID,
			PodName:    pod.Name, PodUID: pod.UID, PodIP: pod.Status.PodIP,
			DaemonSetUID: ds.UID,
		})
	}
	retainedAssignments := assignments[:0]
	ambiguousPublishedDeparture := false
	for i := range assignments {
		assignment := assignments[i]
		if _, selected := selectedUIDs[assignment.NodeUID]; selected {
			retainedAssignments = append(retainedAssignments, assignment)
			continue
		}
		if assignment.EverPublished {
			assignment.State = nvapi.ComputeDomainCliqueAssignmentStateQuarantined
			ambiguousPublishedDeparture = true
			retainedAssignments = append(retainedAssignments, assignment)
		}
		// A never-published reservation was never consumable by a controller-v1
		// daemon and is therefore safe to reclaim.
	}
	assignments = retainedAssignments

	slices.SortFunc(assignments, func(a, b nvapi.ComputeDomainCliqueAssignment) int { return a.Index - b.Index })
	slices.SortFunc(members, func(a, b nvapi.ComputeDomainCliqueMember) int { return a.Index - b.Index })
	hash, err := snapshotHash(members)
	if err != nil {
		return err
	}
	phase := nvapi.ComputeDomainCliqueSnapshotPhasePending
	// Expected membership is derived from the selected Node set, not from Pods
	// which happened to become visible first. This retains scheduler/image-pull
	// pipelining while preventing a partial snapshot from authorizing IMEX.
	complete := expectedSetReady && len(selectedNodes) > 0 && exactMemberSet(members, selectedUIDs) && !ambiguousPublishedDeparture
	key := snapshot.Namespace + "/" + snapshot.Name
	// Generation zero has authorized nobody. Keep all partial assignments in
	// memory and publish nothing until the exact expected member set is ready;
	// otherwise staggered Pod starts would recreate the old growing-object write
	// pattern (Pending 1, Pending 2, ... Pending N, Active N).
	if snapshot.Status.Generation == 0 && !complete {
		m.queue.AddAfter(key, snapshotDebounceWindow)
		return nil
	}
	if snapshot.Status.Generation == 0 {
		for _, node := range selectedNodes {
			if !m.nodeAttestationIsLive(node) {
				m.attestationQueue.Add(node.Name)
				m.queue.AddAfter(key, snapshotDebounceWindow)
				return nil
			}
		}
	}
	if snapshot.Status.Generation == 0 && !m.batchAllowsInitialWrite(key, selectedNodes, members, complete) {
		return nil
	}
	// Existing/pre-created snapshots must pass through the same cluster-scoped
	// ownership boundary before any status write. This also closes the CRD-first
	// rollout interval if a canonical object was seeded before writer admission.
	if err := m.reservePhysicalClique(ctx, owner, snapshot.Spec.CliqueID, nvapi.ComputeDomainCliqueProtocolControllerV1); err != nil {
		return err
	}
	if complete {
		phase = nvapi.ComputeDomainCliqueSnapshotPhaseActive
	}
	firstActivation := phase == nvapi.ComputeDomainCliqueSnapshotPhaseActive && snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseActive
	if firstActivation && !slices.Contains(snapshot.Finalizers, nvapi.ComputeDomainCliqueSnapshotFinalizer) {
		withFinalizer := snapshot.DeepCopy()
		withFinalizer.Finalizers = append(withFinalizer.Finalizers, nvapi.ComputeDomainCliqueSnapshotFinalizer)
		result, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(snapshot.Namespace).Update(ctx, withFinalizer, metav1.UpdateOptions{})
		observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationFinalizerAdd, err)
		if err == nil {
			m.recordWrite(key, result.ResourceVersion)
		}
		return err
	}

	// Once a complete map has been published, an ambiguous departure freezes
	// the last authorized peer map. The durable assignments/conditions still
	// record quarantine, but no hole or replacement is authorized until a
	// future verifier provides actual fence evidence.
	publishedMembers := members
	publishedHash := hash
	publishedPhase := phase
	if snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhaseActive && !complete {
		publishedMembers = slices.Clone(snapshot.Status.Members)
		publishedHash = snapshot.Status.Hash
		publishedPhase = snapshot.Status.Phase
	} else if phase == nvapi.ComputeDomainCliqueSnapshotPhasePending {
		publishedMembers = nil
		publishedHash = ""
	}
	desiredGeneration := snapshot.Status.Generation
	if publishedPhase == nvapi.ComputeDomainCliqueSnapshotPhaseActive && snapshot.Status.Hash != publishedHash {
		desiredGeneration++
	}
	updated := snapshot.DeepCopy()
	updated.Status.Assignments = assignments
	updated.Status.Members = publishedMembers
	updated.Status.MemberCount = len(publishedMembers)
	updated.Status.Phase = publishedPhase
	updated.Status.Hash = publishedHash
	updated.Status.ObservedGeneration = snapshot.Generation
	updated.Status.Generation = desiredGeneration
	publishedNodes := make(map[types.UID]struct{}, len(members))
	if publishedPhase == nvapi.ComputeDomainCliqueSnapshotPhaseActive && complete {
		for i := range members {
			publishedNodes[members[i].NodeUID] = struct{}{}
		}
	}
	for i := range updated.Status.Assignments {
		if _, found := publishedNodes[updated.Status.Assignments[i].NodeUID]; found {
			updated.Status.Assignments[i].EverPublished = true
			updated.Status.Assignments[i].LastAuthorizedGeneration = updated.Status.Generation
		}
	}
	if snapshot.Status.Generation == 0 && updated.Status.Generation > 0 {
		reservationName := physicalCliqueReservationName(snapshot.Spec.CliqueID)
		reservation, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(ctx, reservationName, metav1.GetOptions{})
		observeCliqueAPIAction(metrics.CliqueAPIResourceReservation, metrics.CliqueAPIOperationGet, err)
		if err != nil {
			return fmt.Errorf("read physical clique reservation before first publication: %w", err)
		}
		if reservation.Spec.ComputeDomainUID != snapshot.Spec.ComputeDomainUID || reservation.Spec.CliqueID != snapshot.Spec.CliqueID {
			return fmt.Errorf("physical clique reservation does not match first publication")
		}
		expectedStatus := nvapi.ComputeDomainCliqueReservationStatus{
			Phase: nvapi.ComputeDomainCliqueReservationPhaseActive, SnapshotUID: snapshot.UID,
			ActivationGeneration: updated.Status.Generation, ActivationHash: updated.Status.Hash,
		}
		if reservation.Status.Phase == "" {
			activated := reservation.DeepCopy()
			activated.Status = expectedStatus
			_, err = m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().UpdateStatus(ctx, activated, metav1.UpdateOptions{})
			observeCliqueAPIAction(metrics.CliqueAPIResourceReservation, metrics.CliqueAPIOperationStatusUpdate, err)
			if err != nil {
				return fmt.Errorf("activate physical clique reservation before first publication: %w", err)
			}
		} else if reservation.Status.Phase != nvapi.ComputeDomainCliqueReservationPhaseActive ||
			reservation.Status.SnapshotUID != snapshot.UID || reservation.Status.ActivationGeneration != updated.Status.Generation ||
			reservation.Status.ActivationHash != updated.Status.Hash || reservation.Status.FencedGeneration != 0 ||
			reservation.Status.FencedHash != "" || reservation.Status.ReleaseReason != "" || reservation.Status.ReleasedAt != nil {
			return fmt.Errorf("physical clique reservation has mismatched activation status")
		}
		m.reservationMu.Lock()
		m.validatedActivations[reservationName] = snapshot.UID
		m.reservationMu.Unlock()
	}
	if allocationBlocked || handoffBlocked {
		reason := "UnfencedIncumbent"
		message := "all indices are bound or quarantined; object absence and elapsed time are not fence evidence"
		if handoffBlocked {
			reason = "PodHandoffUnverified"
			message = "a replacement Pod cannot inherit a published index without verified IMEX handoff evidence"
		}
		apiMeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
			Type:               "IndexReuseBlocked",
			Status:             metav1.ConditionTrue,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: snapshot.Generation,
		})
	} else {
		apiMeta.RemoveStatusCondition(&updated.Status.Conditions, "IndexReuseBlocked")
	}
	if !complete {
		apiMeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
			Type: "SnapshotIncomplete", Status: metav1.ConditionTrue,
			Reason:             "ExpectedMembersNotReady",
			Message:            fmt.Sprintf("waiting for an exact eligible member set: expected=%d observed=%d", len(selectedNodes), len(members)),
			ObservedGeneration: snapshot.Generation,
		})
	} else {
		apiMeta.RemoveStatusCondition(&updated.Status.Conditions, "SnapshotIncomplete")
	}
	if snapshot.Status.Hash == updated.Status.Hash && snapshot.Status.Phase == updated.Status.Phase &&
		snapshot.Status.Generation == updated.Status.Generation &&
		snapshot.Status.MemberCount == updated.Status.MemberCount &&
		snapshot.Status.ObservedGeneration == updated.Status.ObservedGeneration &&
		slices.Equal(snapshot.Status.Assignments, updated.Status.Assignments) &&
		slices.Equal(snapshot.Status.Members, updated.Status.Members) &&
		slices.Equal(snapshot.Status.Conditions, updated.Status.Conditions) {
		return nil
	}
	result, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(snapshot.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationStatusUpdate, err)
	if err == nil {
		m.recordWrite(snapshot.Namespace+"/"+snapshot.Name, result.ResourceVersion)
	}
	return err
}

func (m *ControllerOwnedCliqueManager) ensureReservationActivation(ctx context.Context, snapshot *nvapi.ComputeDomainCliqueSnapshot) (bool, error) {
	reservationName := physicalCliqueReservationName(snapshot.Spec.CliqueID)
	m.reservationMu.RLock()
	remembered := m.validatedActivations[reservationName]
	m.reservationMu.RUnlock()
	if remembered == snapshot.UID {
		return true, nil
	}
	reservation, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(ctx, reservationName, metav1.GetOptions{})
	observeCliqueAPIAction(metrics.CliqueAPIResourceReservation, metrics.CliqueAPIOperationGet, err)
	if err != nil {
		return false, fmt.Errorf("read physical clique reservation activation: %w", err)
	}
	if reservation.Spec.ComputeDomainUID != snapshot.Spec.ComputeDomainUID || reservation.Spec.CliqueID != snapshot.Spec.CliqueID {
		return false, fmt.Errorf("physical clique reservation does not match published snapshot")
	}
	if reservation.Status.Phase == nvapi.ComputeDomainCliqueReservationPhaseActive && reservation.Status.SnapshotUID == snapshot.UID &&
		reservation.Status.ActivationGeneration > 0 && reservation.Status.ActivationGeneration <= snapshot.Status.Generation &&
		len(reservation.Status.ActivationHash) == sha256.Size*2 && reservation.Status.FencedGeneration == 0 &&
		reservation.Status.FencedHash == "" && reservation.Status.ReleaseReason == "" && reservation.Status.ReleasedAt == nil {
		m.reservationMu.Lock()
		m.validatedActivations[reservationName] = snapshot.UID
		m.reservationMu.Unlock()
		return true, nil
	}
	if reservation.Status.Phase != "" {
		return false, fmt.Errorf("physical clique reservation has mismatched activation status")
	}
	updated := reservation.DeepCopy()
	updated.Status = nvapi.ComputeDomainCliqueReservationStatus{
		Phase: nvapi.ComputeDomainCliqueReservationPhaseActive, SnapshotUID: snapshot.UID,
		ActivationGeneration: snapshot.Status.Generation, ActivationHash: snapshot.Status.Hash,
	}
	_, err = m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	observeCliqueAPIAction(metrics.CliqueAPIResourceReservation, metrics.CliqueAPIOperationStatusUpdate, err)
	if err != nil {
		return false, fmt.Errorf("backfill physical clique reservation activation: %w", err)
	}
	m.reservationMu.Lock()
	m.validatedActivations[reservationName] = snapshot.UID
	m.reservationMu.Unlock()
	return false, nil
}

func (m *ControllerOwnedCliqueManager) nodeAttestationIsLive(node *corev1.Node) bool {
	if m.liveAttestationCheck != nil {
		return m.liveAttestationCheck(node)
	}
	return m.liveNodeAttestationAuthorized(node)
}

func snapshotEverPublished(snapshot *nvapi.ComputeDomainCliqueSnapshot) bool {
	for i := range snapshot.Status.Assignments {
		if snapshot.Status.Assignments[i].EverPublished {
			return true
		}
	}
	return false
}

func (m *ControllerOwnedCliqueManager) daemonSetFor(namespace string, owner *nvapi.ComputeDomain) (*appsv1.DaemonSet, error) {
	daemonSets, err := m.daemonSetLister.DaemonSets(namespace).List(labels.SelectorFromSet(labels.Set{computeDomainLabelKey: string(owner.UID)}))
	if err != nil {
		return nil, err
	}
	if len(daemonSets) == 0 {
		return nil, nil
	}
	if len(daemonSets) != 1 {
		return nil, fmt.Errorf("found %d DaemonSets for ComputeDomain %s", len(daemonSets), owner.UID)
	}
	ds := daemonSets[0]
	if err := validateExistingDaemonSet(ds, owner, m.config); err != nil {
		return nil, err
	}
	return ds, nil
}

func validateExistingSnapshot(snapshot *nvapi.ComputeDomainCliqueSnapshot, owner *nvapi.ComputeDomain, daemonSet *appsv1.DaemonSet, config *ManagerConfig) error {
	if snapshot == nil || owner == nil || daemonSet == nil {
		return fmt.Errorf("snapshot, ComputeDomain, and DaemonSet identities are required")
	}
	expectedName := snapshotName(string(owner.UID), snapshot.Spec.CliqueID)
	if snapshot.Namespace != config.driverNamespace || snapshot.Name != expectedName {
		return fmt.Errorf("snapshot has unexpected namespace or canonical name")
	}
	if snapshot.Labels[computeDomainLabelKey] != string(owner.UID) || snapshot.Labels[computeDomainCliqueLabelKey] != snapshot.Spec.CliqueID {
		return fmt.Errorf("snapshot has unexpected scope labels")
	}
	if snapshot.Spec.ComputeDomainUID != owner.UID || snapshot.Spec.ComputeDomainName != owner.Name || snapshot.Spec.ComputeDomainNamespace != owner.Namespace ||
		snapshot.Spec.DaemonSetName != daemonSet.Name || snapshot.Spec.DaemonSetUID != daemonSet.UID ||
		snapshot.Spec.Capacity != config.maxNodesPerIMEXDomain || snapshot.Spec.Protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 {
		return fmt.Errorf("snapshot immutable scope does not match the live ComputeDomain, DaemonSet, or controller configuration")
	}
	controllerRef := metav1.GetControllerOf(snapshot)
	if controllerRef == nil || controllerRef.APIVersion != appsv1.SchemeGroupVersion.String() || controllerRef.Kind != "DaemonSet" ||
		controllerRef.Name != daemonSet.Name || controllerRef.UID != daemonSet.UID {
		return fmt.Errorf("snapshot does not have the expected DaemonSet controller reference")
	}
	return nil
}

func eligibleSnapshotPod(pod *corev1.Pod, daemonSet *appsv1.DaemonSet) bool {
	if pod.Spec.NodeName == "" || pod.Status.PodIP == "" || pod.DeletionTimestamp != nil || !metav1.IsControlledBy(pod, daemonSet) {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	default:
		return true
	}
}

func exactMemberSet(members []nvapi.ComputeDomainCliqueMember, expected map[types.UID]struct{}) bool {
	if len(members) != len(expected) {
		return false
	}
	actual := make(map[types.UID]struct{}, len(members))
	for i := range members {
		if _, duplicate := actual[members[i].NodeUID]; duplicate {
			return false
		}
		actual[members[i].NodeUID] = struct{}{}
	}
	for uid := range expected {
		if _, found := actual[uid]; !found {
			return false
		}
	}
	return true
}

func (m *ControllerOwnedCliqueManager) batchAllowsInitialWrite(key string, nodes []*corev1.Node, members []nvapi.ComputeDomainCliqueMember, complete bool) bool {
	nodeUIDs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		nodeUIDs = append(nodeUIDs, string(node.UID))
	}
	memberUIDs := make([]string, 0, len(members))
	for i := range members {
		memberUIDs = append(memberUIDs, string(members[i].PodUID))
	}
	slices.Sort(nodeUIDs)
	slices.Sort(memberUIDs)
	signature := strings.Join(nodeUIDs, ",") + "|" + strings.Join(memberUIDs, ",")
	now := time.Now()

	m.batchMu.Lock()
	defer m.batchMu.Unlock()
	started := m.batchStarted[key]
	if !started.IsZero() && now.Sub(started) >= snapshotHardDeadline {
		delete(m.batchStarted, key)
		delete(m.batchLastChanged, key)
		delete(m.batchSignature, key)
		return true
	}
	if m.batchSignature[key] != signature {
		m.batchSignature[key] = signature
		m.batchLastChanged[key] = now
		if m.batchStarted[key].IsZero() {
			m.batchStarted[key] = now
		}
		m.queue.AddAfter(key, snapshotDebounceWindow)
		return false
	}
	quietFor := now.Sub(m.batchLastChanged[key])
	batchAge := now.Sub(m.batchStarted[key])
	if quietFor < snapshotDebounceWindow && batchAge < snapshotHardDeadline {
		m.queue.AddAfter(key, min(snapshotDebounceWindow-quietFor, snapshotHardDeadline-batchAge))
		return false
	}
	delete(m.batchStarted, key)
	delete(m.batchLastChanged, key)
	delete(m.batchSignature, key)
	return true
}

// firstFreeIndex is retained as an independently tested primitive. The bulk
// allocator below builds its free-index list once instead of rescanning it for
// each selected Node.
func firstFreeIndex(used map[int]struct{}, capacity int) int {
	for index := range capacity {
		if _, exists := used[index]; !exists {
			return index
		}
	}
	return -1
}

// allocateSelectedNodes is the pure allocation core. All missing Nodes are
// sorted before allocation so event and Pod arrival order cannot affect their
// ordinal mapping. Persisted incumbents always retain their valid indices.
func allocateSelectedNodes(
	existing []nvapi.ComputeDomainCliqueAssignment,
	nodes []*corev1.Node,
	capacity int,
) ([]nvapi.ComputeDomainCliqueAssignment, bool, error) {
	assignments := slices.Clone(existing)
	byNode := make(map[types.UID]struct{}, len(assignments))
	used := make(map[int]struct{}, len(assignments))
	for i := range assignments {
		assignment := assignments[i]
		if assignment.Index < 0 || assignment.Index >= capacity {
			return nil, false, fmt.Errorf("persisted index %d is out of range", assignment.Index)
		}
		if _, duplicate := used[assignment.Index]; duplicate {
			return nil, false, fmt.Errorf("persisted index %d is duplicated", assignment.Index)
		}
		if _, duplicate := byNode[assignment.NodeUID]; duplicate {
			return nil, false, fmt.Errorf("persisted Node UID %s is duplicated", assignment.NodeUID)
		}
		used[assignment.Index] = struct{}{}
		byNode[assignment.NodeUID] = struct{}{}
	}
	ordered := slices.Clone(nodes)
	slices.SortFunc(ordered, func(a, b *corev1.Node) int {
		if uidOrder := cmp.Compare(string(a.UID), string(b.UID)); uidOrder != 0 {
			return uidOrder
		}
		return cmp.Compare(a.Name, b.Name)
	})
	blocked := false
	free := make([]int, 0, capacity-len(used))
	for index := range capacity {
		if _, exists := used[index]; !exists {
			free = append(free, index)
		}
	}
	nextFree := 0
	for _, node := range ordered {
		if _, exists := byNode[node.UID]; exists {
			continue
		}
		if nextFree >= len(free) {
			blocked = true
			continue
		}
		index := free[nextFree]
		nextFree++
		assignments = append(assignments, nvapi.ComputeDomainCliqueAssignment{
			NodeName: node.Name, NodeUID: node.UID, Index: index,
			State: nvapi.ComputeDomainCliqueAssignmentStateBound,
		})
		used[index] = struct{}{}
		byNode[node.UID] = struct{}{}
	}
	slices.SortFunc(assignments, func(a, b nvapi.ComputeDomainCliqueAssignment) int { return a.Index - b.Index })
	return assignments, blocked, nil
}

func snapshotHash(members []nvapi.ComputeDomainCliqueMember) (string, error) {
	canonical, err := json.Marshal(members)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}
