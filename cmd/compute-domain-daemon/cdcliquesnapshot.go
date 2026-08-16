/*
Copyright The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"reflect"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/cdclique"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/metrics"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
)

// ControllerSnapshotDesiredState couples one complete peer map with the exact
// receipt which may be published after that map has been installed and IMEX
// has been started or restarted. Keeping these in one event prevents a consumer
// from acknowledging a different snapshot than the one it just applied.
type ControllerSnapshotDesiredState struct {
	ComputeDomainUID   types.UID
	CliqueID           string
	Protocol           nvapi.ComputeDomainCliqueProtocol
	Members            []*nvapi.ComputeDomainDaemonInfo
	Receipt            *nvapi.ComputeDomainCliqueSnapshotReceipt
	RetirementEvidence *nvapi.ComputeDomainCliqueRetirementEvidenceSpec
	daemonSetName      string
	daemonSetUID       types.UID
}

type controllerSnapshotIdentity struct {
	uid        types.UID
	generation int64
	hash       string
	retiring   bool
}

func (s *ControllerSnapshotDesiredState) identity() controllerSnapshotIdentity {
	if s.RetirementEvidence != nil {
		return controllerSnapshotIdentity{
			uid: s.RetirementEvidence.SnapshotUID, generation: s.RetirementEvidence.SnapshotGeneration,
			hash: s.RetirementEvidence.SnapshotHash, retiring: true,
		}
	}
	return controllerSnapshotIdentity{
		uid:        s.Receipt.SnapshotUID,
		generation: s.Receipt.SnapshotGeneration,
		hash:       s.Receipt.SnapshotHash,
	}
}

// ComputeDomainCliqueSnapshotManager is the read-only controller-v1 daemon
// path. It validates that a committed snapshot authorizes this exact Node, Pod
// and IP before exposing the complete peer map to IMEX.
type ComputeDomainCliqueSnapshotManager struct {
	config    *ManagerConfig
	factory   nvinformers.SharedInformerFactory
	informer  cache.SharedIndexInformer
	waitGroup sync.WaitGroup
	cancel    context.CancelFunc
	ctx       context.Context

	mu                 sync.Mutex
	currentSnapshotUID types.UID
	currentGeneration  int64
	currentHash        string
	desired            controllerSnapshotIdentity
	applied            controllerSnapshotIdentity
	retired            controllerSnapshotIdentity
	retirementStarted  bool
	desiredStateChan   chan *ControllerSnapshotDesiredState
}

func NewComputeDomainCliqueSnapshotManager(config *ManagerConfig) *ComputeDomainCliqueSnapshotManager {
	options := []nvinformers.SharedInformerOption{nvinformers.WithNamespace(config.podNamespace)}
	if config.protocol == nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 {
		options = append(options, nvinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labels.SelectorFromSet(labels.Set{
				computeDomainCliqueLabelKey:                       config.cliqueID,
				"resource.nvidia.com/computeDomainCliqueProtocol": string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1),
			}).String()
		}))
	} else if config.cliqueID != "" {
		name := cdclique.SnapshotName(config.computeDomainUUID, config.cliqueID)
		options = append(options, nvinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = "metadata.name=" + name
		}))
	} else {
		// The no-clique path exists only so an exact daemon from a previously
		// published stream can acknowledge Retiring after local topology loss.
		// Scope that exceptional reader to its immutable ComputeDomain rather
		// than making it watch every snapshot in the driver namespace.
		options = append(options, nvinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = computeDomainLabelKey + "=" + config.computeDomainUUID
		}))
	}
	factory := nvinformers.NewSharedInformerFactoryWithOptions(config.clientsets.Nvidia, informerResyncPeriod, options...)
	return &ComputeDomainCliqueSnapshotManager{
		config:           config,
		factory:          factory,
		informer:         factory.Resource().V1beta1().ComputeDomainCliqueSnapshots().Informer(),
		desiredStateChan: make(chan *ControllerSnapshotDesiredState, 1),
	}
}

func (m *ComputeDomainCliqueSnapshotManager) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.ctx = ctx
	if _, err := m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    m.enqueue,
		UpdateFunc: func(_, current any) { m.enqueue(current) },
		DeleteFunc: m.enqueue,
	}); err != nil {
		return err
	}
	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.factory.Start(ctx.Done())
	}()
	if !cache.WaitForCacheSync(ctx.Done(), m.informer.HasSynced) {
		return fmt.Errorf("informer cache sync for ComputeDomainCliqueSnapshots failed")
	}
	if m.config.protocol == nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 {
		m.reconcilePersistentSnapshots()
	}
	return nil
}

func (m *ComputeDomainCliqueSnapshotManager) Stop() error {
	if m.cancel != nil {
		m.cancel()
	}
	m.waitGroup.Wait()
	return nil
}

func (m *ComputeDomainCliqueSnapshotManager) DesiredStateChan() <-chan *ControllerSnapshotDesiredState {
	return m.desiredStateChan
}

func (m *ComputeDomainCliqueSnapshotManager) HasActiveOrRetiringSnapshot() bool {
	for _, object := range m.informer.GetStore().List() {
		snapshot, ok := object.(*nvapi.ComputeDomainCliqueSnapshot)
		if !ok || snapshot.DeletionTimestamp != nil {
			continue
		}
		switch snapshot.Status.Phase {
		case nvapi.ComputeDomainCliqueSnapshotPhaseActive, nvapi.ComputeDomainCliqueSnapshotPhaseRetiring:
			return true
		}
	}
	return false
}

// MarkApplied records success only after the caller has installed the hosts
// mapping, started or restarted IMEX, observed the resulting process READY,
// and atomically written the local receipt.
// A superseded event is intentionally not marked as the applied desired state.
func (m *ComputeDomainCliqueSnapshotManager) MarkApplied(state *ControllerSnapshotDesiredState) {
	if state == nil || state.Receipt == nil {
		return
	}
	identity := state.identity()
	m.mu.Lock()
	defer m.mu.Unlock()
	if identity == m.desired {
		m.applied = identity
	}
}

func (m *ComputeDomainCliqueSnapshotManager) MarkRetired(state *ControllerSnapshotDesiredState) {
	if state == nil || state.RetirementEvidence == nil {
		return
	}
	identity := state.identity()
	m.mu.Lock()
	defer m.mu.Unlock()
	if identity == m.desired {
		m.retired = identity
	}
}

type podJSONPatchOperation struct {
	Operation string `json:"op"`
	Path      string `json:"path"`
	Value     any    `json:"value,omitempty"`
}

func (m *ComputeDomainCliqueSnapshotManager) WriteAppliedState(ctx context.Context, state *ControllerSnapshotDesiredState) error {
	if state == nil || state.Protocol != nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 || state.Receipt == nil {
		return nil
	}
	value, err := json.Marshal(state.Receipt)
	if err != nil {
		return err
	}
	return m.patchAppliedAnnotation(ctx, string(value))
}

func (m *ComputeDomainCliqueSnapshotManager) ClearAppliedState(ctx context.Context, state *ControllerSnapshotDesiredState) error {
	if state == nil || state.Protocol != nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 {
		return nil
	}
	return m.patchAppliedAnnotation(ctx, "")
}

func (m *ComputeDomainCliqueSnapshotManager) patchAppliedAnnotation(ctx context.Context, value string) error {
	pod, err := m.config.clientsets.Core.CoreV1().Pods(m.config.podNamespace).Get(ctx, m.config.podName, metav1.GetOptions{})
	metrics.ObserveCliqueAPIAction(string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1), metrics.CliqueAPIResourcePod, metrics.CliqueAPIOperationGet, metrics.CliqueAPIResultForError(err), false)
	if err != nil {
		return err
	}
	if string(pod.UID) != m.config.podUID || pod.Spec.NodeName != m.config.nodeName || pod.Labels["resource.nvidia.com/persistentComputeDomainAgent"] != "true" {
		return fmt.Errorf("live Pod identity does not match persistent agent")
	}
	controller := metav1.GetControllerOf(pod)
	if controller == nil || controller.APIVersion != "apps/v1" || controller.Kind != "DaemonSet" || controller.Name != "dra-driver-nvidia-gpu-persistent-agent" || controller.UID == "" {
		return fmt.Errorf("persistent agent Pod owner is invalid")
	}
	oldValue, exists := pod.Annotations[nvapi.ComputeDomainCliqueSnapshotAppliedAnnotation]
	if value == oldValue && (value != "" || !exists) {
		return nil
	}
	operations := []podJSONPatchOperation{
		{Operation: "test", Path: "/metadata/uid", Value: string(pod.UID)},
		{Operation: "test", Path: "/metadata/resourceVersion", Value: pod.ResourceVersion},
	}
	path := "/metadata/annotations/resource.nvidia.com~1computeDomainCliqueSnapshotApplied"
	if value == "" {
		operations = append(operations,
			podJSONPatchOperation{Operation: "test", Path: path, Value: oldValue},
			podJSONPatchOperation{Operation: "remove", Path: path},
		)
	} else if pod.Annotations == nil {
		operations = append(operations, podJSONPatchOperation{Operation: "add", Path: "/metadata/annotations", Value: map[string]string{nvapi.ComputeDomainCliqueSnapshotAppliedAnnotation: value}})
	} else {
		operation := "add"
		if exists {
			operations = append(operations, podJSONPatchOperation{Operation: "test", Path: path, Value: oldValue})
			operation = "replace"
		}
		operations = append(operations, podJSONPatchOperation{Operation: operation, Path: path, Value: value})
	}
	patch, err := json.Marshal(operations)
	if err != nil {
		return err
	}
	_, err = m.config.clientsets.Core.CoreV1().Pods(m.config.podNamespace).Patch(ctx, m.config.podName, types.JSONPatchType, patch, metav1.PatchOptions{})
	metrics.ObserveCliqueAPIAction(string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1), metrics.CliqueAPIResourcePod, metrics.CliqueAPIOperationAppliedStateUpdate, metrics.CliqueAPIResultForError(err), err == nil)
	return err
}

func (m *ComputeDomainCliqueSnapshotManager) PublishRetirementEvidence(ctx context.Context, state *ControllerSnapshotDesiredState) error {
	if state == nil || state.RetirementEvidence == nil {
		return fmt.Errorf("retirement evidence is missing")
	}
	spec := *state.RetirementEvidence
	pod, err := m.config.clientsets.Core.CoreV1().Pods(m.config.podNamespace).Get(ctx, m.config.podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read retirement witness Pod: %w", err)
	}
	if string(pod.UID) != m.config.podUID || pod.UID != spec.WitnessPodUID || pod.Spec.NodeName != m.config.nodeName {
		return fmt.Errorf("live Pod identity does not match retirement witness")
	}
	controller := metav1.GetControllerOf(pod)
	validController := controller != nil && controller.APIVersion == "apps/v1" && controller.Kind == "DaemonSet"
	if state.Protocol == nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 {
		validController = validController && controller.Name == "dra-driver-nvidia-gpu-persistent-agent" && controller.UID != ""
	} else {
		validController = validController && controller.Name == state.daemonSetName && controller.UID == state.daemonSetUID
	}
	if !validController {
		return fmt.Errorf("retirement witness Pod owner does not match snapshot DaemonSet")
	}
	computeDomainUID := string(state.ComputeDomainUID)
	if computeDomainUID == "" {
		computeDomainUID = m.config.computeDomainUUID
	}
	evidence := &nvapi.ComputeDomainCliqueRetirementEvidence{
		TypeMeta: metav1.TypeMeta{APIVersion: nvapi.SchemeGroupVersion.String(), Kind: nvapi.ComputeDomainCliqueRetirementEvidenceKind},
		ObjectMeta: metav1.ObjectMeta{
			Name:      nvapi.ComputeDomainCliqueRetirementEvidenceName(spec.SnapshotUID, spec.Index),
			Namespace: m.config.podNamespace,
			Labels:    map[string]string{nvapi.ComputeDomainCliqueRetirementEvidenceComputeDomainLabel: computeDomainUID},
		},
		Spec: spec,
	}
	_, err = m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueRetirementEvidences(m.config.podNamespace).Create(ctx, evidence, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return err
	}
	existing, getErr := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueRetirementEvidences(m.config.podNamespace).Get(ctx, evidence.Name, metav1.GetOptions{})
	if getErr != nil {
		return getErr
	}
	if !reflect.DeepEqual(existing.Spec, evidence.Spec) || !reflect.DeepEqual(existing.Labels, evidence.Labels) {
		return fmt.Errorf("retirement evidence %s/%s already exists with a different immutable identity", evidence.Namespace, evidence.Name)
	}
	return nil
}

func (m *ComputeDomainCliqueSnapshotManager) enqueue(obj any) {
	if m.config.protocol == nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 {
		if m.informer.HasSynced() {
			m.reconcilePersistentSnapshots()
		}
		return
	}
	snapshot, ok := obj.(*nvapi.ComputeDomainCliqueSnapshot)
	if !ok {
		return
	}
	if err := m.consume(snapshot); err != nil {
		klog.Errorf("rejecting ComputeDomainCliqueSnapshot %s/%s: %v", snapshot.Namespace, snapshot.Name, err)
	}
}

func (m *ComputeDomainCliqueSnapshotManager) reconcilePersistentSnapshots() {
	var nonterminal []*nvapi.ComputeDomainCliqueSnapshot
	var fenced []*nvapi.ComputeDomainCliqueSnapshot
	for _, object := range m.informer.GetStore().List() {
		snapshot, ok := object.(*nvapi.ComputeDomainCliqueSnapshot)
		if !ok || snapshot.DeletionTimestamp != nil {
			continue
		}
		switch snapshot.Status.Phase {
		case nvapi.ComputeDomainCliqueSnapshotPhasePending, nvapi.ComputeDomainCliqueSnapshotPhaseActive, nvapi.ComputeDomainCliqueSnapshotPhaseRetiring:
			nonterminal = append(nonterminal, snapshot)
		case nvapi.ComputeDomainCliqueSnapshotPhaseFenced:
			fenced = append(fenced, snapshot)
		}
	}
	if len(nonterminal) > 1 {
		klog.Errorf("persistent agent found %d nonterminal snapshots for clique %q; preserving the current child and refusing new state", len(nonterminal), m.config.cliqueID)
		return
	}
	for _, snapshot := range fenced {
		m.acceptFenced(snapshot)
	}
	if len(nonterminal) == 0 && len(fenced) == 0 {
		m.acceptReleasedReservation()
	}
	if len(nonterminal) == 1 {
		if err := m.consume(nonterminal[0]); err != nil {
			klog.Errorf("rejecting persistent-agent ComputeDomainCliqueSnapshot %s/%s: %v", nonterminal[0].Namespace, nonterminal[0].Name, err)
		}
	}
}

func (m *ComputeDomainCliqueSnapshotManager) acceptReleasedReservation() {
	m.mu.Lock()
	retired := m.retired
	currentUID := m.currentSnapshotUID
	m.mu.Unlock()
	if retired.uid == "" || retired.uid != currentUID || m.config.cliqueID == "" {
		return
	}
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	reservation, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(ctx, cdclique.ReservationName(m.config.cliqueID), metav1.GetOptions{})
	metrics.ObserveCliqueAPIAction(string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1), metrics.CliqueAPIResourceReservation, metrics.CliqueAPIOperationGet, metrics.CliqueAPIResultForError(err), false)
	if err != nil {
		klog.Errorf("preserving retired persistent-agent state until its released reservation is observable: %v", err)
		return
	}
	if reservation.Status.Phase != nvapi.ComputeDomainCliqueReservationPhaseReleased ||
		reservation.Status.ReleaseReason != nvapi.ComputeDomainCliqueReservationReleaseReasonVerifiedFence ||
		reservation.Status.SnapshotUID != retired.uid || reservation.Status.FencedGeneration < retired.generation ||
		reservation.Status.FencedHash != retired.hash {
		klog.Errorf("preserving retired persistent-agent state because reservation %q lacks exact fence release evidence", reservation.Name)
		return
	}
	m.resetRetiredSnapshot(retired.uid)
}

func (m *ComputeDomainCliqueSnapshotManager) acceptFenced(snapshot *nvapi.ComputeDomainCliqueSnapshot) {
	m.resetRetiredSnapshot(snapshot.UID)
}

func (m *ComputeDomainCliqueSnapshotManager) resetRetiredSnapshot(expectedUID types.UID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.retired.uid == "" || m.retired.uid != m.currentSnapshotUID || (expectedUID != "" && expectedUID != m.currentSnapshotUID) {
		return
	}
	m.currentSnapshotUID = ""
	m.currentGeneration = 0
	m.currentHash = ""
	m.desired = controllerSnapshotIdentity{}
	m.applied = controllerSnapshotIdentity{}
	m.retired = controllerSnapshotIdentity{}
	m.retirementStarted = false
}

func (m *ComputeDomainCliqueSnapshotManager) consume(snapshot *nvapi.ComputeDomainCliqueSnapshot) error {
	state, err := m.validate(snapshot)
	if err != nil || state == nil {
		return err
	}
	identity := state.identity()

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.acceptIdentityLocked(identity); err != nil {
		return err
	}
	if identity == m.applied || identity == m.retired || identity == m.desired {
		return nil
	}
	m.desired = identity

	// The latest valid API snapshot supersedes an older unconsumed event. The
	// consumer retains an event locally while retrying, so overwriting this
	// single buffered slot cannot lose the only retry trigger for a failure.
	select {
	case m.desiredStateChan <- state:
	default:
		<-m.desiredStateChan
		m.desiredStateChan <- state
	}
	return nil
}

func (m *ComputeDomainCliqueSnapshotManager) acceptIdentityLocked(identity controllerSnapshotIdentity) error {
	if m.retirementStarted && !identity.retiring {
		return fmt.Errorf("snapshot returned to active after retirement started")
	}
	if identity.retiring {
		m.retirementStarted = true
	}
	if m.currentSnapshotUID == "" {
		m.currentSnapshotUID = identity.uid
		m.currentGeneration = identity.generation
		m.currentHash = identity.hash
		return nil
	}
	if identity.uid == m.currentSnapshotUID {
		if identity.generation < m.currentGeneration {
			return fmt.Errorf("snapshot generation rollback from %d to %d", m.currentGeneration, identity.generation)
		}
		if identity.generation == m.currentGeneration && identity.hash != m.currentHash {
			return fmt.Errorf("snapshot generation %d changed content hash", identity.generation)
		}
		m.currentGeneration = identity.generation
		m.currentHash = identity.hash
		return nil
	}
	// A new Kubernetes object UID is a new allocation stream, not evidence
	// that the IMEX child authorized by the old object stopped using its slot.
	// Normal whole-ComputeDomain deletion now has an exact retirement verifier,
	// but that transition terminates this daemon Pod; an in-place running daemon
	// still never crosses object UIDs. Ambiguous recovery requires a verified
	// whole-clique reset or a future per-member handoff verifier.
	return fmt.Errorf("snapshot object UID changed from %q to %q without verified handoff", m.currentSnapshotUID, identity.uid)
}

func (m *ComputeDomainCliqueSnapshotManager) validate(snapshot *nvapi.ComputeDomainCliqueSnapshot) (*ControllerSnapshotDesiredState, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("snapshot is nil")
	}
	if snapshot.UID == "" {
		return nil, fmt.Errorf("snapshot UID is empty")
	}
	if snapshot.Namespace != m.config.podNamespace {
		return nil, fmt.Errorf("snapshot namespace %q does not match daemon namespace %q", snapshot.Namespace, m.config.podNamespace)
	}
	protocol := nvapi.EffectiveComputeDomainCliqueSnapshotProtocol(snapshot.Spec.Protocol)
	daemonProtocol := m.config.protocol
	if daemonProtocol == "" {
		daemonProtocol = nvapi.ComputeDomainCliqueProtocolControllerV1
	}
	if protocol != daemonProtocol {
		return nil, fmt.Errorf("snapshot protocol %q does not match daemon protocol %q", protocol, daemonProtocol)
	}
	if protocol == nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 {
		if snapshot.Name != cdclique.SnapshotName(string(snapshot.Spec.ComputeDomainUID), snapshot.Spec.CliqueID) ||
			snapshot.Labels[computeDomainLabelKey] != string(snapshot.Spec.ComputeDomainUID) ||
			snapshot.Labels[computeDomainCliqueLabelKey] != snapshot.Spec.CliqueID ||
			snapshot.Labels["resource.nvidia.com/computeDomainCliqueProtocol"] != string(protocol) {
			return nil, fmt.Errorf("persistent-agent snapshot has invalid canonical scope")
		}
	}
	if m.config.protocol != nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 && snapshot.Spec.ComputeDomainUID != types.UID(m.config.computeDomainUUID) {
		return nil, fmt.Errorf("snapshot scope does not match this daemon")
	}
	if m.config.cliqueID != "" && snapshot.Spec.CliqueID != m.config.cliqueID {
		return nil, fmt.Errorf("snapshot scope does not match this daemon")
	}
	if m.config.cliqueID == "" && snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseRetiring && snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseFenced {
		return nil, fmt.Errorf("daemon without a discovered clique may consume only retirement state")
	}
	if snapshot.Spec.Capacity != m.config.maxNodesPerIMEXDomain {
		return nil, fmt.Errorf("snapshot capacity %d does not match daemon capacity %d", snapshot.Spec.Capacity, m.config.maxNodesPerIMEXDomain)
	}
	controller := metav1.GetControllerOf(snapshot)
	if protocol == nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 {
		if controller != nil {
			return nil, fmt.Errorf("persistent-agent snapshot must not have a controller owner")
		}
	} else if controller == nil || controller.APIVersion != "apps/v1" || controller.Kind != "DaemonSet" || controller.Name == "" || controller.UID == "" {
		return nil, fmt.Errorf("snapshot DaemonSet owner identity is incomplete")
	}
	if snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhasePending || snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhaseFenced {
		return nil, nil
	}
	if snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseActive && snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseRetiring {
		return nil, fmt.Errorf("snapshot has invalid phase %q", snapshot.Status.Phase)
	}
	retiring := snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhaseRetiring
	members, err := cdclique.ValidatePublishedState(snapshot)
	if err != nil {
		return nil, err
	}
	if protocol == nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 {
		ctx := m.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		reservation, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(ctx, cdclique.ReservationName(snapshot.Spec.CliqueID), metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("read active physical clique reservation: %w", err)
		}
		if reservation.Spec.CliqueID != snapshot.Spec.CliqueID || reservation.Spec.ComputeDomainUID != snapshot.Spec.ComputeDomainUID ||
			reservation.Status.Phase != nvapi.ComputeDomainCliqueReservationPhaseActive || reservation.Status.SnapshotUID != snapshot.UID ||
			reservation.Status.ActivationGeneration <= 0 || reservation.Status.ActivationGeneration > snapshot.Status.Generation ||
			len(reservation.Status.ActivationHash) != cdclique.HashHexLength || reservation.Status.FencedGeneration != 0 {
			return nil, fmt.Errorf("active physical clique reservation does not authorize snapshot")
		}
	}
	var self *nvapi.ComputeDomainCliqueMember
	daemons := make([]*nvapi.ComputeDomainDaemonInfo, 0, len(members))
	for i := range members {
		member := &members[i]
		if member.NodeName == m.config.nodeName {
			self = member
		}
		daemons = append(daemons, &nvapi.ComputeDomainDaemonInfo{
			NodeName:  member.NodeName,
			IPAddress: member.PodIP,
			CliqueID:  snapshot.Spec.CliqueID,
			Index:     member.Index,
			Status:    nvapi.ComputeDomainStatusNotReady,
		})
	}
	if self == nil {
		return nil, fmt.Errorf("published snapshot has no member for this Node")
	}
	if !retiring {
		if string(self.PodUID) != m.config.podUID || self.PodName != m.config.podName {
			return nil, fmt.Errorf("active snapshot does not authorize this exact Pod UID")
		}
		configuredPodIP, err := netip.ParseAddr(m.config.podIP)
		selfPodIP, selfIPError := netip.ParseAddr(self.PodIP)
		if err != nil || selfIPError != nil || configuredPodIP.Unmap() != selfPodIP.Unmap() {
			return nil, fmt.Errorf("active snapshot does not authorize this exact Pod IP")
		}
	}

	state := &ControllerSnapshotDesiredState{
		ComputeDomainUID: snapshot.Spec.ComputeDomainUID,
		CliqueID:         snapshot.Spec.CliqueID,
		Protocol:         protocol,
		Members:          daemons,
		Receipt: &nvapi.ComputeDomainCliqueSnapshotReceipt{
			SnapshotUID:        snapshot.UID,
			SnapshotGeneration: snapshot.Status.Generation,
			SnapshotHash:       snapshot.Status.Hash,
			NodeUID:            self.NodeUID,
			PodUID:             self.PodUID,
			Index:              self.Index,
		},
	}
	if controller != nil {
		state.daemonSetName = controller.Name
		state.daemonSetUID = controller.UID
	}
	if retiring {
		reason := nvapi.ComputeDomainCliqueRetirementEvidenceReasonProcessExit
		if self.NodeBootID != "" && m.config.bootID != "" && self.NodeBootID != m.config.bootID {
			reason = nvapi.ComputeDomainCliqueRetirementEvidenceReasonNodeReboot
		} else if string(self.PodUID) != m.config.podUID || self.PodName != m.config.podName {
			return nil, fmt.Errorf("same-boot replacement Pod cannot attest retirement for published Pod UID %q", self.PodUID)
		}
		state.Members = nil
		state.Receipt = nil
		state.RetirementEvidence = &nvapi.ComputeDomainCliqueRetirementEvidenceSpec{
			Reason:             reason,
			SnapshotUID:        snapshot.UID,
			SnapshotGeneration: snapshot.Status.Generation,
			SnapshotHash:       snapshot.Status.Hash,
			Index:              self.Index,
			NodeUID:            self.NodeUID,
			ActivationBootID:   self.NodeBootID,
			WitnessBootID:      m.config.bootID,
			WitnessPodUID:      types.UID(m.config.podUID),
		}
	}
	return state, nil
}
