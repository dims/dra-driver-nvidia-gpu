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
	"fmt"
	"net/netip"
	"reflect"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/cdclique"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
)

// ControllerSnapshotDesiredState couples one complete peer map with the exact
// receipt which may be published after that map has been installed and IMEX
// has been started or restarted. Keeping these in one event prevents a consumer
// from acknowledging a different snapshot than the one it just applied.
type ControllerSnapshotDesiredState struct {
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
	if config.cliqueID != "" {
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
	if _, err := m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    m.enqueue,
		UpdateFunc: func(_, current any) { m.enqueue(current) },
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
	if controller == nil || controller.APIVersion != "apps/v1" || controller.Kind != "DaemonSet" ||
		controller.Name != state.daemonSetName || controller.UID != state.daemonSetUID {
		return fmt.Errorf("retirement witness Pod owner does not match snapshot DaemonSet")
	}
	evidence := &nvapi.ComputeDomainCliqueRetirementEvidence{
		TypeMeta: metav1.TypeMeta{APIVersion: nvapi.SchemeGroupVersion.String(), Kind: nvapi.ComputeDomainCliqueRetirementEvidenceKind},
		ObjectMeta: metav1.ObjectMeta{
			Name:      nvapi.ComputeDomainCliqueRetirementEvidenceName(spec.SnapshotUID, spec.Index),
			Namespace: m.config.podNamespace,
			Labels: map[string]string{
				nvapi.ComputeDomainCliqueRetirementEvidenceComputeDomainLabel: m.config.computeDomainUUID,
			},
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
	snapshot, ok := obj.(*nvapi.ComputeDomainCliqueSnapshot)
	if !ok {
		return
	}
	if err := m.consume(snapshot); err != nil {
		klog.Errorf("rejecting ComputeDomainCliqueSnapshot %s/%s: %v", snapshot.Namespace, snapshot.Name, err)
	}
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
	if snapshot.Spec.ComputeDomainUID != types.UID(m.config.computeDomainUUID) {
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
	if controller == nil || controller.APIVersion != "apps/v1" || controller.Kind != "DaemonSet" || controller.Name == "" || controller.UID == "" {
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
		daemonSetName: controller.Name,
		daemonSetUID:  controller.UID,
		Members:       daemons,
		Receipt: &nvapi.ComputeDomainCliqueSnapshotReceipt{
			SnapshotUID:        snapshot.UID,
			SnapshotGeneration: snapshot.Status.Generation,
			SnapshotHash:       snapshot.Status.Hash,
			NodeUID:            self.NodeUID,
			PodUID:             self.PodUID,
			Index:              self.Index,
		},
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
