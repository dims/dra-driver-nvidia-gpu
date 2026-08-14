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
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
)

// ControllerSnapshotDesiredState couples one complete peer map with the exact
// receipt which may be published after that map has been installed and IMEX
// has been started or restarted. Keeping these in one event prevents a consumer
// from acknowledging a different snapshot than the one it just applied.
type ControllerSnapshotDesiredState struct {
	Members           []*nvapi.ComputeDomainDaemonInfo
	Receipt           *nvapi.ComputeDomainCliqueSnapshotReceipt
	RetirementReceipt *nvapi.ComputeDomainCliqueRetirementReceipt
}

type controllerSnapshotIdentity struct {
	uid        types.UID
	generation int64
	hash       string
	retiring   bool
}

func (s *ControllerSnapshotDesiredState) identity() controllerSnapshotIdentity {
	if s.RetirementReceipt != nil {
		return controllerSnapshotIdentity{
			uid: s.RetirementReceipt.SnapshotUID, generation: s.RetirementReceipt.SnapshotGeneration,
			hash: s.RetirementReceipt.SnapshotHash, retiring: true,
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
		digest := sha256.Sum256([]byte(config.cliqueID))
		name := fmt.Sprintf("%s.%s", config.computeDomainUUID, hex.EncodeToString(digest[:8]))
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
	if state == nil || state.RetirementReceipt == nil {
		return
	}
	identity := state.identity()
	m.mu.Lock()
	defer m.mu.Unlock()
	if identity == m.desired {
		m.retired = identity
	}
}

func (m *ComputeDomainCliqueSnapshotManager) PublishRetirementReceipt(ctx context.Context, state *ControllerSnapshotDesiredState) error {
	if state == nil || state.RetirementReceipt == nil {
		return fmt.Errorf("retirement receipt is missing")
	}
	encoded, err := json.Marshal(state.RetirementReceipt)
	if err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		pod, err := m.config.clientsets.Core.CoreV1().Pods(m.config.podNamespace).Get(ctx, m.config.podName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if string(pod.UID) != m.config.podUID || pod.UID != state.RetirementReceipt.PodUID {
			return fmt.Errorf("live Pod UID %q does not match retirement identity %q", pod.UID, state.RetirementReceipt.PodUID)
		}
		existing := pod.Annotations[nvapi.ComputeDomainCliqueRetirementReceiptAnnotation]
		if existing == string(encoded) {
			return nil
		}
		if existing != "" {
			return fmt.Errorf("Pod already has a different immutable retirement receipt")
		}
		updated := pod.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = map[string]string{}
		}
		updated.Annotations[nvapi.ComputeDomainCliqueRetirementReceiptAnnotation] = string(encoded)
		_, err = m.config.clientsets.Core.CoreV1().Pods(m.config.podNamespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
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
	if snapshot.Spec.Protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 {
		return nil, fmt.Errorf("snapshot protocol %q is not controller-v1", snapshot.Spec.Protocol)
	}
	if snapshot.Spec.ComputeDomainUID != types.UID(m.config.computeDomainUUID) ||
		snapshot.Spec.ComputeDomainName != m.config.computeDomainName ||
		snapshot.Spec.ComputeDomainNamespace != m.config.computeDomainNamespace {
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
	if snapshot.Spec.DaemonSetName == "" || snapshot.Spec.DaemonSetUID == "" {
		return nil, fmt.Errorf("snapshot DaemonSet identity is incomplete")
	}
	controller := metav1.GetControllerOf(snapshot)
	if controller == nil || controller.APIVersion != "apps/v1" || controller.Kind != "DaemonSet" ||
		controller.Name != snapshot.Spec.DaemonSetName || controller.UID != snapshot.Spec.DaemonSetUID {
		return nil, fmt.Errorf("snapshot controller owner does not match its DaemonSet scope")
	}
	if snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhasePending || snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhaseFenced {
		return nil, nil
	}
	if snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseActive && snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseRetiring {
		return nil, fmt.Errorf("snapshot has invalid phase %q", snapshot.Status.Phase)
	}
	if snapshot.Status.Generation <= 0 {
		return nil, fmt.Errorf("active snapshot generation must be positive")
	}
	if snapshot.Status.Hash == "" {
		return nil, fmt.Errorf("active snapshot hash is empty")
	}
	if snapshot.Status.MemberCount != len(snapshot.Status.Members) {
		return nil, fmt.Errorf("memberCount %d does not match %d members", snapshot.Status.MemberCount, len(snapshot.Status.Members))
	}

	assignmentsByNodeUID := make(map[types.UID]nvapi.ComputeDomainCliqueAssignment, len(snapshot.Status.Assignments))
	assignmentIndices := make(map[int]struct{}, len(snapshot.Status.Assignments))
	assignmentNodeNames := make(map[string]struct{}, len(snapshot.Status.Assignments))
	assignmentPodUIDs := make(map[types.UID]struct{}, len(snapshot.Status.Assignments))
	for _, assignment := range snapshot.Status.Assignments {
		if assignment.NodeUID == "" || assignment.NodeName == "" {
			return nil, fmt.Errorf("assignment has empty Node identity")
		}
		if assignment.Index < 0 || assignment.Index >= snapshot.Spec.Capacity {
			return nil, fmt.Errorf("assignment index %d out of range", assignment.Index)
		}
		if assignment.State != nvapi.ComputeDomainCliqueAssignmentStateBound && assignment.State != nvapi.ComputeDomainCliqueAssignmentStateQuarantined {
			return nil, fmt.Errorf("assignment for Node UID %q has invalid state %q", assignment.NodeUID, assignment.State)
		}
		if _, duplicate := assignmentsByNodeUID[assignment.NodeUID]; duplicate {
			return nil, fmt.Errorf("duplicate assignment Node UID %q", assignment.NodeUID)
		}
		if _, duplicate := assignmentIndices[assignment.Index]; duplicate {
			return nil, fmt.Errorf("duplicate assignment index %d", assignment.Index)
		}
		if _, duplicate := assignmentNodeNames[assignment.NodeName]; duplicate {
			return nil, fmt.Errorf("duplicate assignment Node name %q", assignment.NodeName)
		}
		if assignment.CurrentPodUID != "" {
			if _, duplicate := assignmentPodUIDs[assignment.CurrentPodUID]; duplicate {
				return nil, fmt.Errorf("duplicate assignment current Pod UID %q", assignment.CurrentPodUID)
			}
			assignmentPodUIDs[assignment.CurrentPodUID] = struct{}{}
		}
		assignmentsByNodeUID[assignment.NodeUID] = assignment
		assignmentIndices[assignment.Index] = struct{}{}
		assignmentNodeNames[assignment.NodeName] = struct{}{}
	}

	members := slices.Clone(snapshot.Status.Members)
	slices.SortFunc(members, func(a, b nvapi.ComputeDomainCliqueMember) int { return cmp.Compare(a.Index, b.Index) })
	canonicalHash, err := canonicalSnapshotHash(members)
	if err != nil {
		return nil, err
	}
	if canonicalHash != snapshot.Status.Hash {
		return nil, fmt.Errorf("snapshot hash mismatch: got %q, want %q", snapshot.Status.Hash, canonicalHash)
	}

	var self *nvapi.ComputeDomainCliqueMember
	seenIndices := make(map[int]struct{}, len(members))
	seenNodeUIDs := make(map[types.UID]struct{}, len(members))
	seenNodeNames := make(map[string]struct{}, len(members))
	seenPodUIDs := make(map[types.UID]struct{}, len(members))
	seenPodNames := make(map[string]struct{}, len(members))
	seenPodIPs := make(map[netip.Addr]struct{}, len(members))
	daemons := make([]*nvapi.ComputeDomainDaemonInfo, 0, len(members))
	for i := range members {
		member := &members[i]
		if member.Index < 0 || member.Index >= snapshot.Spec.Capacity {
			return nil, fmt.Errorf("member index %d out of range", member.Index)
		}
		if member.NodeName == "" || member.NodeUID == "" || member.PodName == "" || member.PodUID == "" || member.DaemonSetUID == "" {
			return nil, fmt.Errorf("member at index %d has incomplete identity", member.Index)
		}
		if member.DaemonSetUID != snapshot.Spec.DaemonSetUID {
			return nil, fmt.Errorf("member at index %d has DaemonSet UID %q, want %q", member.Index, member.DaemonSetUID, snapshot.Spec.DaemonSetUID)
		}
		podIP, err := netip.ParseAddr(member.PodIP)
		if err != nil || podIP.IsUnspecified() {
			return nil, fmt.Errorf("member at index %d has invalid Pod IP %q", member.Index, member.PodIP)
		}
		podIP = podIP.Unmap()
		if _, duplicate := seenIndices[member.Index]; duplicate {
			return nil, fmt.Errorf("duplicate member index %d", member.Index)
		}
		if _, duplicate := seenNodeUIDs[member.NodeUID]; duplicate {
			return nil, fmt.Errorf("duplicate member Node UID %q", member.NodeUID)
		}
		if _, duplicate := seenNodeNames[member.NodeName]; duplicate {
			return nil, fmt.Errorf("duplicate member Node name %q", member.NodeName)
		}
		if _, duplicate := seenPodUIDs[member.PodUID]; duplicate {
			return nil, fmt.Errorf("duplicate member Pod UID %q", member.PodUID)
		}
		if _, duplicate := seenPodNames[member.PodName]; duplicate {
			return nil, fmt.Errorf("duplicate member Pod name %q", member.PodName)
		}
		if _, duplicate := seenPodIPs[podIP]; duplicate {
			return nil, fmt.Errorf("duplicate member Pod IP %q", member.PodIP)
		}
		seenIndices[member.Index] = struct{}{}
		seenNodeUIDs[member.NodeUID] = struct{}{}
		seenNodeNames[member.NodeName] = struct{}{}
		seenPodUIDs[member.PodUID] = struct{}{}
		seenPodNames[member.PodName] = struct{}{}
		seenPodIPs[podIP] = struct{}{}

		assignment, found := assignmentsByNodeUID[member.NodeUID]
		if !found || assignment.State != nvapi.ComputeDomainCliqueAssignmentStateBound ||
			assignment.Index != member.Index || assignment.NodeName != member.NodeName || assignment.CurrentPodUID != member.PodUID {
			return nil, fmt.Errorf("member at index %d does not match its bound assignment", member.Index)
		}
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
	if self == nil || string(self.PodUID) != m.config.podUID || self.PodName != m.config.podName {
		return nil, fmt.Errorf("active snapshot does not authorize this exact Pod UID")
	}
	configuredPodIP, err := netip.ParseAddr(m.config.podIP)
	selfPodIP, selfIPError := netip.ParseAddr(self.PodIP)
	if err != nil || selfIPError != nil || configuredPodIP.Unmap() != selfPodIP.Unmap() {
		return nil, fmt.Errorf("active snapshot does not authorize this exact Pod IP")
	}

	state := &ControllerSnapshotDesiredState{
		Members: daemons,
		Receipt: &nvapi.ComputeDomainCliqueSnapshotReceipt{
			ComputeDomainUID:   snapshot.Spec.ComputeDomainUID,
			SnapshotUID:        snapshot.UID,
			SnapshotGeneration: snapshot.Status.Generation,
			SnapshotHash:       snapshot.Status.Hash,
			NodeUID:            self.NodeUID,
			PodUID:             self.PodUID,
			Index:              self.Index,
		},
	}
	if snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhaseRetiring {
		state.Members = nil
		state.Receipt = nil
		state.RetirementReceipt = &nvapi.ComputeDomainCliqueRetirementReceipt{
			ComputeDomainUID: snapshot.Spec.ComputeDomainUID, SnapshotUID: snapshot.UID,
			SnapshotGeneration: snapshot.Status.Generation, SnapshotHash: snapshot.Status.Hash,
			NodeUID: self.NodeUID, PodUID: self.PodUID, Index: self.Index,
		}
	}
	return state, nil
}

func canonicalSnapshotHash(members []nvapi.ComputeDomainCliqueMember) (string, error) {
	canonical, err := json.Marshal(members)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}
