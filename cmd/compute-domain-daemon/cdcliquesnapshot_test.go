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
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/cdclique"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

func newTestSnapshotManager() *PersistentAgentSnapshotManager {
	reservation := &nvapi.ComputeDomainCliqueReservation{
		ObjectMeta: metav1.ObjectMeta{Name: cdclique.ReservationName("clique-a")},
		Spec: nvapi.ComputeDomainCliqueReservationSpec{
			CliqueID: "clique-a", ComputeDomainUID: types.UID("cd-uid"),
		},
		Status: nvapi.ComputeDomainCliqueReservationStatus{
			Phase:                nvapi.ComputeDomainCliqueReservationPhaseActive,
			SnapshotUID:          types.UID("snapshot-uid"),
			ActivationGeneration: 1,
			ActivationHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	return &PersistentAgentSnapshotManager{
		config: &ManagerConfig{
			computeDomainUUID:      "cd-uid",
			computeDomainName:      "cd-name",
			computeDomainNamespace: "tenant-a",
			cliqueID:               "clique-a",
			nodeName:               "node-a",
			podName:                "pod-a",
			podUID:                 "pod-a-uid",
			podIP:                  "10.0.0.1",
			podNamespace:           "driver",
			bootID:                 "boot-a",
			maxNodesPerIMEXDomain:  18,
			clientsets: flags.ClientSets{
				Nvidia: nvfake.NewSimpleClientset(reservation),
			},
		},
		desiredStateChan: make(chan *PersistentAgentDesiredState, 1),
	}
}

func newTestSnapshot(t *testing.T) *nvapi.ComputeDomainCliqueSnapshot {
	t.Helper()
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cdclique.SnapshotName("cd-uid", "clique-a"),
			Namespace: "driver",
			UID:       types.UID("snapshot-uid"),
			Labels: map[string]string{
				computeDomainLabelKey:                             "cd-uid",
				computeDomainCliqueLabelKey:                       "clique-a",
				"resource.nvidia.com/computeDomainCliqueProtocol": string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1),
			},
		},
		Spec: nvapi.ComputeDomainCliqueSnapshotSpec{
			ComputeDomainUID: types.UID("cd-uid"),
			CliqueID:         "clique-a",
			Capacity:         18,
		},
		Status: nvapi.ComputeDomainCliqueSnapshotStatus{
			Phase:      nvapi.ComputeDomainCliqueSnapshotPhaseActive,
			Generation: 7,
			Assignments: []nvapi.ComputeDomainCliqueAssignment{
				{Index: 0, NodeName: "node-a", NodeUID: types.UID("node-a-uid"), State: nvapi.ComputeDomainCliqueAssignmentStateBound, CurrentPodUID: types.UID("pod-a-uid"), EverPublished: true},
				{Index: 1, NodeName: "node-b", NodeUID: types.UID("node-b-uid"), State: nvapi.ComputeDomainCliqueAssignmentStateBound, CurrentPodUID: types.UID("pod-b-uid"), EverPublished: true},
			},
			// Deliberately unordered: canonical validation and delivery sort by index.
			Members: []nvapi.ComputeDomainCliqueMember{
				{Index: 1, NodeName: "node-b", NodeUID: types.UID("node-b-uid"), NodeBootID: "boot-b", PodName: "pod-b", PodUID: types.UID("pod-b-uid"), PodIP: "10.0.0.2"},
				{Index: 0, NodeName: "node-a", NodeUID: types.UID("node-a-uid"), NodeBootID: "boot-a", PodName: "pod-a", PodUID: types.UID("pod-a-uid"), PodIP: "10.0.0.1"},
			},
		},
	}
	setTestSnapshotHash(t, snapshot)
	return snapshot
}

func setTestSnapshotHash(t *testing.T, snapshot *nvapi.ComputeDomainCliqueSnapshot) {
	t.Helper()
	members := slices.Clone(snapshot.Status.Members)
	slices.SortFunc(members, func(a, b nvapi.ComputeDomainCliqueMember) int { return cmp.Compare(a.Index, b.Index) })
	hash, err := cdclique.CanonicalHash(members)
	require.NoError(t, err)
	snapshot.Status.Hash = hash
}

func TestSnapshotConsumerValidatesAndCouplesDesiredState(t *testing.T) {
	m := newTestSnapshotManager()
	snapshot := newTestSnapshot(t)

	require.NoError(t, m.consume(snapshot))
	desired := <-m.desiredStateChan
	require.Equal(t, []int{0, 1}, []int{desired.Members[0].Index, desired.Members[1].Index})
	require.Equal(t, types.UID("pod-a-uid"), desired.Receipt.PodUID)
	require.Equal(t, int64(7), desired.Receipt.SnapshotGeneration)
	require.Equal(t, snapshot.Status.Hash, desired.Receipt.SnapshotHash)
	require.Empty(t, m.applied, "observing a snapshot must not mark it applied")

	m.MarkApplied(desired)
	require.Equal(t, desired.identity(), m.applied)
	require.NoError(t, m.consume(snapshot.DeepCopy()))
	select {
	case <-m.desiredStateChan:
		t.Fatal("an already applied snapshot was delivered again")
	default:
	}
}

func TestPersistentAgentSnapshotUsesSameValidatedDesiredState(t *testing.T) {
	snapshot := newTestSnapshot(t)
	snapshot.Name = cdclique.SnapshotName(string(snapshot.Spec.ComputeDomainUID), snapshot.Spec.CliqueID)
	snapshot.Spec.Protocol = nvapi.ComputeDomainCliqueProtocolPersistentAgentV1
	snapshot.OwnerReferences = nil
	snapshot.Labels = map[string]string{
		computeDomainLabelKey:                             string(snapshot.Spec.ComputeDomainUID),
		computeDomainCliqueLabelKey:                       snapshot.Spec.CliqueID,
		"resource.nvidia.com/computeDomainCliqueProtocol": string(snapshot.Spec.Protocol),
	}
	reservation := &nvapi.ComputeDomainCliqueReservation{
		ObjectMeta: metav1.ObjectMeta{Name: cdclique.ReservationName(snapshot.Spec.CliqueID)},
		Spec: nvapi.ComputeDomainCliqueReservationSpec{
			CliqueID: snapshot.Spec.CliqueID, ComputeDomainUID: snapshot.Spec.ComputeDomainUID,
		},
		Status: nvapi.ComputeDomainCliqueReservationStatus{
			Phase: nvapi.ComputeDomainCliqueReservationPhaseActive, SnapshotUID: snapshot.UID,
			ActivationGeneration: 1, ActivationHash: snapshot.Status.Hash,
		},
	}
	m := newTestSnapshotManager()
	m.config.computeDomainUUID = ""
	m.config.clientsets = flags.ClientSets{Nvidia: nvfake.NewSimpleClientset(reservation)}
	m.ctx = context.Background()

	require.NoError(t, m.consume(snapshot))
	desired := <-m.desiredStateChan
	require.Equal(t, snapshot.Spec.ComputeDomainUID, desired.ComputeDomainUID)
	require.Equal(t, snapshot.Spec.CliqueID, desired.CliqueID)
	require.Len(t, desired.Members, 2)
	m.MarkApplied(desired)
	retiring := snapshot.DeepCopy()
	retiring.Status.Phase = nvapi.ComputeDomainCliqueSnapshotPhaseRetiring
	require.NoError(t, m.consume(retiring))
	retirement := <-m.desiredStateChan
	require.NotNil(t, retirement.RetirementEvidence)
	m.MarkRetired(retirement)
	fenced := retiring.DeepCopy()
	fenced.Status.Phase = nvapi.ComputeDomainCliqueSnapshotPhaseFenced
	m.acceptFenced(fenced)
	require.Empty(t, m.currentSnapshotUID, "verified fenced state must make the long-lived agent reusable")

	conflicting := snapshot.DeepCopy()
	conflicting.UID = types.UID("other-snapshot")
	conflicting.Status.Generation++
	require.ErrorContains(t, m.consume(conflicting), "does not authorize")
}

func TestPersistentAgentRequiresExactReleasedReservationWhenFencedEventWasMissed(t *testing.T) {
	reservation := &nvapi.ComputeDomainCliqueReservation{
		ObjectMeta: metav1.ObjectMeta{Name: cdclique.ReservationName("clique-a")},
		Spec:       nvapi.ComputeDomainCliqueReservationSpec{CliqueID: "clique-a", ComputeDomainUID: types.UID("cd-uid")},
		Status: nvapi.ComputeDomainCliqueReservationStatus{
			Phase: nvapi.ComputeDomainCliqueReservationPhaseReleased, ReleaseReason: nvapi.ComputeDomainCliqueReservationReleaseReasonVerifiedFence,
			SnapshotUID: types.UID("snapshot-uid"), FencedGeneration: 7,
			FencedHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	m := newTestSnapshotManager()
	m.config.clientsets = flags.ClientSets{Nvidia: nvfake.NewSimpleClientset(reservation)}
	m.currentSnapshotUID = types.UID("snapshot-uid")
	m.retired = persistentAgentSnapshotIdentity{uid: types.UID("snapshot-uid"), generation: 7, hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", retiring: true}

	m.acceptReleasedReservation()
	require.Equal(t, types.UID("snapshot-uid"), m.currentSnapshotUID, "mismatched fence hash must preserve retired state")

	reservation.Status.FencedHash = m.retired.hash
	_, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().UpdateStatus(context.Background(), reservation, metav1.UpdateOptions{})
	require.NoError(t, err)
	m.acceptReleasedReservation()
	require.Empty(t, m.currentSnapshotUID)
}

func TestSnapshotConsumerTracksGenerationWithinUID(t *testing.T) {
	m := newTestSnapshotManager()
	snapshot := newTestSnapshot(t)
	require.NoError(t, m.consume(snapshot))
	first := <-m.desiredStateChan
	m.MarkApplied(first)

	// Generation is part of the receipt even when content is identical.
	next := snapshot.DeepCopy()
	next.Status.Generation++
	require.NoError(t, m.consume(next))
	desired := <-m.desiredStateChan
	require.Equal(t, snapshot.Status.Hash, desired.Receipt.SnapshotHash)
	require.Equal(t, int64(8), desired.Receipt.SnapshotGeneration)

	rollback := next.DeepCopy()
	rollback.Status.Generation = 6
	require.ErrorContains(t, m.consume(rollback), "generation rollback")

	equivocation := next.DeepCopy()
	equivocation.Status.Hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	require.Error(t, m.consume(equivocation))
}

func TestSnapshotConsumerScopesGenerationByObjectUID(t *testing.T) {
	m := newTestSnapshotManager()
	first := newTestSnapshot(t)
	require.NoError(t, m.consume(first))
	<-m.desiredStateChan

	recreated := first.DeepCopy()
	recreated.UID = types.UID("replacement-snapshot-uid")
	recreated.Status.Generation = 1
	reservation, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(
		context.Background(), cdclique.ReservationName(recreated.Spec.CliqueID), metav1.GetOptions{})
	require.NoError(t, err)
	reservation.Status.SnapshotUID = recreated.UID
	reservation.Status.ActivationGeneration = recreated.Status.Generation
	_, err = m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().UpdateStatus(
		context.Background(), reservation, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.ErrorContains(t, m.consume(recreated), "without verified handoff")
	select {
	case <-m.desiredStateChan:
		t.Fatal("replacement snapshot was delivered without a verified handoff")
	default:
	}
}

func TestSnapshotConsumerDeliversRetirementForExactPublishedPod(t *testing.T) {
	m := newTestSnapshotManager()
	active := newTestSnapshot(t)
	require.NoError(t, m.consume(active))
	installed := <-m.desiredStateChan
	m.MarkApplied(installed)

	retiring := active.DeepCopy()
	retiring.Status.Phase = nvapi.ComputeDomainCliqueSnapshotPhaseRetiring
	require.NoError(t, m.consume(retiring))
	desired := <-m.desiredStateChan
	require.Nil(t, desired.Receipt)
	require.Empty(t, desired.Members)
	require.Equal(t, &nvapi.ComputeDomainCliqueRetirementEvidenceSpec{
		Reason:             nvapi.ComputeDomainCliqueRetirementEvidenceReasonProcessExit,
		SnapshotUID:        types.UID("snapshot-uid"),
		SnapshotGeneration: 7, SnapshotHash: active.Status.Hash, Index: 0,
		NodeUID: types.UID("node-a-uid"), ActivationBootID: "boot-a", WitnessBootID: "boot-a",
		WitnessPodUID: types.UID("pod-a-uid"),
	}, desired.RetirementEvidence)

	m.MarkRetired(desired)
	require.NoError(t, m.consume(retiring))
	select {
	case <-m.desiredStateChan:
		t.Fatal("an already retired snapshot was delivered again")
	default:
	}
}

func TestSnapshotConsumerWithoutDiscoveredCliqueAcceptsOnlyExactRetirement(t *testing.T) {
	m := newTestSnapshotManager()
	m.config.cliqueID = ""
	active := newTestSnapshot(t)
	require.ErrorContains(t, m.consume(active), "only retirement state")
	select {
	case <-m.desiredStateChan:
		t.Fatal("daemon without discovered topology consumed Active state")
	default:
	}

	retiring := active.DeepCopy()
	retiring.Status.Phase = nvapi.ComputeDomainCliqueSnapshotPhaseRetiring
	require.NoError(t, m.consume(retiring))
	desired := <-m.desiredStateChan
	require.NotNil(t, desired.RetirementEvidence)
	require.Equal(t, types.UID("pod-a-uid"), desired.RetirementEvidence.WitnessPodUID)
}

func TestSnapshotConsumerAcceptsReplacementOnlyAfterNodeReboot(t *testing.T) {
	m := newTestSnapshotManager()
	active := newTestSnapshot(t)
	require.NoError(t, m.consume(active))
	installed := <-m.desiredStateChan
	m.MarkApplied(installed)

	m.config.podName = "replacement"
	m.config.podUID = "replacement-uid"
	m.config.podIP = "10.0.0.9"
	retiring := active.DeepCopy()
	retiring.Status.Phase = nvapi.ComputeDomainCliqueSnapshotPhaseRetiring
	retiring.Status.Assignments[0].State = nvapi.ComputeDomainCliqueAssignmentStateQuarantined
	require.ErrorContains(t, m.consume(retiring), "same-boot replacement")

	m.config.bootID = "boot-after-reboot"
	require.NoError(t, m.consume(retiring))
	desired := <-m.desiredStateChan
	require.Equal(t, nvapi.ComputeDomainCliqueRetirementEvidenceReasonNodeReboot, desired.RetirementEvidence.Reason)
	require.Equal(t, types.UID("replacement-uid"), desired.RetirementEvidence.WitnessPodUID)
	require.Equal(t, "boot-a", desired.RetirementEvidence.ActivationBootID)
	require.Equal(t, "boot-after-reboot", desired.RetirementEvidence.WitnessBootID)
}

func TestSnapshotConsumerUsesNodeRebootWhenPodUIDSurvivesReboot(t *testing.T) {
	m := newTestSnapshotManager()
	active := newTestSnapshot(t)
	require.NoError(t, m.consume(active))
	installed := <-m.desiredStateChan
	m.MarkApplied(installed)

	// Some node managers restart the container in-place after a machine reboot,
	// retaining the Kubernetes Pod UID. The durable activation boot ID still
	// proves that the previously authorized process epoch ended.
	m.config.bootID = "boot-after-reboot"
	retiring := active.DeepCopy()
	retiring.Status.Phase = nvapi.ComputeDomainCliqueSnapshotPhaseRetiring
	require.NoError(t, m.consume(retiring))
	desired := <-m.desiredStateChan
	require.Equal(t, nvapi.ComputeDomainCliqueRetirementEvidenceReasonNodeReboot, desired.RetirementEvidence.Reason)
	require.Equal(t, types.UID("pod-a-uid"), desired.RetirementEvidence.WitnessPodUID)
	require.Equal(t, "boot-a", desired.RetirementEvidence.ActivationBootID)
	require.Equal(t, "boot-after-reboot", desired.RetirementEvidence.WitnessBootID)
}

func TestSnapshotConsumerRejectsInvalidSnapshots(t *testing.T) {
	tests := map[string]func(*nvapi.ComputeDomainCliqueSnapshot){
		"scope mismatch": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Spec.ComputeDomainUID = types.UID("other")
		},
		"unexpected owner": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			controller := true
			s.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "DaemonSet", Name: "unexpected", UID: "unexpected", Controller: &controller,
			}}
		},
		"hash mismatch": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Hash = "not-the-canonical-hash"
		},
		"invalid phase": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Phase = "Corrupt"
		},
		"invalid IP": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Members[0].PodIP = "not-an-ip"
			setTestSnapshotHash(t, s)
		},
		"duplicate index": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Members[0].Index = s.Status.Members[1].Index
			setTestSnapshotHash(t, s)
		},
		"duplicate Pod UID": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Members[0].PodUID = s.Status.Members[1].PodUID
			setTestSnapshotHash(t, s)
		},
		"duplicate IP": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Members[0].PodIP = s.Status.Members[1].PodIP
			setTestSnapshotHash(t, s)
		},
		"assignment mismatch": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Assignments[0].CurrentPodUID = types.UID("old-pod")
		},
		"duplicate assignment Pod UID": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Assignments[0].CurrentPodUID = s.Status.Assignments[1].CurrentPodUID
		},
		"quarantined member": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Assignments[0].State = nvapi.ComputeDomainCliqueAssignmentStateQuarantined
		},
		"wrong local Pod": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.Members[1].PodUID = types.UID("replacement-pod")
			s.Status.Assignments[0].CurrentPodUID = types.UID("replacement-pod")
			setTestSnapshotHash(t, s)
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			m := newTestSnapshotManager()
			snapshot := newTestSnapshot(t)
			mutate(snapshot)
			require.Error(t, m.consume(snapshot))
			select {
			case <-m.desiredStateChan:
				t.Fatal("invalid snapshot was delivered")
			default:
			}
		})
	}
}

func TestApplyControllerSnapshotRetriesEachFailedStage(t *testing.T) {
	desired := &PersistentAgentDesiredState{
		Members: []*nvapi.ComputeDomainDaemonInfo{{Index: 0, IPAddress: "10.0.0.1"}},
		Receipt: &nvapi.ComputeDomainCliqueSnapshotReceipt{
			SnapshotUID: types.UID("snapshot-uid"), SnapshotGeneration: 7, SnapshotHash: "hash",
		},
	}
	state := &persistentAgentApplyState{desired: desired}
	var hostsCalls, startCalls, restartCalls, checkCalls, receiptCalls int
	failHosts, failStart, failRestart, failCheck, failReceipt := true, true, true, true, true
	ops := persistentAgentApplyOperations{
		updateHosts: func([]*nvapi.ComputeDomainDaemonInfo) (bool, error) {
			hostsCalls++
			if failHosts {
				failHosts = false
				return false, errors.New("hosts failed")
			}
			// Only the first successful write changes the mapping.
			return hostsCalls == 2, nil
		},
		ensureIMEX: func() (bool, error) {
			startCalls++
			if failStart {
				failStart = false
				return false, errors.New("start failed")
			}
			return false, nil
		},
		restartIMEX: func() error {
			restartCalls++
			if failRestart {
				failRestart = false
				return errors.New("restart failed")
			}
			return nil
		},
		checkIMEX: func() error {
			checkCalls++
			if failCheck {
				failCheck = false
				return errors.New("readiness failed")
			}
			return nil
		},
		writeReceipt: func(*nvapi.ComputeDomainCliqueSnapshotReceipt) error {
			receiptCalls++
			if failReceipt {
				failReceipt = false
				return errors.New("receipt failed")
			}
			return nil
		},
	}

	for range 5 {
		require.Error(t, applyPersistentAgentSnapshot(state, ops))
	}
	require.NoError(t, applyPersistentAgentSnapshot(state, ops))
	require.Equal(t, 6, hostsCalls)
	require.Equal(t, 5, startCalls)
	require.Equal(t, 2, restartCalls, "successful restart must not be repeated solely because readiness or receipt persistence failed")
	require.Equal(t, 3, checkCalls)
	require.Equal(t, 2, receiptCalls)
	require.False(t, state.restartRequired)
}

func TestApplyControllerSnapshotFreshProcessNeedsNoRestart(t *testing.T) {
	state := &persistentAgentApplyState{desired: &PersistentAgentDesiredState{
		Members: []*nvapi.ComputeDomainDaemonInfo{{Index: 0}},
		Receipt: &nvapi.ComputeDomainCliqueSnapshotReceipt{SnapshotUID: types.UID("uid"), SnapshotGeneration: 1, SnapshotHash: "hash"},
	}}
	var restartCalls, checkCalls, receiptCalls int
	err := applyPersistentAgentSnapshot(state, persistentAgentApplyOperations{
		updateHosts: func([]*nvapi.ComputeDomainDaemonInfo) (bool, error) { return true, nil },
		ensureIMEX:  func() (bool, error) { return true, nil },
		restartIMEX: func() error {
			restartCalls++
			return nil
		},
		checkIMEX: func() error { checkCalls++; return nil },
		writeReceipt: func(*nvapi.ComputeDomainCliqueSnapshotReceipt) error {
			receiptCalls++
			return nil
		},
	})
	require.NoError(t, err)
	require.Zero(t, restartCalls)
	require.Equal(t, 1, checkCalls)
	require.Equal(t, 1, receiptCalls)
}

func TestApplyControllerSnapshotRetiresBeforePublishingEvidence(t *testing.T) {
	desired := &PersistentAgentDesiredState{RetirementEvidence: &nvapi.ComputeDomainCliqueRetirementEvidenceSpec{
		SnapshotUID: types.UID("snapshot-uid"), SnapshotGeneration: 7, SnapshotHash: "hash",
	}}
	var order []string
	err := applyPersistentAgentSnapshot(&persistentAgentApplyState{desired: desired}, persistentAgentApplyOperations{
		retireIMEX: func() error {
			order = append(order, "stopped-and-reaped")
			return nil
		},
		writeRetirementEvidence: func(state *PersistentAgentDesiredState) error {
			require.Same(t, desired, state)
			order = append(order, "receipt")
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"stopped-and-reaped", "receipt"}, order)
}

func TestApplyControllerSnapshotNeverPublishesReceiptWhenStopFails(t *testing.T) {
	desired := &PersistentAgentDesiredState{RetirementEvidence: &nvapi.ComputeDomainCliqueRetirementEvidenceSpec{
		SnapshotUID: types.UID("snapshot-uid"), SnapshotGeneration: 7, SnapshotHash: "hash",
	}}
	receiptCalls := 0
	err := applyPersistentAgentSnapshot(&persistentAgentApplyState{desired: desired}, persistentAgentApplyOperations{
		retireIMEX: func() error { return errors.New("still running") },
		writeRetirementEvidence: func(*PersistentAgentDesiredState) error {
			receiptCalls++
			return nil
		},
	})
	require.ErrorContains(t, err, "stop and reap")
	require.Zero(t, receiptCalls)
}
