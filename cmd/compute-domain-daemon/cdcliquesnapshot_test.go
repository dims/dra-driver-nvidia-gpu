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
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

func newTestSnapshotManager() *ComputeDomainCliqueSnapshotManager {
	return &ComputeDomainCliqueSnapshotManager{
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
		},
		desiredStateChan: make(chan *ControllerSnapshotDesiredState, 1),
	}
}

func newTestSnapshot(t *testing.T) *nvapi.ComputeDomainCliqueSnapshot {
	t.Helper()
	controller := true
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cd-uid.0123456789abcdef",
			Namespace: "driver",
			UID:       types.UID("snapshot-uid"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "DaemonSet",
				Name:       "computedomain-daemon-cd-uid",
				UID:        types.UID("daemonset-uid"),
				Controller: &controller,
			}},
		},
		Spec: nvapi.ComputeDomainCliqueSnapshotSpec{
			ComputeDomainUID:       types.UID("cd-uid"),
			ComputeDomainName:      "cd-name",
			ComputeDomainNamespace: "tenant-a",
			CliqueID:               "clique-a",
			DaemonSetName:          "computedomain-daemon-cd-uid",
			DaemonSetUID:           types.UID("daemonset-uid"),
			Capacity:               18,
			Protocol:               nvapi.ComputeDomainCliqueProtocolControllerV1,
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
				{Index: 1, NodeName: "node-b", NodeUID: types.UID("node-b-uid"), NodeBootID: "boot-b", PodName: "pod-b", PodUID: types.UID("pod-b-uid"), PodIP: "10.0.0.2", DaemonSetUID: types.UID("daemonset-uid")},
				{Index: 0, NodeName: "node-a", NodeUID: types.UID("node-a-uid"), NodeBootID: "boot-a", PodName: "pod-a", PodUID: types.UID("pod-a-uid"), PodIP: "10.0.0.1", DaemonSetUID: types.UID("daemonset-uid")},
			},
			MemberCount: 2,
		},
	}
	setTestSnapshotHash(t, snapshot)
	return snapshot
}

func setTestSnapshotHash(t *testing.T, snapshot *nvapi.ComputeDomainCliqueSnapshot) {
	t.Helper()
	members := slices.Clone(snapshot.Status.Members)
	slices.SortFunc(members, func(a, b nvapi.ComputeDomainCliqueMember) int { return cmp.Compare(a.Index, b.Index) })
	hash, err := canonicalSnapshotHash(members)
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
		Protocol: nvapi.ComputeDomainCliqueProtocolControllerV1, Reason: nvapi.ComputeDomainCliqueRetirementEvidenceReasonProcessExit,
		ComputeDomainUID: types.UID("cd-uid"), SnapshotName: active.Name, SnapshotUID: types.UID("snapshot-uid"),
		SnapshotGeneration: 7, SnapshotHash: active.Status.Hash, Index: 0,
		NodeName: "node-a", NodeUID: types.UID("node-a-uid"), ActivationBootID: "boot-a", WitnessBootID: "boot-a",
		OriginalPodName: "pod-a", OriginalPodUID: types.UID("pod-a-uid"), WitnessPodName: "pod-a", WitnessPodUID: types.UID("pod-a-uid"),
		DaemonSetName: active.Spec.DaemonSetName, DaemonSetUID: active.Spec.DaemonSetUID,
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
	require.Equal(t, types.UID("pod-a-uid"), desired.RetirementEvidence.OriginalPodUID)
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
	require.Equal(t, types.UID("pod-a-uid"), desired.RetirementEvidence.OriginalPodUID)
	require.Equal(t, types.UID("replacement-uid"), desired.RetirementEvidence.WitnessPodUID)
	require.Equal(t, "boot-a", desired.RetirementEvidence.ActivationBootID)
	require.Equal(t, "boot-after-reboot", desired.RetirementEvidence.WitnessBootID)
}

func TestSnapshotConsumerRejectsInvalidSnapshots(t *testing.T) {
	tests := map[string]func(*nvapi.ComputeDomainCliqueSnapshot){
		"protocol mismatch": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Spec.Protocol = nvapi.ComputeDomainCliqueProtocolLegacyV1
		},
		"scope mismatch": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Spec.ComputeDomainUID = types.UID("other")
		},
		"owner mismatch": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.OwnerReferences[0].UID = types.UID("other-daemonset")
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
		"wrong member count": func(s *nvapi.ComputeDomainCliqueSnapshot) {
			s.Status.MemberCount++
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
	desired := &ControllerSnapshotDesiredState{
		Members: []*nvapi.ComputeDomainDaemonInfo{{Index: 0, IPAddress: "10.0.0.1"}},
		Receipt: &nvapi.ComputeDomainCliqueSnapshotReceipt{
			SnapshotUID: types.UID("snapshot-uid"), SnapshotGeneration: 7, SnapshotHash: "hash",
		},
	}
	state := &controllerSnapshotApplyState{desired: desired}
	var hostsCalls, startCalls, restartCalls, checkCalls, receiptCalls int
	failHosts, failStart, failRestart, failCheck, failReceipt := true, true, true, true, true
	ops := controllerSnapshotApplyOperations{
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
		require.Error(t, applyControllerSnapshot(state, ops))
	}
	require.NoError(t, applyControllerSnapshot(state, ops))
	require.Equal(t, 6, hostsCalls)
	require.Equal(t, 5, startCalls)
	require.Equal(t, 2, restartCalls, "successful restart must not be repeated solely because readiness or receipt persistence failed")
	require.Equal(t, 3, checkCalls)
	require.Equal(t, 2, receiptCalls)
	require.False(t, state.restartRequired)
}

func TestApplyControllerSnapshotFreshProcessNeedsNoRestart(t *testing.T) {
	state := &controllerSnapshotApplyState{desired: &ControllerSnapshotDesiredState{
		Members: []*nvapi.ComputeDomainDaemonInfo{{Index: 0}},
		Receipt: &nvapi.ComputeDomainCliqueSnapshotReceipt{SnapshotUID: types.UID("uid"), SnapshotGeneration: 1, SnapshotHash: "hash"},
	}}
	var restartCalls, checkCalls, receiptCalls int
	err := applyControllerSnapshot(state, controllerSnapshotApplyOperations{
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
	desired := &ControllerSnapshotDesiredState{RetirementEvidence: &nvapi.ComputeDomainCliqueRetirementEvidenceSpec{
		SnapshotUID: types.UID("snapshot-uid"), SnapshotGeneration: 7, SnapshotHash: "hash",
	}}
	var order []string
	err := applyControllerSnapshot(&controllerSnapshotApplyState{desired: desired}, controllerSnapshotApplyOperations{
		retireIMEX: func() error {
			order = append(order, "stopped-and-reaped")
			return nil
		},
		writeRetirementEvidence: func(state *ControllerSnapshotDesiredState) error {
			require.Same(t, desired, state)
			order = append(order, "receipt")
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"stopped-and-reaped", "receipt"}, order)
}

func TestApplyControllerSnapshotNeverPublishesReceiptWhenStopFails(t *testing.T) {
	desired := &ControllerSnapshotDesiredState{RetirementEvidence: &nvapi.ComputeDomainCliqueRetirementEvidenceSpec{
		SnapshotUID: types.UID("snapshot-uid"), SnapshotGeneration: 7, SnapshotHash: "hash",
	}}
	receiptCalls := 0
	err := applyControllerSnapshot(&controllerSnapshotApplyState{desired: desired}, controllerSnapshotApplyOperations{
		retireIMEX: func() error { return errors.New("still running") },
		writeRetirementEvidence: func(*ControllerSnapshotDesiredState) error {
			receiptCalls++
			return nil
		},
	})
	require.ErrorContains(t, err, "stop and reap")
	require.Zero(t, receiptCalls)
}
