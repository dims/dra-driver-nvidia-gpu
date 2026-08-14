/*
Copyright The Kubernetes Authors.

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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

func retirementFixture(t *testing.T, workloadPhase corev1.PodPhase) (*ControllerOwnedCliqueManager, *nvapi.ComputeDomain, *nvapi.ComputeDomainCliqueSnapshot, *attestationCoreClient) {
	t.Helper()
	deletedAt := metav1.NewTime(time.Now())
	cd := &nvapi.ComputeDomain{ObjectMeta: metav1.ObjectMeta{
		Name: "domain", Namespace: "workload", UID: types.UID("cd-uid"), DeletionTimestamp: &deletedAt,
		Annotations: map[string]string{nvapi.ComputeDomainCliqueProtocolAnnotation: string(nvapi.ComputeDomainCliqueProtocolControllerV1)},
	}, Spec: nvapi.ComputeDomainSpec{Channel: &nvapi.ComputeDomainChannelSpec{
		ResourceClaimTemplate: nvapi.ComputeDomainResourceClaimTemplate{Name: "workload-template"},
	}}}
	controller := true
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshot", Namespace: "driver", UID: types.UID("snapshot-uid"),
			Labels: map[string]string{computeDomainLabelKey: string(cd.UID)}, Finalizers: []string{nvapi.ComputeDomainCliqueSnapshotFinalizer}},
		Spec: nvapi.ComputeDomainCliqueSnapshotSpec{
			ComputeDomainUID: cd.UID, ComputeDomainName: cd.Name, ComputeDomainNamespace: cd.Namespace,
			CliqueID: "clique", DaemonSetName: "daemonset", DaemonSetUID: types.UID("ds-uid"), Capacity: 18,
			Protocol: nvapi.ComputeDomainCliqueProtocolControllerV1,
		},
		Status: nvapi.ComputeDomainCliqueSnapshotStatus{
			Phase: nvapi.ComputeDomainCliqueSnapshotPhaseActive, Generation: 3,
			Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", MemberCount: 1,
			Assignments: []nvapi.ComputeDomainCliqueAssignment{{
				NodeName: "node", NodeUID: types.UID("node-uid"), Index: 0,
				State: nvapi.ComputeDomainCliqueAssignmentStateBound, EverPublished: true, CurrentPodUID: types.UID("daemon-pod-uid"),
			}},
			Members: []nvapi.ComputeDomainCliqueMember{{
				Index: 0, NodeName: "node", NodeUID: types.UID("node-uid"), PodName: "daemon-pod",
				PodUID: types.UID("daemon-pod-uid"), PodIP: "10.0.0.1", DaemonSetUID: types.UID("ds-uid"),
			}},
		},
	}
	attestation := computeDomainNodeAttestation{
		ComputeDomainUID: cd.UID, NodeUID: types.UID("node-uid"), PodUID: types.UID("workload-pod-uid"),
		ResourceClaimUID: types.UID("claim-uid"), Protocol: nvapi.ComputeDomainCliqueProtocolControllerV1,
	}
	encoded, err := json.Marshal(attestation)
	require.NoError(t, err)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: types.UID("node-uid"),
		Labels:      map[string]string{computeDomainLabelKey: string(cd.UID)},
		Annotations: map[string]string{computeDomainAttestationAnnotationKey: string(encoded)},
	}}
	templateName := "workload-template"
	workload := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "workload-pod", Namespace: cd.Namespace, UID: attestation.PodUID},
		Spec:       corev1.PodSpec{ResourceClaims: []corev1.PodResourceClaim{{Name: "channel", ResourceClaimTemplateName: &templateName}}},
		Status:     corev1.PodStatus{Phase: workloadPhase},
	}
	daemon := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "daemon-pod", Namespace: "driver", UID: types.UID("daemon-pod-uid"),
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "daemonset", UID: types.UID("ds-uid"), Controller: &controller}}},
		Spec: corev1.PodSpec{NodeName: "node"}}
	core := &attestationCoreClient{nodes: map[string]*corev1.Node{node.Name: node.DeepCopy()}, pods: map[string]*corev1.Pod{
		podMapKey("driver", daemon.Name):             daemon.DeepCopy(),
		podMapKey(workload.Namespace, workload.Name): workload.DeepCopy(),
	}}
	reservation := retirementReservation(cd.UID)
	reservation.Status = nvapi.ComputeDomainCliqueReservationStatus{
		Phase: nvapi.ComputeDomainCliqueReservationPhaseActive, SnapshotUID: snapshot.UID,
		ActivationGeneration: 1, ActivationHash: snapshot.Status.Hash,
	}
	nvidia := nvfake.NewSimpleClientset(cd.DeepCopy(), snapshot.DeepCopy(), reservation)
	manager := NewControllerOwnedCliqueManager(&ManagerConfig{driverNamespace: "driver", clientsets: flags.ClientSets{Core: core, Nvidia: nvidia}})
	require.NoError(t, manager.addInformerIndexes())
	require.NoError(t, manager.attestationNodeInformer.GetStore().Add(node.DeepCopy()))
	require.NoError(t, manager.workloadPodInformer.GetStore().Add(workload.DeepCopy()))
	t.Cleanup(func() { manager.queue.ShutDown(); manager.attestationQueue.ShutDown() })
	return manager, cd, snapshot, core
}

func TestRetirementWaitsForAttestedWorkloadPod(t *testing.T) {
	manager, cd, _, _ := retirementFixture(t, corev1.PodRunning)
	ready, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	require.False(t, ready)
}

func TestRetirementWaitsForEveryWorkloadUsingTheTemplate(t *testing.T) {
	manager, cd, _, _ := retirementFixture(t, corev1.PodSucceeded)
	templateName := cd.Spec.Channel.ResourceClaimTemplate.Name
	second := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "second-workload", Namespace: cd.Namespace, UID: types.UID("second-workload-uid")},
		Spec: corev1.PodSpec{ResourceClaims: []corev1.PodResourceClaim{{
			Name: "channel", ResourceClaimTemplateName: &templateName,
		}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	require.NoError(t, manager.workloadPodInformer.GetStore().Add(second))
	manager.config.clientsets.Core.(*attestationCoreClient).putPod(second)

	ready, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	require.False(t, ready)

	second = second.DeepCopy()
	second.Status.Phase = corev1.PodSucceeded
	require.NoError(t, manager.workloadPodInformer.GetStore().Update(second))
	manager.config.clientsets.Core.(*attestationCoreClient).putPod(second)
	ready, err = manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	require.False(t, ready, "quiescence advances Active to Retiring but does not fence in the same reconcile")
}

func TestRetirementFailsClosedWhenLiveWorkloadInventoryFails(t *testing.T) {
	manager, cd, snapshot, core := retirementFixture(t, corev1.PodSucceeded)
	core.podListErr = apierrors.NewTooManyRequests("injected retirement inventory failure", 1)
	ready, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.ErrorContains(t, err, "live-list ComputeDomain workload Pods")
	require.False(t, ready)
	unchanged, getErr := manager.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(snapshot.Namespace).Get(context.Background(), snapshot.Name, metav1.GetOptions{})
	require.NoError(t, getErr)
	require.Equal(t, nvapi.ComputeDomainCliqueSnapshotPhaseActive, unchanged.Status.Phase)
}

func TestRetirementRequiresEveryExactDaemonReceipt(t *testing.T) {
	manager, cd, _, _ := retirementFixture(t, corev1.PodSucceeded)
	ready, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	require.False(t, ready)

	ready, err = manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	require.False(t, ready, "receipt absence must not fence a published daemon")
}

func TestRetirementFencesOnlyAfterExactReceipt(t *testing.T) {
	manager, cd, original, core := retirementFixture(t, corev1.PodSucceeded)
	ready, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	require.False(t, ready)
	retiring, err := manager.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots("driver").Get(context.Background(), original.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, nvapi.ComputeDomainCliqueSnapshotPhaseRetiring, retiring.Status.Phase)

	receipt := nvapi.ComputeDomainCliqueRetirementReceipt{
		ComputeDomainUID: cd.UID, SnapshotUID: retiring.UID, SnapshotGeneration: retiring.Status.Generation,
		SnapshotHash: retiring.Status.Hash, NodeUID: types.UID("node-uid"), PodUID: types.UID("daemon-pod-uid"), Index: 0,
	}
	encoded, err := json.Marshal(receipt)
	require.NoError(t, err)
	pod := core.pods[podMapKey("driver", "daemon-pod")].DeepCopy()
	pod.Annotations = map[string]string{nvapi.ComputeDomainCliqueRetirementReceiptAnnotation: string(encoded)}
	core.pods[podMapKey("driver", pod.Name)] = pod

	ready, err = manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	require.False(t, ready, "Fenced must be observed durably on the next level-based reconcile")
	fenced, err := manager.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots("driver").Get(context.Background(), original.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, nvapi.ComputeDomainCliqueSnapshotPhaseFenced, fenced.Status.Phase)
	require.Equal(t, nvapi.ComputeDomainCliqueAssignmentStateFenced, fenced.Status.Assignments[0].State)

	ready, err = manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	require.True(t, ready)
}

func TestRetirementDoesNotTreatMissingDaemonPodAsFence(t *testing.T) {
	manager, cd, _, core := retirementFixture(t, corev1.PodSucceeded)
	_, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	delete(core.pods, podMapKey("driver", "daemon-pod"))
	ready, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.ErrorContains(t, err, "absence is not fence evidence")
	require.False(t, ready)
}

func TestRetirementRejectsReceiptForDifferentGeneration(t *testing.T) {
	manager, cd, original, core := retirementFixture(t, corev1.PodSucceeded)
	_, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.NoError(t, err)
	receipt := nvapi.ComputeDomainCliqueRetirementReceipt{
		ComputeDomainUID: cd.UID, SnapshotUID: original.UID, SnapshotGeneration: original.Status.Generation + 1,
		SnapshotHash: original.Status.Hash, NodeUID: types.UID("node-uid"), PodUID: types.UID("daemon-pod-uid"), Index: 0,
	}
	encoded, err := json.Marshal(receipt)
	require.NoError(t, err)
	pod := core.pods[podMapKey("driver", "daemon-pod")].DeepCopy()
	pod.Annotations = map[string]string{nvapi.ComputeDomainCliqueRetirementReceiptAnnotation: string(encoded)}
	core.pods[podMapKey("driver", pod.Name)] = pod
	ready, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.ErrorContains(t, err, "different runtime identity")
	require.False(t, ready)
}

func TestRetirementRejectsActivatedReservationWithoutSnapshot(t *testing.T) {
	manager, cd, snapshot, _ := retirementFixture(t, corev1.PodSucceeded)
	require.NoError(t, manager.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(snapshot.Namespace).Delete(
		context.Background(), snapshot.Name, metav1.DeleteOptions{},
	))
	ready, err := manager.PrepareComputeDomainRetirement(context.Background(), cd)
	require.ErrorContains(t, err, "lost its exact snapshot")
	require.False(t, ready)
}

func TestSuccessorReservationClearsFormerActivationMemo(t *testing.T) {
	name := physicalCliqueReservationName("clique")
	manager := &ControllerOwnedCliqueManager{
		validatedReservations: map[string]nvapi.ComputeDomainCliqueReservationSpec{name: {
			CliqueID: "clique", ComputeDomainUID: types.UID("old"), Protocol: nvapi.ComputeDomainCliqueProtocolControllerV1,
		}},
		validatedActivations: map[string]types.UID{name: types.UID("old-snapshot")},
	}
	successor := &nvapi.ComputeDomainCliqueReservation{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: nvapi.ComputeDomainCliqueReservationSpec{
			CliqueID: "clique", ComputeDomainUID: types.UID("new"), Protocol: nvapi.ComputeDomainCliqueProtocolControllerV1,
		},
	}
	require.NoError(t, manager.validateAndRememberReservation(successor, successor.Spec))
	require.Empty(t, manager.validatedActivations[name])
}

func deletionManager(objects ...runtime.Object) *ComputeDomainManager {
	return &ComputeDomainManager{config: &ManagerConfig{
		driverNamespace: "driver",
		clientsets:      flags.ClientSets{Nvidia: nvfake.NewSimpleClientset(objects...)},
	}}
}

func retirementReservation(cdUID types.UID) *nvapi.ComputeDomainCliqueReservation {
	return &nvapi.ComputeDomainCliqueReservation{
		ObjectMeta: metav1.ObjectMeta{
			Name: physicalCliqueReservationName("clique"), UID: types.UID("reservation-uid"),
			Labels:      map[string]string{computeDomainLabelKey: string(cdUID)},
			Annotations: map[string]string{reservationActivationTrackingAnnotationKey: reservationActivationTrackingStatusV1},
		},
		Spec: nvapi.ComputeDomainCliqueReservationSpec{
			CliqueID: "clique", ComputeDomainUID: cdUID, ComputeDomainName: "domain",
			ComputeDomainNamespace: "workload", Protocol: nvapi.ComputeDomainCliqueProtocolControllerV1,
		},
	}
}

func TestDeleteSnapshotsReleasesFencedRuntime(t *testing.T) {
	cdUID := types.UID("cd-uid")
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: "snapshot", Namespace: "driver", UID: types.UID("snapshot-uid"),
			Labels: map[string]string{computeDomainLabelKey: string(cdUID)}, Finalizers: []string{nvapi.ComputeDomainCliqueSnapshotFinalizer},
		},
		Spec: nvapi.ComputeDomainCliqueSnapshotSpec{CliqueID: "clique", ComputeDomainUID: cdUID},
		Status: nvapi.ComputeDomainCliqueSnapshotStatus{
			Phase: nvapi.ComputeDomainCliqueSnapshotPhaseFenced, Generation: 3,
			Hash:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Assignments: []nvapi.ComputeDomainCliqueAssignment{{EverPublished: true}},
		},
	}
	reservation := retirementReservation(cdUID)
	reservation.Status = nvapi.ComputeDomainCliqueReservationStatus{
		Phase: nvapi.ComputeDomainCliqueReservationPhaseActive, SnapshotUID: snapshot.UID,
		ActivationGeneration: 1, ActivationHash: snapshot.Status.Hash,
	}
	manager := deletionManager(snapshot, reservation)
	require.NoError(t, manager.DeleteSnapshots(context.Background(), string(cdUID)))
	_, err := manager.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots("driver").Get(context.Background(), snapshot.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	_, err = manager.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(context.Background(), reservation.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}

func TestDeleteSnapshotsAcceptsLaterFencedGeneration(t *testing.T) {
	cdUID := types.UID("cd-uid")
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: "snapshot", Namespace: "driver", UID: types.UID("snapshot-uid"),
			Labels: map[string]string{computeDomainLabelKey: string(cdUID)}, Finalizers: []string{nvapi.ComputeDomainCliqueSnapshotFinalizer},
		},
		Spec: nvapi.ComputeDomainCliqueSnapshotSpec{CliqueID: "clique", ComputeDomainUID: cdUID},
		Status: nvapi.ComputeDomainCliqueSnapshotStatus{
			Phase: nvapi.ComputeDomainCliqueSnapshotPhaseFenced, Generation: 7,
			Hash:        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Assignments: []nvapi.ComputeDomainCliqueAssignment{{EverPublished: true}},
		},
	}
	reservation := retirementReservation(cdUID)
	reservation.Status = nvapi.ComputeDomainCliqueReservationStatus{
		Phase: nvapi.ComputeDomainCliqueReservationPhaseActive, SnapshotUID: snapshot.UID,
		ActivationGeneration: 1, ActivationHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	manager := deletionManager(snapshot, reservation)
	require.NoError(t, manager.DeleteSnapshots(context.Background(), string(cdUID)))
}

func TestDeleteSnapshotsReleasesTrackedNeverPublishedReservation(t *testing.T) {
	reservation := retirementReservation(types.UID("cd-uid"))
	manager := deletionManager(reservation)
	require.NoError(t, manager.DeleteSnapshots(context.Background(), "cd-uid"))
	_, err := manager.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(context.Background(), reservation.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}

func TestDeleteSnapshotsReleasesCommittedActivationIntentWhenGenerationOneDidNotPublish(t *testing.T) {
	cdUID := types.UID("cd-uid")
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshot", Namespace: "driver", UID: types.UID("snapshot-uid"), Labels: map[string]string{computeDomainLabelKey: string(cdUID)}},
		Spec:       nvapi.ComputeDomainCliqueSnapshotSpec{CliqueID: "clique", ComputeDomainUID: cdUID},
		Status:     nvapi.ComputeDomainCliqueSnapshotStatus{Phase: nvapi.ComputeDomainCliqueSnapshotPhasePending, Generation: 0},
	}
	reservation := retirementReservation(cdUID)
	reservation.Status = nvapi.ComputeDomainCliqueReservationStatus{
		Phase: nvapi.ComputeDomainCliqueReservationPhaseActive, SnapshotUID: snapshot.UID,
		ActivationGeneration: 1, ActivationHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	manager := deletionManager(snapshot, reservation)
	require.NoError(t, manager.DeleteSnapshots(context.Background(), string(cdUID)))
	_, err := manager.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots("driver").Get(context.Background(), snapshot.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
	_, err = manager.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(context.Background(), reservation.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))
}

func TestDeleteSnapshotsBlocksActivatedReservationWithMissingSnapshot(t *testing.T) {
	reservation := retirementReservation(types.UID("cd-uid"))
	reservation.Status = nvapi.ComputeDomainCliqueReservationStatus{
		Phase: nvapi.ComputeDomainCliqueReservationPhaseActive, SnapshotUID: types.UID("snapshot-uid"),
		ActivationGeneration: 1, ActivationHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	manager := deletionManager(reservation)
	require.ErrorContains(t, manager.DeleteSnapshots(context.Background(), "cd-uid"), "absence is not fence evidence")
}

func TestDeleteSnapshotsBlocksLegacyReservationWithMissingSnapshot(t *testing.T) {
	reservation := retirementReservation(types.UID("cd-uid"))
	reservation.Annotations = nil
	manager := deletionManager(reservation)
	require.ErrorContains(t, manager.DeleteSnapshots(context.Background(), "cd-uid"), "predates activation tracking")
}
