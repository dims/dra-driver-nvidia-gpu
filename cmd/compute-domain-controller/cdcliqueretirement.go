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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/metrics"
)

// PrepareComputeDomainRetirement advances every published clique through the
// evidence-bearing Active -> Retiring -> Fenced protocol. It deliberately
// runs before DaemonSet or Node-route deletion: the exact published daemon
// Pods are the only agents that can stop and attest their supervised IMEX
// children. The caller may destroy runtime objects only after ready is true.
func (m *ControllerOwnedCliqueManager) PrepareComputeDomainRetirement(ctx context.Context, cd *nvapi.ComputeDomain) (bool, error) {
	if cd == nil || cd.DeletionTimestamp == nil {
		return false, fmt.Errorf("retirement requires a deleting ComputeDomain")
	}
	snapshots, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(m.config.driverNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{computeDomainLabelKey: string(cd.UID)}).String(),
	})
	if err != nil {
		return false, fmt.Errorf("list retirement snapshots: %w", err)
	}
	reservations, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{computeDomainLabelKey: string(cd.UID)}).String(),
	})
	if err != nil {
		return false, fmt.Errorf("list retirement reservations: %w", err)
	}
	snapshotsByClique := make(map[string]*nvapi.ComputeDomainCliqueSnapshot, len(snapshots.Items))
	for i := range snapshots.Items {
		snapshot := &snapshots.Items[i]
		if snapshotsByClique[snapshot.Spec.CliqueID] != nil {
			return false, fmt.Errorf("multiple retirement snapshots retain physical clique %q", snapshot.Spec.CliqueID)
		}
		snapshotsByClique[snapshot.Spec.CliqueID] = snapshot
	}
	for i := range reservations.Items {
		reservation := &reservations.Items[i]
		if reservation.Spec.ComputeDomainUID != cd.UID {
			return false, fmt.Errorf("retirement reservation %s has mismatched ComputeDomain scope", reservation.Name)
		}
		snapshot := snapshotsByClique[reservation.Spec.CliqueID]
		if reservation.Status.Phase == nvapi.ComputeDomainCliqueReservationPhaseActive {
			if snapshot == nil || snapshot.UID != reservation.Status.SnapshotUID {
				return false, fmt.Errorf("activated retirement reservation %s lost its exact snapshot; absence is not fence evidence", reservation.Name)
			}
			if snapshot.Status.Generation == 0 && snapshotEverPublished(snapshot) {
				return false, fmt.Errorf("activated retirement reservation %s has inconsistent generation-zero tombstones", reservation.Name)
			}
		}
	}
	allFenced := true
	for i := range snapshots.Items {
		snapshot := &snapshots.Items[i]
		if snapshot.Spec.ComputeDomainUID != cd.UID || snapshot.Spec.Protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 {
			return false, fmt.Errorf("snapshot %s/%s has mismatched retirement scope", snapshot.Namespace, snapshot.Name)
		}
		if snapshot.Status.Generation == 0 && !snapshotEverPublished(snapshot) {
			continue
		}
		if snapshot.Status.Generation > 0 && snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseFenced {
			ready, err := m.ensureReservationActivation(ctx, snapshot)
			if err != nil {
				return false, fmt.Errorf("verify retirement reservation activation for snapshot %s/%s: %w", snapshot.Namespace, snapshot.Name, err)
			}
			if !ready {
				allFenced = false
				continue
			}
		}
		switch snapshot.Status.Phase {
		case nvapi.ComputeDomainCliqueSnapshotPhaseActive:
			quiesced, err := m.snapshotWorkloadsQuiesced(ctx, cd, snapshot)
			if err != nil {
				return false, err
			}
			if !quiesced {
				allFenced = false
				continue
			}
			updated := snapshot.DeepCopy()
			updated.Status.Phase = nvapi.ComputeDomainCliqueSnapshotPhaseRetiring
			apiMeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
				Type: "RetirementReady", Status: metav1.ConditionFalse,
				Reason:             "WaitingForProcessExitReceipts",
				Message:            "every exact published daemon must stop and reap its IMEX child before retirement can complete",
				ObservedGeneration: snapshot.Generation,
			})
			if _, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(snapshot.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
				observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationStatusUpdate, err)
				return false, err
			}
			observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationStatusUpdate, nil)
			allFenced = false
		case nvapi.ComputeDomainCliqueSnapshotPhaseRetiring:
			complete, err := m.snapshotRetirementReceiptsComplete(ctx, snapshot)
			if err != nil {
				return false, err
			}
			if !complete {
				allFenced = false
				continue
			}
			updated := snapshot.DeepCopy()
			updated.Status.Phase = nvapi.ComputeDomainCliqueSnapshotPhaseFenced
			for j := range updated.Status.Assignments {
				if updated.Status.Assignments[j].EverPublished {
					updated.Status.Assignments[j].State = nvapi.ComputeDomainCliqueAssignmentStateFenced
				}
			}
			apiMeta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{
				Type: "RetirementReady", Status: metav1.ConditionTrue,
				Reason:             "VerifiedProcessExit",
				Message:            "every exact published daemon stopped and reaped its IMEX child",
				ObservedGeneration: snapshot.Generation,
			})
			if _, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(snapshot.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
				observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationStatusUpdate, err)
				return false, err
			}
			observeCliqueAPIAction(metrics.CliqueAPIResourceSnapshot, metrics.CliqueAPIOperationStatusUpdate, nil)
			allFenced = false
		case nvapi.ComputeDomainCliqueSnapshotPhaseFenced:
			// Durable evidence is already committed.
		default:
			return false, fmt.Errorf("published snapshot %s/%s has non-retirable phase %q", snapshot.Namespace, snapshot.Name, snapshot.Status.Phase)
		}
	}
	return allFenced, nil
}

func (m *ControllerOwnedCliqueManager) snapshotWorkloadsQuiesced(ctx context.Context, cd *nvapi.ComputeDomain, snapshot *nvapi.ComputeDomainCliqueSnapshot) (bool, error) {
	if cd.Spec.Channel == nil || cd.Spec.Channel.ResourceClaimTemplate.Name == "" {
		return false, fmt.Errorf("ComputeDomain %s/%s has no workload ResourceClaimTemplate identity", cd.Namespace, cd.Name)
	}
	// A Node attestation records the exact Pod which authorized that Node's
	// route, but it is not a complete inventory of workloads using the same
	// ComputeDomain. Conservatively wait for every Pod which still references
	// the controller-owned workload template. Pod deletion or a terminal phase
	// is the API-level quiescence prerequisite; process-exit receipts below are
	// the separate runtime fence.
	// Use a quorum-backed API read for this destructive transition. Informer
	// state is suitable for routing reconciliation, but stale cache absence is
	// not sufficient evidence that a workload has stopped.
	livePods, err := m.config.clientsets.Core.CoreV1().Pods(cd.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("live-list ComputeDomain workload Pods: %w", err)
	}
	for i := range livePods.Items {
		pod := &livePods.Items[i]
		if podReferencesClaimTemplate(pod, cd.Spec.Channel.ResourceClaimTemplate.Name) &&
			pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return false, nil
		}
	}

	for i := range snapshot.Status.Members {
		member := &snapshot.Status.Members[i]
		node, err := m.attestationNodeLister.Get(member.NodeName)
		if err != nil {
			return false, fmt.Errorf("published member Node %q is unavailable; absence is not workload fence evidence: %w", member.NodeName, err)
		}
		if node.UID != member.NodeUID || node.Labels[computeDomainLabelKey] != string(snapshot.Spec.ComputeDomainUID) {
			return false, fmt.Errorf("published member Node %q no longer has the exact retirement identity", member.NodeName)
		}
		var attestation computeDomainNodeAttestation
		if err := decodeStrictJSON([]byte(node.Annotations[computeDomainAttestationAnnotationKey]), &attestation); err != nil ||
			attestation.ComputeDomainUID != snapshot.Spec.ComputeDomainUID || attestation.NodeUID != member.NodeUID || attestation.PodUID == "" {
			return false, fmt.Errorf("published member Node %q lacks its exact workload attestation", member.NodeName)
		}
		objects, err := m.workloadPodInformer.GetIndexer().ByIndex(workloadPodUIDIndex, string(attestation.PodUID))
		if err != nil {
			return false, err
		}
		if len(objects) > 1 {
			return false, fmt.Errorf("multiple workload Pods have UID %q", attestation.PodUID)
		}
		if len(objects) == 0 {
			continue
		}
		pod, ok := objects[0].(*corev1.Pod)
		if !ok || pod.Namespace != snapshot.Spec.ComputeDomainNamespace || pod.UID != attestation.PodUID {
			return false, fmt.Errorf("attested workload Pod %q has mismatched identity", attestation.PodUID)
		}
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return false, nil
		}
	}
	return true, nil
}

func podReferencesClaimTemplate(pod *corev1.Pod, templateName string) bool {
	for i := range pod.Spec.ResourceClaims {
		if name := pod.Spec.ResourceClaims[i].ResourceClaimTemplateName; name != nil && *name == templateName {
			return true
		}
	}
	return false
}

func (m *ControllerOwnedCliqueManager) snapshotRetirementReceiptsComplete(ctx context.Context, snapshot *nvapi.ComputeDomainCliqueSnapshot) (bool, error) {
	if len(snapshot.Status.Members) == 0 || snapshot.Status.MemberCount != len(snapshot.Status.Members) {
		return false, fmt.Errorf("retiring snapshot %s/%s has an invalid published member set", snapshot.Namespace, snapshot.Name)
	}
	for i := range snapshot.Status.Members {
		member := &snapshot.Status.Members[i]
		pod, err := m.config.clientsets.Core.CoreV1().Pods(snapshot.Namespace).Get(ctx, member.PodName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, fmt.Errorf("published daemon Pod %s/%s disappeared before its process-exit receipt; absence is not fence evidence", snapshot.Namespace, member.PodName)
		}
		if err != nil {
			return false, fmt.Errorf("read published daemon Pod %s/%s: %w", snapshot.Namespace, member.PodName, err)
		}
		if pod.UID != member.PodUID || pod.Spec.NodeName != member.NodeName {
			return false, fmt.Errorf("published daemon Pod %s/%s identity changed before retirement", snapshot.Namespace, member.PodName)
		}
		controller := metav1.GetControllerOf(pod)
		if controller == nil || controller.APIVersion != "apps/v1" || controller.Kind != "DaemonSet" ||
			controller.Name != snapshot.Spec.DaemonSetName || controller.UID != snapshot.Spec.DaemonSetUID {
			return false, fmt.Errorf("published daemon Pod %s/%s owner no longer matches the snapshot DaemonSet", snapshot.Namespace, member.PodName)
		}
		encoded := pod.Annotations[nvapi.ComputeDomainCliqueRetirementReceiptAnnotation]
		if encoded == "" {
			return false, nil
		}
		var receipt nvapi.ComputeDomainCliqueRetirementReceipt
		if err := decodeStrictJSON([]byte(encoded), &receipt); err != nil {
			return false, fmt.Errorf("published daemon Pod %s/%s has malformed retirement receipt: %w", snapshot.Namespace, member.PodName, err)
		}
		expected := nvapi.ComputeDomainCliqueRetirementReceipt{
			ComputeDomainUID: snapshot.Spec.ComputeDomainUID, SnapshotUID: snapshot.UID,
			SnapshotGeneration: snapshot.Status.Generation, SnapshotHash: snapshot.Status.Hash,
			NodeUID: member.NodeUID, PodUID: member.PodUID, Index: member.Index,
		}
		if receipt != expected {
			return false, fmt.Errorf("published daemon Pod %s/%s has a retirement receipt for a different runtime identity", snapshot.Namespace, member.PodName)
		}
	}
	return true, nil
}

func decodeStrictJSON(data []byte, into any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return fmt.Errorf("multiple JSON values")
	} else if err != io.EOF {
		return err
	}
	return nil
}
