/*
Copyright The Kubernetes Authors.

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
	"reflect"
	"slices"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/metrics"
)

type computeDomainNodeAttestation struct {
	ComputeDomainUID types.UID                         `json:"computeDomainUID"`
	NodeUID          types.UID                         `json:"nodeUID"`
	PodUID           types.UID                         `json:"podUID"`
	ResourceClaimUID types.UID                         `json:"resourceClaimUID"`
	Protocol         nvapi.ComputeDomainCliqueProtocol `json:"protocol"`
}

type attestedNodeCandidate struct {
	computeDomain *nvapi.ComputeDomain
	pod           *corev1.Pod
	claim         *resourceapi.ResourceClaim
	protocol      nvapi.ComputeDomainCliqueProtocol
}

func namespacedIndexKey(namespace, name string) string { return namespace + "\x00" + name }

func workloadPodNodeIndexKeys(obj any) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("expected Pod, got %T", obj)
	}
	if pod.Spec.NodeName == "" || !podUsesGeneratedClaim(pod) {
		return nil, nil
	}
	return []string{pod.Spec.NodeName}, nil
}

func workloadPodUIDIndexKeys(obj any) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("expected Pod, got %T", obj)
	}
	if pod.UID == "" || !podUsesGeneratedClaim(pod) {
		return nil, nil
	}
	return []string{string(pod.UID)}, nil
}

func workloadPodClaimIndexKeys(obj any) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("expected Pod, got %T", obj)
	}
	var keys []string
	for i := range pod.Spec.ResourceClaims {
		if name := pod.Spec.ResourceClaims[i].ResourceClaimName; name != nil {
			keys = append(keys, namespacedIndexKey(pod.Namespace, *name))
		}
		for j := range pod.Status.ResourceClaimStatuses {
			status := &pod.Status.ResourceClaimStatuses[j]
			if status.Name == pod.Spec.ResourceClaims[i].Name && status.ResourceClaimName != nil {
				keys = append(keys, namespacedIndexKey(pod.Namespace, *status.ResourceClaimName))
			}
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys), nil
}

func workloadPodTemplateIndexKeys(obj any) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("expected Pod, got %T", obj)
	}
	var keys []string
	for i := range pod.Spec.ResourceClaims {
		if name := pod.Spec.ResourceClaims[i].ResourceClaimTemplateName; name != nil {
			keys = append(keys, namespacedIndexKey(pod.Namespace, *name))
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys), nil
}

func unwrapDeleted(obj any) any {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return tombstone.Obj
	}
	return obj
}

func (m *ControllerOwnedCliqueManager) enqueueWorkloadPod(obj any) {
	pod, ok := unwrapDeleted(obj).(*corev1.Pod)
	if ok && pod.Spec.NodeName != "" && podUsesGeneratedClaim(pod) {
		m.attestationQueue.Add(pod.Spec.NodeName)
	}
}

func (m *ControllerOwnedCliqueManager) enqueueAttestationNodeChange(previous, current any) {
	previousNode, _ := unwrapDeleted(previous).(*corev1.Node)
	currentNode, _ := unwrapDeleted(current).(*corev1.Node)
	for _, node := range []*corev1.Node{previousNode, currentNode} {
		if node != nil && (node.Labels[gpuCliqueNodeLabelKey] != "" || node.Labels[computeDomainLabelKey] != "" || node.Annotations[computeDomainAttestationAnnotationKey] != "") {
			m.attestationQueue.Add(node.Name)
		}
	}
	if !attestationBoundaryChanged(previousNode, currentNode) {
		return
	}
	cliques := map[string]struct{}{}
	for _, node := range []*corev1.Node{previousNode, currentNode} {
		if node == nil {
			continue
		}
		if cliqueID := node.Labels[gpuCliqueNodeLabelKey]; cliqueID != "" {
			cliques[cliqueID] = struct{}{}
		}
		if startupCliqueID := node.Annotations[computeDomainCliqueStartupAnnotationKey]; startupCliqueID != "" {
			cliques[startupCliqueID] = struct{}{}
		}
	}
	for cliqueID := range cliques {
		objects, err := m.attestationNodeInformer.GetIndexer().ByIndex(physicalCliqueIndex, cliqueID)
		if err != nil {
			continue
		}
		for _, object := range objects {
			if node, ok := object.(*corev1.Node); ok {
				m.attestationQueue.Add(node.Name)
			}
		}
	}
}

func attestationBoundaryChanged(previous, current *corev1.Node) bool {
	if previous == nil || current == nil {
		return true
	}
	for _, key := range []string{gpuCliqueNodeLabelKey, controllerOwnedCliqueIsolationLabelKey, computeDomainLabelKey} {
		if previous.Labels[key] != current.Labels[key] {
			return true
		}
	}
	for _, key := range []string{computeDomainCliqueStartupAnnotationKey, computeDomainAttestationAnnotationKey} {
		if previous.Annotations[key] != current.Annotations[key] {
			return true
		}
	}
	return false
}

func podUsesGeneratedClaim(pod *corev1.Pod) bool {
	for i := range pod.Spec.ResourceClaims {
		if pod.Spec.ResourceClaims[i].ResourceClaimTemplateName != nil {
			return true
		}
	}
	return false
}

func (m *ControllerOwnedCliqueManager) enqueueResourceClaim(obj any) {
	claim, ok := unwrapDeleted(obj).(*resourceapi.ResourceClaim)
	if !ok {
		return
	}
	objects, _ := m.workloadPodInformer.GetIndexer().ByIndex(workloadPodClaimIndex, namespacedIndexKey(claim.Namespace, claim.Name))
	for _, object := range objects {
		m.enqueueWorkloadPod(object)
	}
	for i := range claim.Status.ReservedFor {
		ref := &claim.Status.ReservedFor[i]
		if ref.APIGroup != "" || ref.Resource != "pods" {
			continue
		}
		pods, _ := m.workloadPodInformer.GetIndexer().ByIndex(workloadPodUIDIndex, string(ref.UID))
		for _, object := range pods {
			m.enqueueWorkloadPod(object)
		}
	}
}

func (m *ControllerOwnedCliqueManager) enqueueClaimTemplate(obj any) {
	template, ok := unwrapDeleted(obj).(*resourceapi.ResourceClaimTemplate)
	if !ok {
		return
	}
	objects, _ := m.workloadPodInformer.GetIndexer().ByIndex(workloadPodTemplateIndex, namespacedIndexKey(template.Namespace, template.Name))
	for _, object := range objects {
		m.enqueueWorkloadPod(object)
	}
}

func (m *ControllerOwnedCliqueManager) runAttestationWorker(ctx context.Context) {
	for {
		nodeName, shutdown := m.attestationQueue.Get()
		if shutdown {
			return
		}
		err := m.reconcileNodeAttestation(ctx, nodeName)
		if err != nil {
			klog.Errorf("reconciling ComputeDomain Node attestation %s: %v", nodeName, err)
			m.attestationQueue.AddRateLimited(nodeName)
		} else {
			m.attestationQueue.Forget(nodeName)
		}
		m.attestationQueue.Done(nodeName)
	}
}

func (m *ControllerOwnedCliqueManager) reconcileNodeAttestation(ctx context.Context, nodeName string) error {
	node, err := m.attestationNodeLister.Get(nodeName)
	if err != nil {
		return nil
	}
	podObjects, err := m.workloadPodInformer.GetIndexer().ByIndex(workloadPodNodeIndex, nodeName)
	if err != nil {
		return err
	}
	var candidates []attestedNodeCandidate
	for _, object := range podObjects {
		pod, ok := object.(*corev1.Pod)
		if !ok {
			continue
		}
		candidate, valid := m.validateAttestationCandidate(pod, node)
		if valid {
			candidates = append(candidates, candidate)
		}
	}
	currentCDUID := node.Labels[computeDomainLabelKey]
	slices.SortFunc(candidates, func(a, b attestedNodeCandidate) int {
		aCurrent, bCurrent := string(a.computeDomain.UID) == currentCDUID, string(b.computeDomain.UID) == currentCDUID
		if aCurrent != bCurrent {
			if aCurrent {
				return -1
			}
			return 1
		}
		if c := stringCompare(string(a.computeDomain.UID), string(b.computeDomain.UID)); c != 0 {
			return c
		}
		return stringCompare(string(a.pod.UID), string(b.pod.UID))
	})
	if len(candidates) == 0 {
		return m.publishNodeAttestation(ctx, node, nil)
	}
	// A second scheduled contender must never revoke an already-attested,
	// still-valid incumbent. The reservation makes the incumbent durable; the
	// losing ComputeDomain remains unselected and retries elsewhere.
	for i := range candidates {
		if candidateMatchesNodeAttestation(candidates[i], node) {
			candidates = []attestedNodeCandidate{candidates[i]}
			break
		}
	}
	winner := candidates[0]
	for i := 1; i < len(candidates); i++ {
		if candidates[i].computeDomain.UID != winner.computeDomain.UID {
			return m.publishNodeAttestation(ctx, node, nil)
		}
	}
	cliqueID := node.Labels[gpuCliqueNodeLabelKey]
	startupCliqueID := node.Annotations[computeDomainCliqueStartupAnnotationKey]
	if cliqueID == "" {
		return m.publishNodeAttestation(ctx, node, nil)
	}
	if startupCliqueID != "" && startupCliqueID != cliqueID {
		return m.publishNodeAttestation(ctx, node, nil)
	}
	if winner.protocol == nvapi.ComputeDomainCliqueProtocolControllerV1 &&
		(startupCliqueID != cliqueID || node.Annotations[computeDomainCliqueCapabilityAnnotationKey] != string(nvapi.ComputeDomainCliqueProtocolControllerV1)) {
		return m.publishNodeAttestation(ctx, node, nil)
	}
	isolated, err := m.wholeCliqueIsolated(cliqueID, winner.computeDomain.UID)
	if err != nil {
		return err
	}
	if !isolated {
		return m.publishNodeAttestation(ctx, node, nil)
	}
	if err := m.reservePhysicalClique(ctx, winner.computeDomain, cliqueID, winner.protocol); err != nil {
		// A stale or spoofed routing projection must not survive a failed
		// singleton acquisition. Remove authorization before retrying.
		if cleanupErr := m.publishNodeAttestation(ctx, node, nil); cleanupErr != nil {
			return fmt.Errorf("reserve physical clique: %v; remove stale Node attestation: %w", err, cleanupErr)
		}
		return err
	}
	return m.publishNodeAttestation(ctx, node, &winner)
}

// wholeCliqueIsolated is the explicit scheduling boundary between legacy and
// controller protocols. An operator must reserve every Node currently
// reporting the physical clique for the exact controller-v1 ComputeDomain UID
// before any claim-derived route is published. Admission prevents the legacy
// kubelet writer from adding its route on an isolated Node.
func (m *ControllerOwnedCliqueManager) wholeCliqueIsolated(cliqueID string, cdUID types.UID) (bool, error) {
	objects, err := m.attestationNodeInformer.GetIndexer().ByIndex(physicalCliqueIndex, cliqueID)
	if err != nil {
		return false, err
	}
	if len(objects) == 0 {
		return false, nil
	}
	for _, object := range objects {
		node, ok := object.(*corev1.Node)
		if !ok || node.Labels[controllerOwnedCliqueIsolationLabelKey] != string(cdUID) {
			return false, nil
		}
		currentCliqueID := node.Labels[gpuCliqueNodeLabelKey]
		startupCliqueID := node.Annotations[computeDomainCliqueStartupAnnotationKey]
		if currentCliqueID != cliqueID || startupCliqueID != cliqueID {
			// The immutable startup identity keeps a Node affiliated with this
			// physical clique even after a topology probe removes its routable
			// label. Missing or mismatched current topology is not a fence.
			return false, nil
		}
		route := node.Labels[computeDomainLabelKey]
		if route != "" && (route != string(cdUID) || !validNodeAttestation(node, nvapi.ComputeDomainCliqueProtocolControllerV1)) {
			// A bare or foreign route may already have activated a legacy
			// daemon. Isolation labels and reservation Create are not a
			// runtime fence, so never overwrite that projection.
			return false, nil
		}
	}
	return true, nil
}

func candidateMatchesNodeAttestation(candidate attestedNodeCandidate, node *corev1.Node) bool {
	var attestation computeDomainNodeAttestation
	if node == nil || json.Unmarshal([]byte(node.Annotations[computeDomainAttestationAnnotationKey]), &attestation) != nil {
		return false
	}
	return attestation.ComputeDomainUID == candidate.computeDomain.UID &&
		attestation.NodeUID == node.UID && attestation.PodUID == candidate.pod.UID &&
		attestation.ResourceClaimUID == candidate.claim.UID && attestation.Protocol == candidate.protocol &&
		node.Labels[computeDomainLabelKey] == string(candidate.computeDomain.UID)
}

func validNodeAttestation(node *corev1.Node, protocol nvapi.ComputeDomainCliqueProtocol) bool {
	if node == nil || node.Labels[computeDomainLabelKey] == "" {
		return false
	}
	var attestation computeDomainNodeAttestation
	if err := json.Unmarshal([]byte(node.Annotations[computeDomainAttestationAnnotationKey]), &attestation); err != nil {
		return false
	}
	return attestation.ComputeDomainUID != "" && string(attestation.ComputeDomainUID) == node.Labels[computeDomainLabelKey] &&
		node.Labels[controllerOwnedCliqueIsolationLabelKey] == string(attestation.ComputeDomainUID) &&
		attestation.NodeUID == node.UID && attestation.PodUID != "" && attestation.ResourceClaimUID != "" && attestation.Protocol == protocol
}

// liveNodeAttestationAuthorized revalidates the complete Pod/claim/template
// authorization chain immediately before generation zero can become Active.
// The Node annotation is a routing projection, not an irrevocable credential;
// a stale projection discovered after controller restart must not authorize the
// first durable peer map before the attestation worker has removed it.
func (m *ControllerOwnedCliqueManager) liveNodeAttestationAuthorized(node *corev1.Node) bool {
	if !validNodeAttestation(node, nvapi.ComputeDomainCliqueProtocolControllerV1) {
		return false
	}
	objects, err := m.workloadPodInformer.GetIndexer().ByIndex(workloadPodNodeIndex, node.Name)
	if err != nil {
		return false
	}
	for _, object := range objects {
		pod, ok := object.(*corev1.Pod)
		if !ok {
			continue
		}
		candidate, valid := m.validateAttestationCandidate(pod, node)
		if valid && candidateMatchesNodeAttestation(candidate, node) {
			return true
		}
	}
	return false
}

func stringCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func (m *ControllerOwnedCliqueManager) validateAttestationCandidate(pod *corev1.Pod, node *corev1.Node) (attestedNodeCandidate, bool) {
	if pod == nil || node == nil || pod.Spec.NodeName != node.Name || pod.UID == "" || node.UID == "" ||
		pod.DeletionTimestamp != nil || node.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded ||
		!podConditionTrue(pod.Status.Conditions, corev1.PodScheduled) {
		return attestedNodeCandidate{}, false
	}
	for i := range pod.Spec.ResourceClaims {
		podClaim := &pod.Spec.ResourceClaims[i]
		if podClaim.ResourceClaimTemplateName == nil || podClaim.ResourceClaimName != nil {
			continue
		}
		claimName := generatedClaimName(pod, podClaim.Name)
		if claimName == "" {
			continue
		}
		claim, err := m.resourceClaimLister.ResourceClaims(pod.Namespace).Get(claimName)
		if err != nil || claim.DeletionTimestamp != nil || claim.Status.Allocation == nil || claim.UID == "" {
			continue
		}
		if len(claim.Status.ReservedFor) != 1 || !reservedExactlyForPod(claim.Status.ReservedFor[0], pod) || !metav1.IsControlledBy(claim, pod) {
			continue
		}
		if !allocationSelectsNode(claim.Status.Allocation.NodeSelector, node.Name) {
			continue
		}
		rct, err := m.claimTemplateLister.ResourceClaimTemplates(pod.Namespace).Get(*podClaim.ResourceClaimTemplateName)
		if err != nil || rct.DeletionTimestamp != nil || !reflect.DeepEqual(claim.Spec, rct.Spec.Spec) {
			continue
		}
		config, ok := allocatedChannelConfig(claim)
		if !ok {
			continue
		}
		objects, err := m.computeDomainInformer.GetIndexer().ByIndex("uid", config.DomainID)
		if err != nil || len(objects) != 1 {
			continue
		}
		cd, ok := objects[0].(*nvapi.ComputeDomain)
		if !ok || cd.DeletionTimestamp != nil || cd.Namespace != pod.Namespace || cd.Spec.Channel.ResourceClaimTemplate.Name != rct.Name {
			continue
		}
		protocol, err := computeDomainCliqueProtocol(cd)
		if err != nil || nvapi.EffectiveComputeDomainCliqueProtocol(config.Protocol) != protocol || config.AllocationMode != channelAllocationModeFor(cd, false) {
			continue
		}
		// Legacy routing remains kubelet-owned for brownfield compatibility.
		// Only controller-v1 accepts this controller-issued attestation.
		if protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 {
			continue
		}
		if err := validateExistingResourceClaimTemplate(rct, cd.Namespace, cd.Spec.Channel.ResourceClaimTemplate.Name, cd.UID,
			computeDomainResourceClaimTemplateTargetWorkload, "channel", computeDomainDefaultChannelDeviceClass, protocol, channelAllocationModeFor(cd, false)); err != nil {
			continue
		}
		return attestedNodeCandidate{computeDomain: cd, pod: pod, claim: claim, protocol: protocol}, true
	}
	return attestedNodeCandidate{}, false
}

func generatedClaimName(pod *corev1.Pod, podClaimName string) string {
	for i := range pod.Status.ResourceClaimStatuses {
		status := &pod.Status.ResourceClaimStatuses[i]
		if status.Name == podClaimName && status.ResourceClaimName != nil {
			return *status.ResourceClaimName
		}
	}
	return ""
}

func podConditionTrue(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) bool {
	for i := range conditions {
		if conditions[i].Type == conditionType && conditions[i].Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func reservedExactlyForPod(ref resourceapi.ResourceClaimConsumerReference, pod *corev1.Pod) bool {
	return ref.APIGroup == "" && ref.Resource == "pods" && ref.Name == pod.Name && ref.UID == pod.UID
}

func allocationSelectsNode(selector *corev1.NodeSelector, nodeName string) bool {
	return selector != nil && reflect.DeepEqual(selector.NodeSelectorTerms, []corev1.NodeSelectorTerm{{
		MatchFields: []corev1.NodeSelectorRequirement{{Key: metav1.ObjectNameField, Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName}}},
	}})
}

func allocatedChannelConfig(claim *resourceapi.ResourceClaim) (*nvapi.ComputeDomainChannelConfig, bool) {
	if claim.Status.Allocation == nil {
		return nil, false
	}
	matchingResult := false
	for i := range claim.Status.Allocation.Devices.Results {
		result := &claim.Status.Allocation.Devices.Results[i]
		if result.Driver == DriverName && result.Request == "channel" {
			matchingResult = true
		}
	}
	if !matchingResult {
		return nil, false
	}
	var found *nvapi.ComputeDomainChannelConfig
	for i := range claim.Status.Allocation.Devices.Config {
		config := &claim.Status.Allocation.Devices.Config[i]
		if config.Source != resourceapi.AllocationConfigSourceClaim || config.Opaque == nil || config.Opaque.Driver != DriverName ||
			!reflect.DeepEqual(config.Requests, []string{"channel"}) {
			continue
		}
		decoded, err := runtime.Decode(nvapi.StrictDecoder, config.Opaque.Parameters.Raw)
		if err != nil {
			return nil, false
		}
		channel, ok := decoded.(*nvapi.ComputeDomainChannelConfig)
		if !ok || found != nil {
			return nil, false
		}
		found = channel
	}
	return found, found != nil
}

func (m *ControllerOwnedCliqueManager) publishNodeAttestation(ctx context.Context, node *corev1.Node, candidate *attestedNodeCandidate) error {
	updated := node.DeepCopy()
	if updated.Labels == nil {
		updated.Labels = map[string]string{}
	}
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	if candidate == nil {
		// A bare ComputeDomain route is owned by the legacy kubelet protocol.
		// Controller-v1 reconciliation must not erase it merely because there
		// is no controller-v1 claim candidate on this Node. A controller-owned
		// route is always paired with the attestation annotation and may be
		// removed together when its authorization facts disappear.
		if updated.Annotations[computeDomainAttestationAnnotationKey] == "" {
			return nil
		}
		delete(updated.Labels, computeDomainLabelKey)
		delete(updated.Annotations, computeDomainAttestationAnnotationKey)
	} else {
		attestation := computeDomainNodeAttestation{
			ComputeDomainUID: candidate.computeDomain.UID, NodeUID: node.UID, PodUID: candidate.pod.UID,
			ResourceClaimUID: candidate.claim.UID, Protocol: candidate.protocol,
		}
		encoded, err := json.Marshal(attestation)
		if err != nil {
			return err
		}
		updated.Labels[computeDomainLabelKey] = string(candidate.computeDomain.UID)
		updated.Annotations[computeDomainAttestationAnnotationKey] = string(encoded)
	}
	if reflect.DeepEqual(updated.Labels, node.Labels) && reflect.DeepEqual(updated.Annotations, node.Annotations) {
		return nil
	}
	_, err := m.config.clientsets.Core.CoreV1().Nodes().Update(ctx, updated, metav1.UpdateOptions{})
	observeCliqueAPIAction(metrics.CliqueAPIResourceNode, metrics.CliqueAPIOperationAttestationUpdate, err)
	return err
}
