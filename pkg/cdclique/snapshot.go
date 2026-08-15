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

package cdclique

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

const HashHexLength = sha256.Size * 2

func insertUnique[T comparable](set map[T]struct{}, value T) bool {
	if _, found := set[value]; found {
		return false
	}
	set[value] = struct{}{}
	return true
}

func SnapshotName(computeDomainUID, cliqueID string) string {
	digest := sha256.Sum256([]byte(cliqueID))
	return strings.ToLower(fmt.Sprintf("%s.%s", computeDomainUID, hex.EncodeToString(digest[:8])))
}

func ReservationName(cliqueID string) string {
	digest := sha256.Sum256([]byte(cliqueID))
	return "clique-" + hex.EncodeToString(digest[:])
}

func CanonicalHash(members []nvapi.ComputeDomainCliqueMember) (string, error) {
	canonical, err := json.Marshal(members)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

// ValidatePublishedState validates the API-owned structure shared by snapshot
// consumers. Identity local to a daemon or kubelet remains the consumer's job.
func ValidatePublishedState(snapshot *nvapi.ComputeDomainCliqueSnapshot) ([]nvapi.ComputeDomainCliqueMember, error) {
	retiring := snapshot.Status.Phase == nvapi.ComputeDomainCliqueSnapshotPhaseRetiring
	if (!retiring && snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseActive) || snapshot.Status.Generation < 1 || snapshot.Status.Hash == "" {
		return nil, fmt.Errorf("snapshot is not a published generation")
	}
	controller := metav1.GetControllerOf(snapshot)
	if controller == nil || controller.APIVersion != "apps/v1" || controller.Kind != "DaemonSet" ||
		controller.Name == "" || controller.UID == "" {
		return nil, fmt.Errorf("snapshot controller owner is not an exact DaemonSet identity")
	}

	assignments := make(map[types.UID]nvapi.ComputeDomainCliqueAssignment, len(snapshot.Status.Assignments))
	indices := make(map[int]struct{}, len(snapshot.Status.Assignments))
	nodeNames := make(map[string]struct{}, len(snapshot.Status.Assignments))
	podUIDs := make(map[types.UID]struct{}, len(snapshot.Status.Assignments))
	for _, assignment := range snapshot.Status.Assignments {
		if assignment.NodeUID == "" || assignment.NodeName == "" || assignment.Index < 0 || assignment.Index >= snapshot.Spec.Capacity {
			return nil, fmt.Errorf("assignment identity or index is invalid")
		}
		if assignment.State != nvapi.ComputeDomainCliqueAssignmentStateBound && assignment.State != nvapi.ComputeDomainCliqueAssignmentStateQuarantined {
			return nil, fmt.Errorf("assignment state %q is invalid", assignment.State)
		}
		if _, found := assignments[assignment.NodeUID]; found {
			return nil, fmt.Errorf("duplicate assignment Node UID %q", assignment.NodeUID)
		}
		if !insertUnique(indices, assignment.Index) {
			return nil, fmt.Errorf("duplicate assignment index %d", assignment.Index)
		}
		if !insertUnique(nodeNames, assignment.NodeName) {
			return nil, fmt.Errorf("duplicate assignment Node name %q", assignment.NodeName)
		}
		if assignment.CurrentPodUID != "" && !insertUnique(podUIDs, assignment.CurrentPodUID) {
			return nil, fmt.Errorf("duplicate assignment current Pod UID %q", assignment.CurrentPodUID)
		}
		assignments[assignment.NodeUID] = assignment
	}

	members := slices.Clone(snapshot.Status.Members)
	slices.SortFunc(members, func(a, b nvapi.ComputeDomainCliqueMember) int { return cmp.Compare(a.Index, b.Index) })
	hash, err := CanonicalHash(members)
	if err != nil {
		return nil, err
	}
	if hash != snapshot.Status.Hash {
		return nil, fmt.Errorf("snapshot hash does not match canonical membership")
	}

	memberIndices := make(map[int]struct{}, len(members))
	memberNodeUIDs := make(map[types.UID]struct{}, len(members))
	memberNodeNames := make(map[string]struct{}, len(members))
	memberPodUIDs := make(map[types.UID]struct{}, len(members))
	memberPodNames := make(map[string]struct{}, len(members))
	memberPodIPs := make(map[netip.Addr]struct{}, len(members))
	for _, member := range members {
		if member.Index < 0 || member.Index >= snapshot.Spec.Capacity || member.NodeName == "" || member.NodeUID == "" ||
			member.PodName == "" || member.PodUID == "" {
			return nil, fmt.Errorf("member identity or index is invalid")
		}
		podIP, err := netip.ParseAddr(member.PodIP)
		if err != nil || podIP.IsUnspecified() {
			return nil, fmt.Errorf("member Pod IP %q is invalid", member.PodIP)
		}
		podIP = podIP.Unmap()
		if !insertUnique(memberIndices, member.Index) {
			return nil, fmt.Errorf("duplicate member index %d", member.Index)
		}
		if !insertUnique(memberNodeUIDs, member.NodeUID) {
			return nil, fmt.Errorf("duplicate member Node UID %q", member.NodeUID)
		}
		if !insertUnique(memberNodeNames, member.NodeName) {
			return nil, fmt.Errorf("duplicate member Node name %q", member.NodeName)
		}
		if !insertUnique(memberPodUIDs, member.PodUID) {
			return nil, fmt.Errorf("duplicate member Pod UID %q", member.PodUID)
		}
		if !insertUnique(memberPodNames, member.PodName) {
			return nil, fmt.Errorf("duplicate member Pod name %q", member.PodName)
		}
		if !insertUnique(memberPodIPs, podIP) {
			return nil, fmt.Errorf("duplicate member Pod IP %q", member.PodIP)
		}
		assignment, found := assignments[member.NodeUID]
		validState := found && assignment.State == nvapi.ComputeDomainCliqueAssignmentStateBound
		validPod := assignment.CurrentPodUID == member.PodUID
		if retiring {
			validState = found && assignment.EverPublished &&
				(assignment.State == nvapi.ComputeDomainCliqueAssignmentStateBound || assignment.State == nvapi.ComputeDomainCliqueAssignmentStateQuarantined)
			validPod = assignment.CurrentPodUID == "" || assignment.CurrentPodUID == member.PodUID
		}
		if !validState || assignment.Index != member.Index || assignment.NodeName != member.NodeName || !validPod {
			return nil, fmt.Errorf("member at index %d does not match its published assignment", member.Index)
		}
	}
	return members, nil
}
