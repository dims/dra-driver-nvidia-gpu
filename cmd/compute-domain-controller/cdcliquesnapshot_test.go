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
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/cdclique"
)

func TestFirstFreeIndex(t *testing.T) {
	require.Equal(t, 0, firstFreeIndex(nil, 18))
	require.Equal(t, 1, firstFreeIndex(map[int]struct{}{0: {}, 2: {}}, 18))
	require.Equal(t, -1, firstFreeIndex(map[int]struct{}{0: {}, 1: {}}, 2))
}

func TestAllocateSelectedNodesIsIndependentOfEventOrder(t *testing.T) {
	nodes := make([]*corev1.Node, 144)
	for i := range nodes {
		nodes[i] = &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("node-%03d", i), UID: types.UID(fmt.Sprintf("uid-%03d", i)),
		}}
	}
	want, blocked, err := allocateSelectedNodes(nil, nodes, 144)
	require.NoError(t, err)
	require.False(t, blocked)

	for seed := int64(0); seed < 100; seed++ {
		permuted := slices.Clone(nodes)
		rand.New(rand.NewSource(seed)).Shuffle(len(permuted), func(i, j int) { //nolint:gosec
			permuted[i], permuted[j] = permuted[j], permuted[i]
		})
		got, gotBlocked, gotErr := allocateSelectedNodes(nil, permuted, 144)
		require.NoError(t, gotErr)
		require.False(t, gotBlocked)
		require.Equal(t, want, got)
	}
}

func TestAllocateSelectedNodesPreservesIncumbentsAndQuarantine(t *testing.T) {
	existing := []nvapi.ComputeDomainCliqueAssignment{{
		NodeName: "old", NodeUID: types.UID("old-uid"), Index: 7,
		State: nvapi.ComputeDomainCliqueAssignmentStateQuarantined, EverPublished: true,
	}}
	nodes := []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "new", UID: types.UID("new-uid")}}}
	got, blocked, err := allocateSelectedNodes(existing, nodes, 8)
	require.NoError(t, err)
	require.False(t, blocked)
	require.Len(t, got, 2)
	require.Equal(t, 7, got[1].Index)
	require.True(t, got[1].EverPublished)
}

func TestAllocateSelectedNodesFullCapacityAndCorruption(t *testing.T) {
	full := []nvapi.ComputeDomainCliqueAssignment{
		{NodeName: "a", NodeUID: types.UID("a"), Index: 0, EverPublished: true},
		{NodeName: "b", NodeUID: types.UID("b"), Index: 1, EverPublished: true},
	}
	nodes := []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "a", UID: types.UID("a")}},
		{ObjectMeta: metav1.ObjectMeta{Name: "b", UID: types.UID("b")}},
		{ObjectMeta: metav1.ObjectMeta{Name: "c", UID: types.UID("c")}},
	}
	got, blocked, err := allocateSelectedNodes(full, nodes, 2)
	require.NoError(t, err)
	require.True(t, blocked)
	require.Equal(t, full, got, "capacity pressure must not recycle a published slot")

	duplicate := slices.Clone(full)
	duplicate[1].Index = 0
	_, _, err = allocateSelectedNodes(duplicate, nodes[:2], 2)
	require.ErrorContains(t, err, "duplicated")

	outOfRange := slices.Clone(full)
	outOfRange[1].Index = 2
	_, _, err = allocateSelectedNodes(outOfRange, nodes[:2], 2)
	require.ErrorContains(t, err, "out of range")

	duplicateNode := slices.Clone(full)
	duplicateNode[1].NodeUID = duplicateNode[0].NodeUID
	_, _, err = allocateSelectedNodes(duplicateNode, nodes[:2], 2)
	require.ErrorContains(t, err, "Node UID")
}

func TestCompareResourceVersionArbitraryLength(t *testing.T) {
	comparison, err := compareResourceVersion("10000000000000000000000000000000000000000", "9999999999999999999999999999999999999999")
	require.NoError(t, err)
	require.Equal(t, 1, comparison)
	_, err = compareResourceVersion("01", "2")
	require.Error(t, err)
}

func TestSnapshotNameIsBoundedAndCliqueSpecific(t *testing.T) {
	name := cdclique.SnapshotName("12345678-1234-1234-1234-123456789abc", "rack.example.com/clique:144")
	require.LessOrEqual(t, len(name), 253)
	require.NotEqual(t, name, cdclique.SnapshotName("12345678-1234-1234-1234-123456789abc", "other"))
}

func TestSnapshotHashIsStableOnlyForCanonicalOrder(t *testing.T) {
	members := []nvapi.ComputeDomainCliqueMember{
		{Index: 1, NodeUID: types.UID("node-b"), PodUID: types.UID("pod-b"), PodIP: "10.0.0.2"},
		{Index: 0, NodeUID: types.UID("node-a"), PodUID: types.UID("pod-a"), PodIP: "10.0.0.1"},
	}
	ordered := slices.Clone(members)
	slices.SortFunc(ordered, func(a, b nvapi.ComputeDomainCliqueMember) int { return a.Index - b.Index })
	hash1, err := cdclique.CanonicalHash(ordered)
	require.NoError(t, err)
	hash2, err := cdclique.CanonicalHash(slices.Clone(ordered))
	require.NoError(t, err)
	require.Equal(t, hash1, hash2)

	members[0].PodIP = "10.0.0.3"
	slices.SortFunc(members, func(a, b nvapi.ComputeDomainCliqueMember) int { return a.Index - b.Index })
	hash3, err := cdclique.CanonicalHash(members)
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash3)
}

func TestInitialBatchSignatureIgnoresInformerOrder(t *testing.T) {
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	defer queue.ShutDown()
	m := &ControllerOwnedCliqueManager{
		queue:            queue,
		batchStarted:     make(map[string]time.Time),
		batchLastChanged: make(map[string]time.Time),
		batchSignature:   make(map[string]string),
	}
	nodes := []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{UID: types.UID("node-b")}},
		{ObjectMeta: metav1.ObjectMeta{UID: types.UID("node-a")}},
	}
	members := []nvapi.ComputeDomainCliqueMember{{PodUID: types.UID("pod-b")}, {PodUID: types.UID("pod-a")}}
	require.False(t, m.batchAllowsInitialWrite("ns/name", nodes, members, true))
	started := m.batchStarted["ns/name"]
	lastChanged := m.batchLastChanged["ns/name"]
	slices.Reverse(nodes)
	slices.Reverse(members)
	require.False(t, m.batchAllowsInitialWrite("ns/name", nodes, members, true))
	require.Equal(t, started, m.batchStarted["ns/name"])
	require.Equal(t, lastChanged, m.batchLastChanged["ns/name"])
}

func TestInitialBatchHardDeadlineCannotBeResetByChurn(t *testing.T) {
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	defer queue.ShutDown()
	m := &ControllerOwnedCliqueManager{
		queue:            queue,
		batchStarted:     map[string]time.Time{"ns/name": time.Now().Add(-snapshotHardDeadline)},
		batchLastChanged: map[string]time.Time{"ns/name": time.Now()},
		batchSignature:   map[string]string{"ns/name": "old"},
	}
	require.True(t, m.batchAllowsInitialWrite(
		"ns/name",
		[]*corev1.Node{{ObjectMeta: metav1.ObjectMeta{UID: types.UID("new-node")}}},
		nil,
		false,
	))
	require.NotContains(t, m.batchStarted, "ns/name")
}

func TestPhysicalCliqueReservationNameIsGlobalAndBounded(t *testing.T) {
	require.Equal(t, cdclique.ReservationName("rack-1"), cdclique.ReservationName("rack-1"))
	require.NotEqual(t, cdclique.ReservationName("rack-1"), cdclique.ReservationName("rack-2"))
	require.LessOrEqual(t, len(cdclique.ReservationName(string(make([]byte, 1024)))), 253)
}

func TestValidateExistingSnapshotRejectsNonCanonicalScope(t *testing.T) {
	controller := true
	cd := &nvapi.ComputeDomain{ObjectMeta: metav1.ObjectMeta{
		Name: "domain", Namespace: "workload", UID: types.UID("cd-uid"),
	}}
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{
		Name: "computedomain-daemon-cd-uid", Namespace: "driver", UID: types.UID("ds-uid"),
	}}
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: cdclique.SnapshotName(string(cd.UID), "clique-a"), Namespace: "driver",
			Labels: map[string]string{computeDomainLabelKey: string(cd.UID), computeDomainCliqueLabelKey: "clique-a"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "DaemonSet", Name: ds.Name, UID: ds.UID, Controller: &controller,
			}},
		},
		Spec: nvapi.ComputeDomainCliqueSnapshotSpec{
			ComputeDomainUID: cd.UID,
			CliqueID:         "clique-a",
			Capacity:         18,
		},
	}
	config := &ManagerConfig{driverNamespace: "driver", maxNodesPerIMEXDomain: 18}
	require.NoError(t, validateExistingSnapshot(snapshot, cd, ds, config))

	tests := map[string]func(*nvapi.ComputeDomainCliqueSnapshot){
		"wrong capacity": func(candidate *nvapi.ComputeDomainCliqueSnapshot) { candidate.Spec.Capacity = 17 },
		"wrong label": func(candidate *nvapi.ComputeDomainCliqueSnapshot) {
			candidate.Labels[computeDomainCliqueLabelKey] = "other"
		},
		"wrong owner": func(candidate *nvapi.ComputeDomainCliqueSnapshot) {
			candidate.OwnerReferences[0].UID = types.UID("other-ds")
		},
		"wrong name": func(candidate *nvapi.ComputeDomainCliqueSnapshot) { candidate.Name = "precreated" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := snapshot.DeepCopy()
			mutate(candidate)
			require.Error(t, validateExistingSnapshot(candidate, cd, ds, config))
		})
	}
}

func TestComputeDomainCliqueProtocol(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        nvapi.ComputeDomainCliqueProtocol
		wantErr     bool
	}{
		{name: "marker-less is legacy", want: nvapi.ComputeDomainCliqueProtocolLegacyV1},
		{name: "controller", annotations: map[string]string{computeDomainProtocolAnnotation: string(nvapi.ComputeDomainCliqueProtocolControllerV1)}, want: nvapi.ComputeDomainCliqueProtocolControllerV1},
		{name: "unknown", annotations: map[string]string{computeDomainProtocolAnnotation: "future-v9"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			protocol, err := computeDomainCliqueProtocol(&nvapi.ComputeDomain{ObjectMeta: metav1.ObjectMeta{Annotations: test.annotations}})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, protocol)
		})
	}
}
