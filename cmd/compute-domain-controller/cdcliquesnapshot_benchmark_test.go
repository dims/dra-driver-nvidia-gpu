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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

// BenchmarkSnapshotHash144 is a local CPU guard for the largest single-clique
// shape in the design. The API-scale and end-to-end experiments remain
// mandatory before changing the feature-gate default.
func BenchmarkSnapshotHash144(b *testing.B) {
	members := make([]nvapi.ComputeDomainCliqueMember, 144)
	for i := range members {
		members[i] = nvapi.ComputeDomainCliqueMember{
			Index: i, NodeName: fmt.Sprintf("node-%03d", i),
			PodName: fmt.Sprintf("daemon-%03d", i), PodIP: fmt.Sprintf("10.0.0.%d", i+1),
		}
	}
	b.ResetTimer()
	for range b.N {
		if _, err := snapshotHash(members); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocateSelectedNodes144(b *testing.B) {
	nodes := make([]*corev1.Node, 144)
	for i := range nodes {
		nodes[i] = &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("node-%03d", i), UID: types.UID(fmt.Sprintf("uid-%03d", i)),
		}}
	}
	b.ResetTimer()
	for range b.N {
		if _, blocked, err := allocateSelectedNodes(nil, nodes, 144); err != nil || blocked {
			b.Fatalf("allocation failed: blocked=%v err=%v", blocked, err)
		}
	}
}

type indexedReconcileBenchmarkState struct {
	nodes       cache.Indexer
	pods        cache.Indexer
	cdUID       string
	cliqueIDs   []string
	nodeSummary selectedNodeSummary
}

func newIndexedReconcileBenchmarkState(b *testing.B, cliques, membersPerClique int) indexedReconcileBenchmarkState {
	b.Helper()
	state := indexedReconcileBenchmarkState{
		nodes: cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
			computeDomainCliqueIndex: nodeComputeDomainCliqueIndexKeys,
		}),
		pods: cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
			podComputeDomainNodeIndex: podComputeDomainNodeIndexKeys,
		}),
		cdUID:       "benchmark-cd",
		cliqueIDs:   make([]string, cliques),
		nodeSummary: selectedNodeSummary{selected: cliques * membersPerClique, topologyReady: cliques * membersPerClique},
	}
	for clique := range cliques {
		cliqueID := fmt.Sprintf("clique-%03d", clique)
		state.cliqueIDs[clique] = cliqueID
		for member := range membersPerClique {
			nodeName := fmt.Sprintf("node-%03d-%03d", clique, member)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				UID:  types.UID("uid-" + nodeName),
				Labels: map[string]string{
					computeDomainLabelKey:                  state.cdUID,
					gpuCliqueNodeLabelKey:                  cliqueID,
					controllerOwnedCliqueIsolationLabelKey: state.cdUID,
				},
			}}
			setTestNodeAttestation(b, node, state.cdUID, "pod-"+nodeName)
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: "driver",
				Name:      "daemon-" + nodeName,
				UID:       types.UID("pod-" + nodeName),
				Labels:    map[string]string{computeDomainLabelKey: state.cdUID},
			}, Spec: corev1.PodSpec{NodeName: nodeName}}
			if err := state.nodes.Add(node); err != nil {
				b.Fatal(err)
			}
			if err := state.pods.Add(pod); err != nil {
				b.Fatal(err)
			}
		}
	}
	return state
}

func benchmarkIndexedReconcileInputs(b *testing.B, cliques, membersPerClique int) {
	state := newIndexedReconcileBenchmarkState(b, cliques, membersPerClique)
	expected := cliques * membersPerClique
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !expectedSetReadyForSummary(state.nodeSummary, expected) {
			b.Fatal("expected set was not ready")
		}
		observed := 0
		for _, cliqueID := range state.cliqueIDs {
			nodes, err := selectedNodesForClique(state.nodes, state.cdUID, cliqueID)
			if err != nil {
				b.Fatal(err)
			}
			pods, err := candidatePodsForNodes(state.pods, state.cdUID, nodes)
			if err != nil {
				b.Fatal(err)
			}
			observed += len(nodes) + len(pods)
		}
		if observed != 2*expected {
			b.Fatalf("observed %d indexed objects, want %d", observed, 2*expected)
		}
	}
}

func BenchmarkIndexedReconcileInputs18(b *testing.B) {
	benchmarkIndexedReconcileInputs(b, 1, 18)
}

func BenchmarkIndexedReconcileInputs144(b *testing.B) {
	benchmarkIndexedReconcileInputs(b, 1, 144)
}

// BenchmarkIndexedReconcileInputs280x18 measures one complete reconciliation
// wave across the 5,040-Node design shape. Each clique reads only its 18 Nodes
// and per-Node Pod buckets; it does not scan 5,040 Nodes or Pods 280 times.
func BenchmarkIndexedReconcileInputs280x18(b *testing.B) {
	benchmarkIndexedReconcileInputs(b, 280, 18)
}
