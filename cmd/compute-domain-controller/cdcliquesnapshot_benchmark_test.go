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
