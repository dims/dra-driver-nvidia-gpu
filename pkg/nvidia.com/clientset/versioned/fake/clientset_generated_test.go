/*
 * Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fake

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	resourcev1beta1 "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

func TestComputeDomainCliqueSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	seeded := &resourcev1beta1.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "seeded", Namespace: "driver"},
		Spec: resourcev1beta1.ComputeDomainCliqueSnapshotSpec{
			Protocol: "controller-v1",
			CliqueID: "clique-0",
			Capacity: 18,
		},
	}
	client := NewSimpleClientset(seeded)
	snapshots := client.ResourceV1beta1().ComputeDomainCliqueSnapshots("driver")

	got, err := snapshots.Get(ctx, seeded.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get seeded snapshot: %v", err)
	}
	if got.Spec.CliqueID != seeded.Spec.CliqueID {
		t.Fatalf("got clique ID %q, want %q", got.Spec.CliqueID, seeded.Spec.CliqueID)
	}

	created := &resourcev1beta1.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "created", Namespace: "driver"},
		Spec: resourcev1beta1.ComputeDomainCliqueSnapshotSpec{
			Protocol: "controller-v1",
			CliqueID: "clique-1",
			Capacity: 18,
		},
	}
	if _, err := snapshots.Create(ctx, created, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	list, err := snapshots.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("got %d snapshots, want 2", len(list.Items))
	}
}

func TestComputeDomainCliqueRetirementEvidenceRoundTrip(t *testing.T) {
	ctx := context.Background()
	evidence := &resourcev1beta1.ComputeDomainCliqueRetirementEvidence{
		ObjectMeta: metav1.ObjectMeta{Name: "retirement-snapshot-0", Namespace: "driver"},
		Spec: resourcev1beta1.ComputeDomainCliqueRetirementEvidenceSpec{
			Protocol:    resourcev1beta1.ComputeDomainCliqueProtocolControllerV1,
			Reason:      resourcev1beta1.ComputeDomainCliqueRetirementEvidenceReasonNodeReboot,
			SnapshotUID: "snapshot",
			Index:       0,
			NodeName:    "node-a",
			NodeUID:     "node-a-uid",
		},
	}
	client := NewSimpleClientset(evidence)
	evidences := client.ResourceV1beta1().ComputeDomainCliqueRetirementEvidences("driver")

	got, err := evidences.Get(ctx, evidence.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get seeded retirement evidence: %v", err)
	}
	if got.Spec.Reason != evidence.Spec.Reason {
		t.Fatalf("got reason %q, want %q", got.Spec.Reason, evidence.Spec.Reason)
	}
	list, err := evidences.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list retirement evidence: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("got %d retirement evidence objects, want 1", len(list.Items))
	}
}
