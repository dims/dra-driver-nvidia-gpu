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

package v1beta1

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
)

func TestComputeDomainCliqueSnapshotSchemeRoundTrip(t *testing.T) {
	snapshot := ComputeDomainCliqueSnapshot{
		TypeMeta: metav1.TypeMeta{
			APIVersion: SchemeGroupVersion.String(),
			Kind:       ComputeDomainCliqueSnapshotKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test-namespace",
			Name:      "test-snapshot",
			UID:       types.UID("snapshot-uid"),
		},
		Spec: ComputeDomainCliqueSnapshotSpec{
			ComputeDomainUID: types.UID("compute-domain-uid"),
			CliqueID:         "clique-0",
			Capacity:         18,
		},
		Status: ComputeDomainCliqueSnapshotStatus{
			Phase:      ComputeDomainCliqueSnapshotPhaseActive,
			Generation: 1,
			Hash:       "snapshot-hash",
			Assignments: []ComputeDomainCliqueAssignment{{
				NodeName:      "node-0",
				NodeUID:       types.UID("node-uid"),
				Index:         0,
				State:         ComputeDomainCliqueAssignmentStateBound,
				EverPublished: true,
			}},
			Members: []ComputeDomainCliqueMember{{
				Index:    0,
				NodeName: "node-0",
				NodeUID:  types.UID("node-uid"),
				PodName:  "daemon-0",
				PodUID:   types.UID("pod-uid"),
				PodIP:    "10.0.0.1",
			}},
		},
	}

	tests := []struct {
		name string
		in   runtime.Object
	}{
		{
			name: "snapshot",
			in:   &snapshot,
		},
		{
			name: "snapshot list",
			in: &ComputeDomainCliqueSnapshotList{
				TypeMeta: metav1.TypeMeta{
					APIVersion: SchemeGroupVersion.String(),
					Kind:       ComputeDomainCliqueSnapshotKind + "List",
				},
				Items: []ComputeDomainCliqueSnapshot{snapshot},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertSchemeRoundTrip(t, test.in)
		})
	}
}

func TestComputeDomainCliqueReservationSchemeRoundTrip(t *testing.T) {
	reservation := &ComputeDomainCliqueReservation{
		TypeMeta:   metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: ComputeDomainCliqueReservationKind},
		ObjectMeta: metav1.ObjectMeta{Name: "clique-deadbeef"},
		Spec: ComputeDomainCliqueReservationSpec{
			CliqueID:         "rack-0",
			ComputeDomainUID: types.UID("compute-domain-uid"),
		},
	}
	assertSchemeRoundTrip(t, reservation)
}

func TestComputeDomainCliqueRetirementEvidenceSchemeRoundTrip(t *testing.T) {
	evidence := ComputeDomainCliqueRetirementEvidence{
		TypeMeta:   metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: ComputeDomainCliqueRetirementEvidenceKind},
		ObjectMeta: metav1.ObjectMeta{Name: "retirement-snapshot-0", Namespace: "driver", UID: types.UID("evidence-uid")},
		Spec: ComputeDomainCliqueRetirementEvidenceSpec{
			Reason:             ComputeDomainCliqueRetirementEvidenceReasonNodeReboot,
			SnapshotUID:        types.UID("snapshot-uid"),
			SnapshotGeneration: 3, SnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Index: 0, NodeUID: types.UID("node-uid"), ActivationBootID: "boot-old", WitnessBootID: "boot-new",
			WitnessPodUID: types.UID("pod-new"),
		},
	}
	tests := []runtime.Object{
		&evidence,
		&ComputeDomainCliqueRetirementEvidenceList{
			TypeMeta: metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: ComputeDomainCliqueRetirementEvidenceKind + "List"},
			Items:    []ComputeDomainCliqueRetirementEvidence{evidence},
		},
	}
	for _, object := range tests {
		assertSchemeRoundTrip(t, object)
	}
}

func assertSchemeRoundTrip(t *testing.T, object runtime.Object) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, AddToScheme(scheme))
	codecs := serializer.NewCodecFactory(scheme)
	encoded, err := runtime.Encode(codecs.LegacyCodec(SchemeGroupVersion), object)
	require.NoError(t, err)
	decoded, gvk, err := codecs.UniversalDecoder(SchemeGroupVersion).Decode(encoded, nil, nil)
	require.NoError(t, err)
	require.Equal(t, object.GetObjectKind().GroupVersionKind(), *gvk)
	require.Equal(t, object, decoded)
}
