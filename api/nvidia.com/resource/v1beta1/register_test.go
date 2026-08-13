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
			ComputeDomainUID:       types.UID("compute-domain-uid"),
			ComputeDomainName:      "test-domain",
			ComputeDomainNamespace: "test-namespace",
			CliqueID:               "clique-0",
			Capacity:               18,
			Protocol:               ComputeDomainCliqueProtocolControllerV1,
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
				Index:        0,
				NodeName:     "node-0",
				NodeUID:      types.UID("node-uid"),
				PodName:      "daemon-0",
				PodUID:       types.UID("pod-uid"),
				PodIP:        "10.0.0.1",
				DaemonSetUID: types.UID("daemonset-uid"),
			}},
			MemberCount: 1,
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
			scheme := runtime.NewScheme()
			require.NoError(t, AddToScheme(scheme))
			codecs := serializer.NewCodecFactory(scheme)

			encoded, err := runtime.Encode(codecs.LegacyCodec(SchemeGroupVersion), test.in)
			require.NoError(t, err)

			decoded, gvk, err := codecs.UniversalDecoder(SchemeGroupVersion).Decode(encoded, nil, nil)
			require.NoError(t, err)
			require.Equal(t, test.in.GetObjectKind().GroupVersionKind(), *gvk)
			require.Equal(t, test.in, decoded)
		})
	}
}

func TestComputeDomainCliqueReservationSchemeRoundTrip(t *testing.T) {
	reservation := &ComputeDomainCliqueReservation{
		TypeMeta:   metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: ComputeDomainCliqueReservationKind},
		ObjectMeta: metav1.ObjectMeta{Name: "clique-deadbeef"},
		Spec: ComputeDomainCliqueReservationSpec{
			CliqueID:               "rack-0",
			ComputeDomainUID:       types.UID("compute-domain-uid"),
			ComputeDomainName:      "test-domain",
			ComputeDomainNamespace: "test-namespace",
			Protocol:               ComputeDomainCliqueProtocolControllerV1,
		},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, AddToScheme(scheme))
	codecs := serializer.NewCodecFactory(scheme)
	encoded, err := runtime.Encode(codecs.LegacyCodec(SchemeGroupVersion), reservation)
	require.NoError(t, err)
	decoded, gvk, err := codecs.UniversalDecoder(SchemeGroupVersion).Decode(encoded, nil, nil)
	require.NoError(t, err)
	require.Equal(t, reservation.GetObjectKind().GroupVersionKind(), *gvk)
	require.Equal(t, reservation, decoded)
}
