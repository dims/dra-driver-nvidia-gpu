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

package v1beta1_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

func TestGeneratedFakeClientSupportsCliqueSnapshot(t *testing.T) {
	ctx := context.Background()
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshot", Namespace: "driver"},
		Spec:       nvapi.ComputeDomainCliqueSnapshotSpec{CliqueID: "clique", Capacity: 18},
	}
	client := nvfake.NewSimpleClientset(snapshot).ResourceV1beta1()

	snapshots := client.ComputeDomainCliqueSnapshots("driver")
	got, err := snapshots.Get(ctx, snapshot.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, snapshot.Spec.CliqueID, got.Spec.CliqueID)
	_, err = snapshots.Create(ctx, &nvapi.ComputeDomainCliqueSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: "driver"}}, metav1.CreateOptions{})
	require.NoError(t, err)
	list, err := snapshots.List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
}
