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
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
)

func TestCalculateGlobalStatusHostManaged(t *testing.T) {
	m := &ComputeDomainManager{
		config: &ManagerConfig{
			imexConfig: imex.Config{Mode: imex.ModeHostManaged, Isolation: imex.IsolationIMEXDomain},
		},
	}

	// In driver-managed mode this would be NotReady (required nodes are
	// missing). Under host-managed IMEX the controller does not track
	// per-node readiness, so it reports Ready once admitted.
	cd := &nvapi.ComputeDomain{}
	cd.Spec.NumNodes = 8
	require.Equal(t, nvapi.ComputeDomainStatusReady, m.calculateGlobalStatus(cd))
}

func TestCalculateGlobalStatusDriverManagedUnaffected(t *testing.T) {
	m := &ComputeDomainManager{
		config: &ManagerConfig{
			imexConfig: imex.Config{Mode: imex.ModeDriverManaged},
		},
	}

	cd := &nvapi.ComputeDomain{}
	cd.Spec.NumNodes = 8
	require.Equal(t, nvapi.ComputeDomainStatusNotReady, m.calculateGlobalStatus(cd))
}

func TestSelectComputeDomainCliqueProtocol(t *testing.T) {
	tests := []struct {
		name      string
		requested string
		finalized bool
		gate      bool
		api       bool
		want      nvapi.ComputeDomainCliqueProtocol
		wantErr   bool
	}{
		{name: "markerless remains legacy", want: nvapi.ComputeDomainCliqueProtocolLegacyV1},
		{name: "explicit legacy remains legacy", requested: string(nvapi.ComputeDomainCliqueProtocolLegacyV1), gate: true, api: true, want: nvapi.ComputeDomainCliqueProtocolLegacyV1},
		{name: "explicit controller canary", requested: string(nvapi.ComputeDomainCliqueProtocolControllerV1), gate: true, api: true, want: nvapi.ComputeDomainCliqueProtocolControllerV1},
		{name: "controller request fails when gate is off", requested: string(nvapi.ComputeDomainCliqueProtocolControllerV1), api: true, wantErr: true},
		{name: "controller request fails when API is absent", requested: string(nvapi.ComputeDomainCliqueProtocolControllerV1), gate: true, wantErr: true},
		{name: "old finalized object cannot switch protocols", requested: string(nvapi.ComputeDomainCliqueProtocolControllerV1), finalized: true, gate: true, api: true, want: nvapi.ComputeDomainCliqueProtocolLegacyV1},
		{name: "invalid request fails", requested: "future-v9", gate: true, api: true, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cd := &nvapi.ComputeDomain{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				nvapi.ComputeDomainCliqueRequestedProtocolAnnotation: test.requested,
			}}, Spec: nvapi.ComputeDomainSpec{NumNodes: 18}}
			if test.finalized {
				cd.Finalizers = []string{computeDomainFinalizer}
			}
			got, err := selectComputeDomainCliqueProtocol(cd, test.gate, test.api, 18)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestSelectComputeDomainCliqueProtocolRejectsDeclaredSizeAboveCapacity(t *testing.T) {
	cd := &nvapi.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			nvapi.ComputeDomainCliqueRequestedProtocolAnnotation: string(nvapi.ComputeDomainCliqueProtocolControllerV1),
		}},
		Spec: nvapi.ComputeDomainSpec{NumNodes: 19},
	}
	_, err := selectComputeDomainCliqueProtocol(cd, true, true, 18)
	require.ErrorContains(t, err, "exceeds the configured IMEX clique capacity")
}

// NewComputeDomainManager only stores clientsets on the informer factories it
// builds (it never calls them synchronously), so a zero-value ClientSets is
// sufficient here: these tests only assert on which sub-managers get
// constructed, not on their runtime behavior.

func TestNewComputeDomainManagerHostManagedSkipsDaemonAndNodeManagers(t *testing.T) {
	config := &ManagerConfig{
		imexConfig: imex.Config{Mode: imex.ModeHostManaged, Isolation: imex.IsolationIMEXDomain},
	}

	m := NewComputeDomainManager(config)

	// Host-managed IMEX never creates DaemonSets or ComputeDomain node
	// labels, so this machinery (including the DaemonSet manager's nested
	// ComputeDomainClique/status tracking) must not even be constructed.
	require.Nil(t, m.daemonSetManager, "daemonSetManager must not be constructed under host-managed IMEX")
	require.Nil(t, m.nodeManager, "nodeManager must not be constructed under host-managed IMEX")
	require.NotNil(t, m.resourceClaimTemplateManager, "resourceClaimTemplateManager is still needed to manage the workload ResourceClaimTemplate")
}

func TestNewComputeDomainManagerDriverManagedConstructsDaemonAndNodeManagers(t *testing.T) {
	config := &ManagerConfig{
		imexConfig: imex.Config{Mode: imex.ModeDriverManaged},
	}

	m := NewComputeDomainManager(config)

	require.NotNil(t, m.daemonSetManager)
	require.NotNil(t, m.nodeManager)
	require.NotNil(t, m.resourceClaimTemplateManager)
}
