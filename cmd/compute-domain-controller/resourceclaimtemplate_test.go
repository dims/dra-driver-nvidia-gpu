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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

func TestChannelAllocationModeFor(t *testing.T) {
	cdWithMode := func(mode string) *nvapi.ComputeDomain {
		cd := &nvapi.ComputeDomain{}
		cd.Spec.Channel = &nvapi.ComputeDomainChannelSpec{AllocationMode: mode}
		return cd
	}

	tests := []struct {
		name        string
		cd          *nvapi.ComputeDomain
		hostManaged bool
		want        string
	}{
		{
			name:        "host-managed always forces Single, even when the ComputeDomain requested All",
			cd:          cdWithMode(nvapi.ComputeDomainChannelAllocationModeAll),
			hostManaged: true,
			want:        nvapi.ComputeDomainChannelAllocationModeSingle,
		},
		{
			name:        "host-managed always forces Single, even when the ComputeDomain requested nothing",
			cd:          cdWithMode(""),
			hostManaged: true,
			want:        nvapi.ComputeDomainChannelAllocationModeSingle,
		},
		{
			name:        "driver-managed passes through the ComputeDomain's AllocationMode unchanged (All)",
			cd:          cdWithMode(nvapi.ComputeDomainChannelAllocationModeAll),
			hostManaged: false,
			want:        nvapi.ComputeDomainChannelAllocationModeAll,
		},
		{
			name:        "driver-managed passes through the ComputeDomain's AllocationMode unchanged (Single)",
			cd:          cdWithMode(nvapi.ComputeDomainChannelAllocationModeSingle),
			hostManaged: false,
			want:        nvapi.ComputeDomainChannelAllocationModeSingle,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, channelAllocationModeFor(tt.cd, tt.hostManaged))
		})
	}
}

func TestValidateExistingResourceClaimTemplate(t *testing.T) {
	uid := types.UID("cd-uid")
	parameters, err := json.Marshal(nvapi.ComputeDomainDaemonConfig{TypeMeta: metav1.TypeMeta{APIVersion: nvapi.GroupName + "/" + nvapi.Version, Kind: nvapi.ComputeDomainDaemonConfigKind}, DomainID: string(uid), Protocol: nvapi.ComputeDomainCliqueProtocolControllerV1})
	require.NoError(t, err)
	rct := &resourceapi.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "computedomain-daemon-cd-uid", Namespace: "driver",
			Labels: map[string]string{computeDomainLabelKey: string(uid), computeDomainResourceClaimTemplateTargetLabelKey: computeDomainResourceClaimTemplateTargetDaemon},
		},
		Spec: resourceapi.ResourceClaimTemplateSpec{Spec: resourceapi.ResourceClaimSpec{Devices: resourceapi.DeviceClaim{
			Requests: []resourceapi.DeviceRequest{{Name: "daemon", Exactly: &resourceapi.ExactDeviceRequest{DeviceClassName: computeDomainDaemonDeviceClass, AllocationMode: resourceapi.DeviceAllocationModeExactCount, Count: 1}}},
			Config:   []resourceapi.DeviceClaimConfiguration{{Requests: []string{"daemon"}, DeviceConfiguration: resourceapi.DeviceConfiguration{Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: DriverName, Parameters: runtime.RawExtension{Raw: parameters}}}}},
		}}},
	}
	require.NoError(t, validateExistingResourceClaimTemplate(rct, "driver", rct.Name, uid, computeDomainResourceClaimTemplateTargetDaemon, "daemon", computeDomainDaemonDeviceClass, nvapi.ComputeDomainCliqueProtocolControllerV1, ""))

	spoof := rct.DeepCopy()
	spoof.Spec.Spec.Devices.Config[0].Opaque.Driver = "attacker.example.com"
	require.ErrorContains(t, validateExistingResourceClaimTemplate(spoof, "driver", spoof.Name, uid, computeDomainResourceClaimTemplateTargetDaemon, "daemon", computeDomainDaemonDeviceClass, nvapi.ComputeDomainCliqueProtocolControllerV1, ""), "unexpected request")

	adminAccess := true
	spoof = rct.DeepCopy()
	spoof.Spec.Spec.Devices.Requests[0].Exactly.AdminAccess = &adminAccess
	require.ErrorContains(t, validateExistingResourceClaimTemplate(spoof, "driver", spoof.Name, uid, computeDomainResourceClaimTemplateTargetDaemon, "daemon", computeDomainDaemonDeviceClass, nvapi.ComputeDomainCliqueProtocolControllerV1, ""), "unexpected request")

	spoof = rct.DeepCopy()
	spoof.Spec.Spec.Devices.Config[0].Opaque.Parameters.Raw = append(spoof.Spec.Spec.Devices.Config[0].Opaque.Parameters.Raw[:len(spoof.Spec.Spec.Devices.Config[0].Opaque.Parameters.Raw)-1], []byte(`,"unknown":"field"}`)...)
	require.ErrorContains(t, validateExistingResourceClaimTemplate(spoof, "driver", spoof.Name, uid, computeDomainResourceClaimTemplateTargetDaemon, "daemon", computeDomainDaemonDeviceClass, nvapi.ComputeDomainCliqueProtocolControllerV1, ""), "decode existing")
}
