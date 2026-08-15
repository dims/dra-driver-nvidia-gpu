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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

func TestValidateExistingDaemonSetNormalizesAPIServerDefaults(t *testing.T) {
	oldTemplatePath := DaemonSetTemplatePath
	DaemonSetTemplatePath = "../../templates/compute-domain-daemon.tmpl.yaml"
	t.Cleanup(func() { DaemonSetTemplatePath = oldTemplatePath })

	cd := &nvapi.ComputeDomain{ObjectMeta: metav1.ObjectMeta{
		Name: "domain", Namespace: "workload", UID: types.UID("cd-uid"),
		Annotations: map[string]string{
			nvapi.ComputeDomainCliqueProtocolAnnotation: string(nvapi.ComputeDomainCliqueProtocolControllerV1),
		},
	}}
	config := &ManagerConfig{
		driverNamespace:       "driver",
		imageName:             "example.invalid/daemon:test",
		maxNodesPerIMEXDomain: 18,
		logVerbosityCDDaemon:  2,
	}
	ds, err := expectedDaemonSet(cd, "computedomain-daemon-cd-uid", config)
	require.NoError(t, err)
	// The expected object itself is API-defaulted, which makes exact security-
	// relevant PodTemplate comparison stable against informer objects.
	require.Equal(t, "Always", string(ds.Spec.Template.Spec.RestartPolicy))
	require.Equal(t, "ClusterFirst", string(ds.Spec.Template.Spec.DNSPolicy))
	require.Equal(t, "default-scheduler", ds.Spec.Template.Spec.SchedulerName)
	require.NoError(t, validateExistingDaemonSet(ds, cd, config))

	rolled := ds.DeepCopy()
	rolled.Spec.Template.Spec.Containers[0].Image = "example.invalid/daemon:old-release"
	for i := range rolled.Spec.Template.Spec.Containers[0].Env {
		if rolled.Spec.Template.Spec.Containers[0].Env[i].Name == "FEATURE_GATES" {
			rolled.Spec.Template.Spec.Containers[0].Env[i].Value = "ControllerOwnedCDCliques=false"
		}
	}
	require.NoError(t, validateExistingDaemonSet(rolled, cd, config), "admission-only gate disable and an old immutable daemon image must remain reconcilable")

	spoof := ds.DeepCopy()
	spoof.Spec.Template.Spec.Containers[0].Env[0].Name = "ATTACKER_CONTROLLED"
	require.Error(t, validateExistingDaemonSet(spoof, cd, config))
}
