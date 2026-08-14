//go:build controllerownedcdctest

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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pkgflags "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

func TestTestOnlyGPUCliqueProvider(t *testing.T) {
	tests := []struct {
		name             string
		annotation       *string
		hardwareClique   string
		expectedClique   string
		expectedError    string
		expectedHardware bool
	}{
		{name: "persistent synthetic value", annotation: ptrTo("synthetic-clique-a"), hardwareClique: "hardware-clique", expectedClique: "synthetic-clique-a"},
		{name: "explicit empty", annotation: ptrTo(testOnlyGPUCliqueEmpty), hardwareClique: "hardware-clique", expectedClique: ""},
		{name: "explicit error", annotation: ptrTo(testOnlyGPUCliqueError), hardwareClique: "hardware-clique", expectedError: "injected GPU clique discovery error"},
		{name: "absent annotation delegates to hardware", hardwareClique: "hardware-clique", expectedClique: "hardware-clique", expectedHardware: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			annotations := map[string]string{}
			if test.annotation != nil {
				annotations[testOnlyGPUCliqueAnnotation] = *test.annotation
			}
			coreClient := &recordingCoreClient{nodes: map[string]*corev1.Node{
				testNodeName: {ObjectMeta: metav1.ObjectMeta{Name: testNodeName, Annotations: annotations}},
			}}
			config := &Config{
				flags:      &Flags{nodeName: testNodeName},
				clientsets: pkgflags.ClientSets{Core: coreClient},
			}
			hardwareCalled := false
			provider := gpuCliqueIDProvider(config, func() (string, error) {
				hardwareCalled = true
				return test.hardwareClique, nil
			})
			clique, err := provider()
			if test.expectedError != "" {
				require.ErrorContains(t, err, test.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedClique, clique)
			}
			require.Equal(t, test.expectedHardware, hardwareCalled)
		})
	}
}

func TestTestOnlyGPUCliqueProviderSurvivesManagerRestart(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: testNodeName,
		Annotations: map[string]string{
			testOnlyGPUCliqueAnnotation: "synthetic-clique-a",
		},
	}}
	coreClient := &recordingCoreClient{nodes: map[string]*corev1.Node{testNodeName: node}}
	config := &Config{
		flags: &Flags{
			nodeName:                    testNodeName,
			namespace:                   testDriverNamespace,
			kubeletPluginsDirectoryPath: t.TempDir(),
			gpuCliqueLabelEnabled:       true,
		},
		clientsets: pkgflags.ClientSets{Core: coreClient, Nvidia: nvfake.NewSimpleClientset()},
	}
	hardwareProvider := func() (string, error) {
		t.Fatal("the synthetic annotation must remain authoritative across manager restart")
		return "", nil
	}

	first, err := NewComputeDomainManager(config, gpuCliqueIDProvider(config, hardwareProvider))
	require.NoError(t, err)
	require.NoError(t, first.SetGPUCliqueLabel(context.Background()))

	second, err := NewComputeDomainManager(config, gpuCliqueIDProvider(config, hardwareProvider))
	require.NoError(t, err)
	require.NoError(t, second.SetGPUCliqueLabel(context.Background()))
	require.Equal(t, "synthetic-clique-a", second.CliqueID())
	require.Equal(t, "synthetic-clique-a", coreClient.nodes[testNodeName].Labels[gpuCliqueLabelKey])
	require.Equal(t, "synthetic-clique-a", coreClient.nodes[testNodeName].Annotations[computeDomainCliqueStartupAnnotationKey])
}

func ptrTo(value string) *string {
	return &value
}
