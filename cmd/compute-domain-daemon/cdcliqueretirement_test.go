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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	coretyped "k8s.io/client-go/kubernetes/typed/core/v1"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

type retirementPodClient struct {
	kubernetes.Interface
	pod *corev1.Pod
}

func (c *retirementPodClient) CoreV1() coretyped.CoreV1Interface {
	return &retirementCoreV1{client: c}
}

type retirementCoreV1 struct {
	coretyped.CoreV1Interface
	client *retirementPodClient
}

func (c *retirementCoreV1) Pods(namespace string) coretyped.PodInterface {
	return &retirementPods{client: c.client, namespace: namespace}
}

type retirementPods struct {
	coretyped.PodInterface
	client    *retirementPodClient
	namespace string
}

func (p *retirementPods) Get(_ context.Context, name string, _ metav1.GetOptions) (*corev1.Pod, error) {
	if p.client.pod == nil || p.client.pod.Namespace != p.namespace || p.client.pod.Name != name {
		return nil, apierrors.NewNotFound(corev1.Resource("pods"), name)
	}
	return p.client.pod.DeepCopy(), nil
}

func (p *retirementPods) Update(_ context.Context, pod *corev1.Pod, _ metav1.UpdateOptions) (*corev1.Pod, error) {
	if p.client.pod == nil || p.client.pod.UID != pod.UID {
		return nil, apierrors.NewNotFound(corev1.Resource("pods"), pod.Name)
	}
	p.client.pod = pod.DeepCopy()
	return pod.DeepCopy(), nil
}

func TestPublishRetirementEvidenceIsExactDurableAndIdempotent(t *testing.T) {
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "daemon", Namespace: "driver", UID: types.UID("pod-uid"),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "daemonset", UID: types.UID("daemonset-uid"), Controller: &controller}},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
	}
	core := &retirementPodClient{pod: pod}
	nvidia := nvfake.NewSimpleClientset()
	manager := &ComputeDomainCliqueSnapshotManager{config: &ManagerConfig{
		clientsets: flags.ClientSets{Core: core, Nvidia: nvidia}, podNamespace: pod.Namespace, podName: pod.Name, podUID: string(pod.UID),
	}}
	state := &ControllerSnapshotDesiredState{RetirementEvidence: &nvapi.ComputeDomainCliqueRetirementEvidenceSpec{
		Protocol: nvapi.ComputeDomainCliqueProtocolControllerV1, Reason: nvapi.ComputeDomainCliqueRetirementEvidenceReasonProcessExit,
		ComputeDomainUID: types.UID("cd-uid"), SnapshotName: "snapshot", SnapshotUID: types.UID("snapshot-uid"), SnapshotGeneration: 4,
		SnapshotHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Index: 0,
		NodeName: "node-a", NodeUID: types.UID("node-uid"), ActivationBootID: "boot-a", WitnessBootID: "boot-a",
		OriginalPodName: pod.Name, OriginalPodUID: pod.UID, WitnessPodName: pod.Name, WitnessPodUID: pod.UID,
		DaemonSetName: "daemonset", DaemonSetUID: types.UID("daemonset-uid"),
	}}
	require.NoError(t, manager.PublishRetirementEvidence(context.Background(), state))
	require.NoError(t, manager.PublishRetirementEvidence(context.Background(), state))
	evidence, err := nvidia.ResourceV1beta1().ComputeDomainCliqueRetirementEvidences("driver").Get(
		context.Background(), nvapi.ComputeDomainCliqueRetirementEvidenceName(types.UID("snapshot-uid"), 0), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, *state.RetirementEvidence, evidence.Spec)
	require.Empty(t, core.pod.Annotations, "durable evidence must not be stored on the witness Pod")
}

func TestPublishRetirementEvidenceRejectsDifferentWitnessPodUID(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "daemon", Namespace: "driver", UID: types.UID("pod-uid")}, Spec: corev1.PodSpec{NodeName: "node-a"}}
	core := &retirementPodClient{pod: pod}
	manager := &ComputeDomainCliqueSnapshotManager{config: &ManagerConfig{
		clientsets: flags.ClientSets{Core: core, Nvidia: nvfake.NewSimpleClientset()}, podNamespace: pod.Namespace, podName: pod.Name, podUID: string(pod.UID),
	}}
	err := manager.PublishRetirementEvidence(context.Background(), &ControllerSnapshotDesiredState{RetirementEvidence: &nvapi.ComputeDomainCliqueRetirementEvidenceSpec{
		WitnessPodName: pod.Name, WitnessPodUID: types.UID("other"), NodeName: "node-a",
	}})
	require.ErrorContains(t, err, "does not match retirement witness")
	require.Empty(t, core.pod.Annotations)
}
