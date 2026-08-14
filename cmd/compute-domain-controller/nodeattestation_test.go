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
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	coretyped "k8s.io/client-go/kubernetes/typed/core/v1"
	draclient "k8s.io/dynamic-resource-allocation/client"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

type nodeAttestationFixture struct {
	manager *ControllerOwnedCliqueManager
	core    *attestationCoreClient
	nvidia  *nvfake.Clientset
	cd      *nvapi.ComputeDomain
	node    *corev1.Node
	pod     *corev1.Pod
	claim   *resourceapi.ResourceClaim
	rct     *resourceapi.ResourceClaimTemplate
}

func newNodeAttestationFixture(t *testing.T, suffix string, protocol nvapi.ComputeDomainCliqueProtocol) *nodeAttestationFixture {
	t.Helper()
	namespace := "workload"
	cdUID := types.UID("cd-" + suffix)
	nodeName := "node-" + suffix
	podName := "pod-" + suffix
	claimName := "claim-" + suffix
	templateName := "channel-" + suffix
	channel := nvapi.DefaultComputeDomainChannelConfig()
	channel.DomainID = string(cdUID)
	channel.AllocationMode = nvapi.ComputeDomainChannelAllocationModeSingle
	channel.Protocol = protocol
	claimConfigBytes, err := json.Marshal(channel)
	require.NoError(t, err)
	templateChannel := channel.DeepCopyObject().(*nvapi.ComputeDomainChannelConfig)
	if protocol == nvapi.ComputeDomainCliqueProtocolLegacyV1 {
		templateChannel.Protocol = ""
	}
	templateConfigBytes, err := json.Marshal(templateChannel)
	require.NoError(t, err)

	cd := &nvapi.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{Name: "domain-" + suffix, Namespace: namespace, UID: cdUID, Annotations: map[string]string{
			nvapi.ComputeDomainCliqueProtocolAnnotation: string(protocol),
		}},
		Spec: nvapi.ComputeDomainSpec{Channel: &nvapi.ComputeDomainChannelSpec{
			ResourceClaimTemplate: nvapi.ComputeDomainResourceClaimTemplate{Name: templateName},
			AllocationMode:        nvapi.ComputeDomainChannelAllocationModeSingle,
		}},
	}
	claimSpec := resourceapi.ResourceClaimSpec{Devices: resourceapi.DeviceClaim{
		Requests: []resourceapi.DeviceRequest{{Name: "channel", Exactly: &resourceapi.ExactDeviceRequest{
			DeviceClassName: computeDomainDefaultChannelDeviceClass, AllocationMode: resourceapi.DeviceAllocationModeExactCount, Count: 1,
		}}},
		Config: []resourceapi.DeviceClaimConfiguration{{Requests: []string{"channel"}, DeviceConfiguration: resourceapi.DeviceConfiguration{
			Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: DriverName, Parameters: runtime.RawExtension{Raw: templateConfigBytes}},
		}}},
	}}
	rct := &resourceapi.ResourceClaimTemplate{ObjectMeta: metav1.ObjectMeta{
		Name: templateName, Namespace: namespace,
		Labels: map[string]string{computeDomainLabelKey: string(cdUID), computeDomainResourceClaimTemplateTargetLabelKey: computeDomainResourceClaimTemplateTargetWorkload},
	}, Spec: resourceapi.ResourceClaimTemplateSpec{Spec: claimSpec}}
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace, UID: types.UID("pod-uid-" + suffix)},
		Spec: corev1.PodSpec{NodeName: nodeName, ResourceClaims: []corev1.PodResourceClaim{{
			Name: "channel", ResourceClaimTemplateName: &templateName,
		}}},
		Status: corev1.PodStatus{
			Phase:                 corev1.PodPending,
			Conditions:            []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
			ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: "channel", ResourceClaimName: &claimName}},
		},
	}
	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: namespace, UID: types.UID("claim-uid-" + suffix), OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID, Controller: &controller,
		}}},
		Spec: claimSpec,
		Status: resourceapi.ResourceClaimStatus{
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{{Resource: "pods", Name: pod.Name, UID: pod.UID}},
			Allocation: &resourceapi.AllocationResult{
				NodeSelector: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{
					Key: metav1.ObjectNameField, Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName},
				}}}}},
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{{Request: "channel", Driver: DriverName, Pool: nodeName, Device: "channel-0"}},
					Config: []resourceapi.DeviceAllocationConfiguration{{Source: resourceapi.AllocationConfigSourceClaim, Requests: []string{"channel"}, DeviceConfiguration: resourceapi.DeviceConfiguration{
						Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: DriverName, Parameters: runtime.RawExtension{Raw: claimConfigBytes}},
					}}},
				},
			},
		},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: nodeName, UID: types.UID("node-uid-" + suffix),
		Labels: map[string]string{
			gpuCliqueNodeLabelKey:                  "physical-clique",
			controllerOwnedCliqueIsolationLabelKey: string(cd.UID),
		},
		Annotations: map[string]string{
			computeDomainCliqueStartupAnnotationKey:    "physical-clique",
			computeDomainCliqueCapabilityAnnotationKey: string(nvapi.ComputeDomainCliqueProtocolControllerV1),
		},
	}}
	coreClient := &attestationCoreClient{nodes: map[string]*corev1.Node{node.Name: node.DeepCopy()}}
	nvidia := nvfake.NewSimpleClientset(cd.DeepCopy())
	manager := NewControllerOwnedCliqueManager(&ManagerConfig{
		driverNamespace: "driver", maxNodesPerIMEXDomain: 18,
		clientsets: flags.ClientSets{Core: coreClient, Resource: draclient.New(coreClient), Nvidia: nvidia},
	})
	require.NoError(t, manager.addInformerIndexes())
	require.NoError(t, manager.computeDomainInformer.GetStore().Add(cd.DeepCopy()))
	require.NoError(t, manager.workloadPodInformer.GetStore().Add(pod.DeepCopy()))
	require.NoError(t, manager.resourceClaimInformer.GetStore().Add(claim.DeepCopy()))
	require.NoError(t, manager.claimTemplateInformer.GetStore().Add(rct.DeepCopy()))
	require.NoError(t, manager.attestationNodeInformer.GetStore().Add(node.DeepCopy()))
	t.Cleanup(func() { manager.queue.ShutDown(); manager.attestationQueue.ShutDown() })
	return &nodeAttestationFixture{manager: manager, core: coreClient, nvidia: nvidia, cd: cd, node: node, pod: pod, claim: claim, rct: rct}
}

func TestNodeAttestationAcquiresReservationBeforePublishingControllerV1(t *testing.T) {
	f := newNodeAttestationFixture(t, "controller", nvapi.ComputeDomainCliqueProtocolControllerV1)
	require.NoError(t, f.manager.reconcileNodeAttestation(context.Background(), f.node.Name))
	node, err := f.core.CoreV1().Nodes().Get(context.Background(), f.node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(f.cd.UID), node.Labels[computeDomainLabelKey])
	require.True(t, validNodeAttestation(node, nvapi.ComputeDomainCliqueProtocolControllerV1))
	reservation, err := f.nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(context.Background(), physicalCliqueReservationName("physical-clique"), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, f.cd.UID, reservation.Spec.ComputeDomainUID)
	require.Equal(t, nvapi.ComputeDomainCliqueProtocolControllerV1, reservation.Spec.Protocol)
}

func TestLiveNodeAttestationRejectsStaleClaimAfterRestart(t *testing.T) {
	f := newNodeAttestationFixture(t, "stale-claim", nvapi.ComputeDomainCliqueProtocolControllerV1)
	require.NoError(t, f.manager.reconcileNodeAttestation(context.Background(), f.node.Name))
	node, err := f.core.CoreV1().Nodes().Get(context.Background(), f.node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, f.manager.attestationNodeInformer.GetStore().Update(node.DeepCopy()))
	require.True(t, f.manager.liveNodeAttestationAuthorized(node))

	f.claim.Status.ReservedFor[0].UID = "replaced-pod"
	require.NoError(t, f.manager.resourceClaimInformer.GetStore().Update(f.claim.DeepCopy()))
	require.False(t, f.manager.liveNodeAttestationAuthorized(node), "generation zero must not trust a stale routing projection")
}

func TestPublishedSnapshotKeepsDaemonRouteAfterWorkloadDeletion(t *testing.T) {
	f := newNodeAttestationFixture(t, "retirement-route", nvapi.ComputeDomainCliqueProtocolControllerV1)
	require.NoError(t, f.manager.reconcileNodeAttestation(context.Background(), f.node.Name))
	node, err := f.core.CoreV1().Nodes().Get(context.Background(), f.node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, f.manager.attestationNodeInformer.GetStore().Update(node.DeepCopy()))
	require.NoError(t, f.manager.workloadPodInformer.GetStore().Delete(f.pod))
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshot", Namespace: "driver"},
		Spec:       nvapi.ComputeDomainCliqueSnapshotSpec{ComputeDomainUID: f.cd.UID},
		Status: nvapi.ComputeDomainCliqueSnapshotStatus{
			Phase: nvapi.ComputeDomainCliqueSnapshotPhaseActive, Generation: 1,
			Members: []nvapi.ComputeDomainCliqueMember{{NodeUID: node.UID}},
		},
	}
	require.NoError(t, f.manager.snapshotInformer.GetStore().Add(snapshot))

	require.NoError(t, f.manager.reconcileNodeAttestation(context.Background(), node.Name))
	after, err := f.core.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(f.cd.UID), after.Labels[computeDomainLabelKey])
	require.True(t, validNodeAttestation(after, nvapi.ComputeDomainCliqueProtocolControllerV1))
}

func TestReservationCreateIsSerializedPerPhysicalClique(t *testing.T) {
	f := newNodeAttestationFixture(t, "singleflight", nvapi.ComputeDomainCliqueProtocolControllerV1)
	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			errs <- f.manager.reservePhysicalClique(context.Background(), f.cd, "physical-clique", nvapi.ComputeDomainCliqueProtocolControllerV1)
		}()
	}
	close(start)
	for range 2 {
		require.NoError(t, <-errs)
	}
	var creates int
	for _, action := range f.nvidia.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "computedomaincliquereservations" {
			creates++
		}
	}
	require.Equal(t, 1, creates)
}

func TestIsolationBoundaryChangeRequeuesWholePhysicalClique(t *testing.T) {
	f := newNodeAttestationFixture(t, "fanout", nvapi.ComputeDomainCliqueProtocolControllerV1)
	second := f.node.DeepCopy()
	second.Name = "node-fanout-second"
	second.UID = "node-uid-fanout-second"
	require.NoError(t, f.manager.attestationNodeInformer.GetStore().Add(second))

	previous := f.node.DeepCopy()
	delete(previous.Labels, controllerOwnedCliqueIsolationLabelKey)
	current := f.node.DeepCopy()
	f.manager.enqueueAttestationNodeChange(previous, current)

	keys := map[string]bool{}
	for f.manager.attestationQueue.Len() > 0 {
		key, shutdown := f.manager.attestationQueue.Get()
		require.False(t, shutdown)
		keys[key] = true
		f.manager.attestationQueue.Done(key)
		f.manager.attestationQueue.Forget(key)
	}
	require.True(t, keys[f.node.Name])
	require.True(t, keys[second.Name], "the final isolation event must wake candidates that previously failed closed")
}

func TestStartupCliqueIdentityChangeRequeuesWholePhysicalClique(t *testing.T) {
	f := newNodeAttestationFixture(t, "startup-fanout", nvapi.ComputeDomainCliqueProtocolControllerV1)
	peer := f.node.DeepCopy()
	peer.Name = "node-startup-fanout-peer"
	peer.UID = "node-uid-startup-fanout-peer"
	delete(peer.Annotations, computeDomainCliqueStartupAnnotationKey)
	require.NoError(t, f.manager.attestationNodeInformer.GetStore().Add(peer))

	previous := peer.DeepCopy()
	current := peer.DeepCopy()
	current.Annotations[computeDomainCliqueStartupAnnotationKey] = "physical-clique"
	f.manager.enqueueAttestationNodeChange(previous, current)

	keys := map[string]bool{}
	for f.manager.attestationQueue.Len() > 0 {
		key, shutdown := f.manager.attestationQueue.Get()
		require.False(t, shutdown)
		keys[key] = true
		f.manager.attestationQueue.Done(key)
		f.manager.attestationQueue.Forget(key)
	}
	require.True(t, keys[peer.Name])
	require.True(t, keys[f.node.Name], "the final startup identity event must wake candidates that previously failed closed")
}

func TestNodeAttestationDoesNotTakeOverLegacyRouting(t *testing.T) {
	f := newNodeAttestationFixture(t, "legacy", nvapi.ComputeDomainCliqueProtocolLegacyV1)
	f.node.Labels[computeDomainLabelKey] = string(f.cd.UID)
	delete(f.node.Labels, controllerOwnedCliqueIsolationLabelKey)
	require.NoError(t, f.manager.attestationNodeInformer.GetStore().Update(f.node.DeepCopy()))
	f.core.putNode(f.node.DeepCopy())
	require.NoError(t, f.manager.reconcileNodeAttestation(context.Background(), f.node.Name))
	node, err := f.core.CoreV1().Nodes().Get(context.Background(), f.node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, string(f.cd.UID), node.Labels[computeDomainLabelKey])
	require.Empty(t, node.Annotations[computeDomainAttestationAnnotationKey])
	reservations, err := f.nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, reservations.Items)
}

func TestNodeAttestationFailsClosedWithoutPhysicalClique(t *testing.T) {
	f := newNodeAttestationFixture(t, "missing-clique", nvapi.ComputeDomainCliqueProtocolControllerV1)
	delete(f.node.Labels, gpuCliqueNodeLabelKey)
	require.NoError(t, f.manager.attestationNodeInformer.GetStore().Update(f.node.DeepCopy()))
	require.NoError(t, f.manager.reconcileNodeAttestation(context.Background(), f.node.Name))
	node, err := f.core.CoreV1().Nodes().Get(context.Background(), f.node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, node.Labels[computeDomainLabelKey])
	reservations, err := f.nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, reservations.Items)
}

func TestNodeAttestationRequiresWholeCliqueIsolation(t *testing.T) {
	f := newNodeAttestationFixture(t, "not-isolated", nvapi.ComputeDomainCliqueProtocolControllerV1)
	delete(f.node.Labels, controllerOwnedCliqueIsolationLabelKey)
	require.NoError(t, f.manager.attestationNodeInformer.GetStore().Update(f.node.DeepCopy()))
	require.NoError(t, f.manager.reconcileNodeAttestation(context.Background(), f.node.Name))
	node, err := f.core.CoreV1().Nodes().Get(context.Background(), f.node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, node.Labels[computeDomainLabelKey])
	reservations, err := f.nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, reservations.Items)
}

func TestNodeAttestationRejectsIsolatedCliqueWithExistingLegacyRoute(t *testing.T) {
	f := newNodeAttestationFixture(t, "legacy-route", nvapi.ComputeDomainCliqueProtocolControllerV1)
	f.node.Labels[computeDomainLabelKey] = "legacy-domain"
	require.NoError(t, f.manager.attestationNodeInformer.GetStore().Update(f.node.DeepCopy()))
	f.core.putNode(f.node.DeepCopy())
	require.NoError(t, f.manager.reconcileNodeAttestation(context.Background(), f.node.Name))

	node, err := f.core.CoreV1().Nodes().Get(context.Background(), f.node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "legacy-domain", node.Labels[computeDomainLabelKey])
	require.Empty(t, node.Annotations[computeDomainAttestationAnnotationKey])
	reservations, err := f.nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, reservations.Items)
}

func TestWholeCliqueIsolationIncludesStartupIdentityAfterRouteLoss(t *testing.T) {
	f := newNodeAttestationFixture(t, "lost-topology", nvapi.ComputeDomainCliqueProtocolControllerV1)
	lost := f.node.DeepCopy()
	lost.Name = "node-lost-topology-peer"
	lost.UID = "node-uid-lost-topology-peer"
	delete(lost.Labels, gpuCliqueNodeLabelKey)
	delete(lost.Labels, controllerOwnedCliqueIsolationLabelKey)
	require.NoError(t, f.manager.attestationNodeInformer.GetStore().Add(lost))

	isolated, err := f.manager.wholeCliqueIsolated("physical-clique", f.cd.UID)
	require.NoError(t, err)
	require.False(t, isolated, "removing a routable topology label is not fence evidence for the startup clique")
}

func TestSplitWholeCliqueIsolationPublishesNeitherContender(t *testing.T) {
	legacy := newNodeAttestationFixture(t, "controller-a", nvapi.ComputeDomainCliqueProtocolControllerV1)
	controller := newNodeAttestationFixture(t, "controller-b", nvapi.ComputeDomainCliqueProtocolControllerV1)
	// Use one API server and one manager cache to exercise exact-name Create.
	controller.manager.config.clientsets.Nvidia = legacy.nvidia
	require.NoError(t, legacy.manager.computeDomainInformer.GetStore().Add(controller.cd.DeepCopy()))
	require.NoError(t, legacy.manager.workloadPodInformer.GetStore().Add(controller.pod.DeepCopy()))
	require.NoError(t, legacy.manager.resourceClaimInformer.GetStore().Add(controller.claim.DeepCopy()))
	require.NoError(t, legacy.manager.claimTemplateInformer.GetStore().Add(controller.rct.DeepCopy()))
	require.NoError(t, legacy.manager.attestationNodeInformer.GetStore().Add(controller.node.DeepCopy()))
	controller.manager = legacy.manager
	controller.core = legacy.core
	legacy.core.putNode(controller.node.DeepCopy())

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, nodeName := range []string{legacy.node.Name, controller.node.Name} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- legacy.manager.reconcileNodeAttestation(context.Background(), nodeName)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var labeled int
	for _, nodeName := range []string{legacy.node.Name, controller.node.Name} {
		node, err := legacy.core.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
		require.NoError(t, err)
		if node.Labels[computeDomainLabelKey] != "" {
			labeled++
		}
	}
	require.Zero(t, labeled)
	reservations, err := legacy.nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, reservations.Items)
}

type attestationCoreClient struct {
	kubernetes.Interface
	mu         sync.Mutex
	nodes      map[string]*corev1.Node
	pods       map[string]*corev1.Pod
	podListErr error
}

func (c *attestationCoreClient) CoreV1() coretyped.CoreV1Interface {
	return &attestationCoreV1{client: c}
}

func (c *attestationCoreClient) putNode(node *corev1.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[node.Name] = node.DeepCopy()
}

func (c *attestationCoreClient) putPod(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pods[podMapKey(pod.Namespace, pod.Name)] = pod.DeepCopy()
}

type attestationCoreV1 struct {
	coretyped.CoreV1Interface
	client *attestationCoreClient
}

func (c *attestationCoreV1) Nodes() coretyped.NodeInterface {
	return &attestationNodes{client: c.client}
}

func (c *attestationCoreV1) Pods(namespace string) coretyped.PodInterface {
	return &attestationPods{client: c.client, namespace: namespace}
}

type attestationNodes struct {
	coretyped.NodeInterface
	client *attestationCoreClient
}

type attestationPods struct {
	coretyped.PodInterface
	client    *attestationCoreClient
	namespace string
}

func podMapKey(namespace, name string) string { return namespace + "/" + name }

func (p *attestationPods) Get(_ context.Context, name string, _ metav1.GetOptions) (*corev1.Pod, error) {
	p.client.mu.Lock()
	defer p.client.mu.Unlock()
	pod := p.client.pods[podMapKey(p.namespace, name)]
	if pod == nil {
		return nil, apierrors.NewNotFound(corev1.Resource("pods"), name)
	}
	return pod.DeepCopy(), nil
}

func (p *attestationPods) List(_ context.Context, _ metav1.ListOptions) (*corev1.PodList, error) {
	p.client.mu.Lock()
	defer p.client.mu.Unlock()
	if p.client.podListErr != nil {
		return nil, p.client.podListErr
	}
	list := &corev1.PodList{}
	for _, pod := range p.client.pods {
		if pod.Namespace == p.namespace {
			list.Items = append(list.Items, *pod.DeepCopy())
		}
	}
	return list, nil
}

func (p *attestationPods) Update(_ context.Context, pod *corev1.Pod, _ metav1.UpdateOptions) (*corev1.Pod, error) {
	p.client.mu.Lock()
	defer p.client.mu.Unlock()
	key := podMapKey(p.namespace, pod.Name)
	if p.client.pods[key] == nil {
		return nil, apierrors.NewNotFound(corev1.Resource("pods"), pod.Name)
	}
	p.client.pods[key] = pod.DeepCopy()
	return pod.DeepCopy(), nil
}

func (n *attestationNodes) Get(_ context.Context, name string, _ metav1.GetOptions) (*corev1.Node, error) {
	n.client.mu.Lock()
	defer n.client.mu.Unlock()
	node := n.client.nodes[name]
	if node == nil {
		return nil, apierrors.NewNotFound(corev1.Resource("nodes"), name)
	}
	return node.DeepCopy(), nil
}

func (n *attestationNodes) List(_ context.Context, options metav1.ListOptions) (*corev1.NodeList, error) {
	n.client.mu.Lock()
	defer n.client.mu.Unlock()
	selector, err := labels.Parse(options.LabelSelector)
	if err != nil {
		return nil, err
	}
	list := &corev1.NodeList{}
	for _, node := range n.client.nodes {
		if selector.Matches(labels.Set(node.Labels)) {
			list.Items = append(list.Items, *node.DeepCopy())
		}
	}
	return list, nil
}

func (n *attestationNodes) Update(_ context.Context, node *corev1.Node, _ metav1.UpdateOptions) (*corev1.Node, error) {
	n.client.mu.Lock()
	defer n.client.mu.Unlock()
	if n.client.nodes[node.Name] == nil {
		return nil, apierrors.NewNotFound(corev1.Resource("nodes"), node.Name)
	}
	n.client.nodes[node.Name] = node.DeepCopy()
	return node.DeepCopy(), nil
}

func TestRetirementFencesControllerIsolatedNodeAfterRouteLoss(t *testing.T) {
	const cdUID = "cd-uid"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-a",
		Labels: map[string]string{
			controllerOwnedCliqueIsolationLabelKey: cdUID,
		},
		Annotations: map[string]string{
			computeDomainAttestationAnnotationKey:   `{"computeDomainUID":"cd-uid"}`,
			computeDomainCliqueStartupAnnotationKey: "clique-a",
		},
	}}
	coreClient := &attestationCoreClient{nodes: map[string]*corev1.Node{node.Name: node.DeepCopy()}}
	manager := &NodeManager{config: &ManagerConfig{clientsets: flags.ClientSets{Core: coreClient}}}

	require.NoError(t, manager.RemoveComputeDomainLabelsAndAttestations(context.Background(), cdUID, true))
	updated, err := coreClient.CoreV1().Nodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, updated.Labels[computeDomainLabelKey])
	require.Empty(t, updated.Labels[controllerOwnedCliqueIsolationLabelKey])
	require.Empty(t, updated.Annotations[computeDomainAttestationAnnotationKey])
	require.Equal(t, cdUID, updated.Annotations[nvapi.ComputeDomainCliqueRetirementFencedAnnotation])
	require.Equal(t, "clique-a", updated.Annotations[computeDomainCliqueStartupAnnotationKey], "the kubelet consumes the fence before clearing startup topology")
}

func TestNodeAttestationRejectsInexactAuthorization(t *testing.T) {
	tests := map[string]func(*nodeAttestationFixture){
		"reservedFor Pod UID": func(f *nodeAttestationFixture) { f.claim.Status.ReservedFor[0].UID = "other" },
		"selected Node": func(f *nodeAttestationFixture) {
			f.claim.Status.Allocation.NodeSelector.NodeSelectorTerms[0].MatchFields[0].Values[0] = "other"
		},
		"config DomainID": func(f *nodeAttestationFixture) {
			config := nvapi.DefaultComputeDomainChannelConfig()
			config.DomainID = "other"
			config.AllocationMode = nvapi.ComputeDomainChannelAllocationModeSingle
			config.Protocol = nvapi.ComputeDomainCliqueProtocolControllerV1
			f.claim.Status.Allocation.Devices.Config[0].Opaque.Parameters.Raw, _ = json.Marshal(config)
		},
		"claim owner":        func(f *nodeAttestationFixture) { f.claim.OwnerReferences[0].UID = "other" },
		"live scheduled Pod": func(f *nodeAttestationFixture) { f.pod.Status.Conditions[0].Status = corev1.ConditionFalse },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			f := newNodeAttestationFixture(t, "reject", nvapi.ComputeDomainCliqueProtocolControllerV1)
			mutate(f)
			require.NoError(t, f.manager.workloadPodInformer.GetStore().Update(f.pod.DeepCopy()))
			require.NoError(t, f.manager.resourceClaimInformer.GetStore().Update(f.claim.DeepCopy()))
			require.NoError(t, f.manager.reconcileNodeAttestation(context.Background(), f.node.Name))
			node, err := f.core.CoreV1().Nodes().Get(context.Background(), f.node.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Empty(t, node.Labels[computeDomainLabelKey])
			require.Empty(t, node.Annotations[computeDomainAttestationAnnotationKey])
		})
	}
}
