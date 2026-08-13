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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	coretyped "k8s.io/client-go/kubernetes/typed/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	pkgflags "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

func TestComputeDomainManagerStartsWithoutSnapshotAPI(t *testing.T) {
	nvidiaClient := nvfake.NewSimpleClientset()
	nvidiaClient.PrependReactor("list", "computedomaincliquesnapshots", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(nvapi.Resource("computedomaincliquesnapshots"), "")
	})
	coreClient := &recordingCoreClient{}
	manager := newTestComputeDomainManager(t, nvidiaClient, coreClient, "")

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, manager.Start(ctx))
	assert.False(t, manager.snapshotAPIAvailable)
	assert.False(t, manager.snapshotInformer.HasSynced(), "the absent snapshot API must not be started or awaited")
	cancel()
	require.NoError(t, manager.Stop())

	assertWatchScope(t, nvidiaClient.Actions(), "computedomains", "", "", "")
	assertListScope(t, nvidiaClient.Actions(), "computedomaincliquesnapshots", testDriverNamespace, "", "")
	coreClient.assertInformerScope(t, "pods", testDriverNamespace, computeDomainLabelKey, "spec.nodeName="+testNodeName)
	coreClient.assertInformerScope(t, "nodes", "", "", "metadata.name="+testNodeName)
}

func TestComputeDomainManagerStartsSnapshotInformerWhenAPIExists(t *testing.T) {
	nvidiaClient := nvfake.NewSimpleClientset()
	manager := newTestComputeDomainManager(t, nvidiaClient, &recordingCoreClient{}, "clique-a")

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, manager.Start(ctx))
	assert.True(t, manager.snapshotAPIAvailable)
	assert.True(t, manager.snapshotInformer.HasSynced())
	cancel()
	require.NoError(t, manager.Stop())

	assertWatchScope(t, nvidiaClient.Actions(), "computedomaincliquesnapshots", testDriverNamespace, computeDomainCliqueLabelKey+"=clique-a", "")
	for _, action := range nvidiaClient.Actions() {
		if action.GetVerb() == "list" && action.GetResource().Resource == "computedomaincliquesnapshots" {
			assert.Equal(t, testDriverNamespace, action.GetNamespace(), "snapshot discovery and watches must stay in the driver namespace")
		}
	}
}

func TestSetGPUCliqueLabelRejectsRestartTopologyChange(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: testNodeName,
		Annotations: map[string]string{
			computeDomainCliqueStartupAnnotationKey: "old-clique",
		},
	}}
	coreClient := &recordingCoreClient{nodes: map[string]*corev1.Node{testNodeName: node}}
	manager := newTestComputeDomainManager(t, nvfake.NewSimpleClientset(), coreClient, "new-clique")
	err := manager.SetGPUCliqueLabel(context.Background())
	require.ErrorContains(t, err, "does not match immutable Node startup clique")

	require.Equal(t, "old-clique", node.Annotations[computeDomainCliqueStartupAnnotationKey])
	require.Empty(t, node.Labels[gpuCliqueLabelKey])
}

func TestSetGPUCliqueLabelRejectsMissingRestartTopology(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: testNodeName,
		Annotations: map[string]string{
			computeDomainCliqueStartupAnnotationKey: "old-clique",
		},
	}}
	coreClient := &recordingCoreClient{nodes: map[string]*corev1.Node{testNodeName: node}}
	manager := newTestComputeDomainManager(t, nvfake.NewSimpleClientset(), coreClient, "")
	err := manager.SetGPUCliqueLabel(context.Background())
	require.ErrorContains(t, err, "startup GPU clique is absent")
	require.Error(t, manager.assertTopologyValid())
}

func TestRefreshGPUCliqueIDInvalidatesNonemptyToEmpty(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: testNodeName,
		Annotations: map[string]string{
			computeDomainCliqueStartupAnnotationKey: "old-clique",
		},
	}}
	coreClient := &recordingCoreClient{nodes: map[string]*corev1.Node{testNodeName: node}}
	manager := newTestComputeDomainManager(t, nvfake.NewSimpleClientset(), coreClient, "old-clique")
	manager.getCliqueIDFunc = func() (string, error) { return "", nil }
	err := manager.refreshGPUCliqueID(context.Background())
	require.ErrorContains(t, err, `changed from "old-clique" to ""`)
	require.Error(t, manager.assertTopologyValid())
}

func TestRefreshGPUCliqueIDInvalidatesOnDiscoveryError(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: testNodeName,
		Labels: map[string]string{
			gpuCliqueLabelKey: "old-clique",
		},
		Annotations: map[string]string{
			computeDomainCliqueStartupAnnotationKey: "old-clique",
		},
	}}
	coreClient := &recordingCoreClient{nodes: map[string]*corev1.Node{testNodeName: node}}
	manager := newTestComputeDomainManager(t, nvfake.NewSimpleClientset(), coreClient, "old-clique")
	manager.config.flags.gpuCliqueLabelEnabled = true
	manager.getCliqueIDFunc = func() (string, error) { return "", fmt.Errorf("fabric registration failed") }
	err := manager.refreshGPUCliqueID(context.Background())
	require.ErrorContains(t, err, `verifying GPU clique ID "old-clique" failed`)
	require.Error(t, manager.assertTopologyValid())
	require.Empty(t, node.Labels[gpuCliqueLabelKey])
	require.Equal(t, "old-clique", node.Annotations[computeDomainCliqueStartupAnnotationKey])
}

func TestPhysicalCliqueReservationBlocksDifferentComputeDomain(t *testing.T) {
	cliqueID := "clique-a"
	digest := sha256.Sum256([]byte(cliqueID))
	reservation := &nvapi.ComputeDomainCliqueReservation{
		ObjectMeta: metav1.ObjectMeta{Name: "clique-" + hex.EncodeToString(digest[:])},
		Spec: nvapi.ComputeDomainCliqueReservationSpec{
			CliqueID: cliqueID, ComputeDomainUID: types.UID("old-cd"),
			ComputeDomainName: "old", ComputeDomainNamespace: "tenant-a",
			Protocol: nvapi.ComputeDomainCliqueProtocolControllerV1,
		},
	}
	manager := newTestComputeDomainManager(t, nvfake.NewSimpleClientset(reservation), &recordingCoreClient{}, cliqueID)
	require.ErrorContains(t, manager.AssertPhysicalCliqueAvailable(context.Background(), "new-cd"), "remains reserved")
	require.NoError(t, manager.AssertPhysicalCliqueAvailable(context.Background(), "old-cd"))
}

func TestControllerOwnedReadinessUsesExactCachedIdentityAndReceipt(t *testing.T) {
	manager, cd, _, pod := readyControllerOwnedManager(t)

	// No installed-snapshot Pod annotation is needed. The exact node-local
	// receipt is the authoritative proof that this daemon installed the
	// active generation.
	assert.Empty(t, pod.Annotations)
	require.NoError(t, manager.AssertComputeDomainReady(context.Background(), string(cd.UID), nvapi.ComputeDomainCliqueProtocolControllerV1))

	// The Core fake has no Node object. Success therefore also proves the
	// readiness retry used the local-node cache instead of a live GET.
	coreClient, ok := manager.config.clientsets.Core.(*recordingCoreClient)
	require.True(t, ok)
	assert.Zero(t, coreClient.getNodeCalls())

	deleting := pod.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	require.NoError(t, manager.podInformer.GetIndexer().Update(deleting))
	err := manager.AssertComputeDomainReady(context.Background(), string(cd.UID), nvapi.ComputeDomainCliqueProtocolControllerV1)
	require.ErrorContains(t, err, "deleting or terminal")

	terminal := pod.DeepCopy()
	terminal.Status.Phase = corev1.PodFailed
	require.NoError(t, manager.podInformer.GetIndexer().Update(terminal))
	err = manager.AssertComputeDomainReady(context.Background(), string(cd.UID), nvapi.ComputeDomainCliqueProtocolControllerV1)
	require.ErrorContains(t, err, "deleting or terminal")

}

func TestComputeDomainReadyRejectsProtocolMismatchBeforeSelectingPath(t *testing.T) {
	manager, cd, _, _ := readyControllerOwnedManager(t)

	err := manager.AssertComputeDomainReady(context.Background(), string(cd.UID), nvapi.ComputeDomainCliqueProtocolLegacyV1)
	require.ErrorContains(t, err, "does not match ComputeDomain protocol")
}

func TestControllerOwnedReadinessFailsClosedWithoutCliqueOrSnapshotAPI(t *testing.T) {
	manager, cd, _, _ := readyControllerOwnedManager(t)

	manager.setCliqueID("")
	err := manager.AssertComputeDomainReady(context.Background(), string(cd.UID), nvapi.ComputeDomainCliqueProtocolControllerV1)
	require.ErrorContains(t, err, "non-empty GPU clique ID")

	manager.setCliqueID("clique-a")
	manager.snapshotAPIAvailable = false
	err = manager.AssertComputeDomainReady(context.Background(), string(cd.UID), nvapi.ComputeDomainCliqueProtocolControllerV1)
	require.ErrorContains(t, err, "API is unavailable")
}

const (
	testDriverNamespace = "driver-namespace"
	testNodeName        = "node-a"
)

func newTestComputeDomainManager(t *testing.T, nvidiaClient *nvfake.Clientset, coreClient *recordingCoreClient, cliqueID string) *ComputeDomainManager {
	t.Helper()
	nvidiaClient.PrependWatchReactor("*", func(action k8stesting.Action) (bool, watch.Interface, error) {
		var bookmark runtime.Object
		switch action.GetResource().Resource {
		case "computedomains":
			bookmark = &nvapi.ComputeDomain{}
		case "computedomaincliquesnapshots":
			bookmark = &nvapi.ComputeDomainCliqueSnapshot{}
		default:
			return false, nil, nil
		}
		return true, initialEventsEndWatch(bookmark), nil
	})
	config := &Config{
		flags: &Flags{
			nodeName:                    testNodeName,
			namespace:                   testDriverNamespace,
			kubeletPluginsDirectoryPath: t.TempDir(),
		},
		clientsets: pkgflags.ClientSets{Nvidia: nvidiaClient, Core: coreClient},
	}
	manager, err := NewComputeDomainManager(config, func() (string, error) { return cliqueID, nil })
	require.NoError(t, err)
	return manager
}

func readyControllerOwnedManager(t *testing.T) (*ComputeDomainManager, *nvapi.ComputeDomain, *nvapi.ComputeDomainCliqueSnapshot, *corev1.Pod) {
	t.Helper()
	manager := newTestComputeDomainManager(t, nvfake.NewSimpleClientset(), &recordingCoreClient{}, "clique-a")
	manager.snapshotAPIAvailable = true
	require.NoError(t, manager.informer.AddIndexers(cacheIndexersForComputeDomainUID()))

	cd := &nvapi.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "domain",
			Namespace: "workload-namespace",
			UID:       types.UID("domain-uid"),
			Annotations: map[string]string{
				nvapi.ComputeDomainCliqueProtocolAnnotation: string(nvapi.ComputeDomainCliqueProtocolControllerV1),
			},
		},
	}
	require.NoError(t, manager.informer.GetIndexer().Add(cd))

	digest := sha256.Sum256([]byte(manager.CliqueID()))
	snapshot := &nvapi.ComputeDomainCliqueSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s.%s", cd.UID, hex.EncodeToString(digest[:8])),
			Namespace: testDriverNamespace,
			UID:       types.UID("snapshot-uid"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "DaemonSet", Name: "daemonset", UID: types.UID("daemonset-uid"), Controller: ptr.To(true),
			}},
		},
		Spec: nvapi.ComputeDomainCliqueSnapshotSpec{
			ComputeDomainUID:       cd.UID,
			ComputeDomainName:      cd.Name,
			ComputeDomainNamespace: cd.Namespace,
			CliqueID:               manager.CliqueID(),
			Capacity:               18,
			DaemonSetName:          "daemonset",
			DaemonSetUID:           types.UID("daemonset-uid"),
			Protocol:               nvapi.ComputeDomainCliqueProtocolControllerV1,
		},
		Status: nvapi.ComputeDomainCliqueSnapshotStatus{
			Phase:      nvapi.ComputeDomainCliqueSnapshotPhaseActive,
			Generation: 3,
			Assignments: []nvapi.ComputeDomainCliqueAssignment{{
				Index: 0, NodeName: testNodeName, NodeUID: types.UID("node-uid"), CurrentPodUID: types.UID("pod-uid"), State: nvapi.ComputeDomainCliqueAssignmentStateBound, EverPublished: true,
			}},
			Members: []nvapi.ComputeDomainCliqueMember{{
				Index:        0,
				NodeName:     testNodeName,
				NodeUID:      types.UID("node-uid"),
				PodName:      "daemon-pod",
				PodUID:       types.UID("pod-uid"),
				PodIP:        "10.0.0.1",
				DaemonSetUID: types.UID("daemonset-uid"),
			}},
			MemberCount: 1,
		},
	}
	canonical, err := json.Marshal(snapshot.Status.Members)
	require.NoError(t, err)
	hash := sha256.Sum256(canonical)
	snapshot.Status.Hash = hex.EncodeToString(hash[:])
	require.NoError(t, manager.snapshotInformer.GetIndexer().Add(snapshot))

	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "daemon-pod",
			Namespace: testDriverNamespace,
			UID:       types.UID("pod-uid"),
			OwnerReferences: []metav1.OwnerReference{{
				UID:        types.UID("daemonset-uid"),
				Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{NodeName: testNodeName},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.1",
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodReady, Status: corev1.ConditionTrue,
			}},
		},
	}
	require.NoError(t, manager.podInformer.GetIndexer().Add(pod))
	require.NoError(t, manager.nodeInformer.GetIndexer().Add(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNodeName, UID: types.UID("node-uid")}}))

	receipt := nvapi.ComputeDomainCliqueSnapshotReceipt{
		ComputeDomainUID:   cd.UID,
		SnapshotUID:        snapshot.UID,
		SnapshotGeneration: snapshot.Status.Generation,
		SnapshotHash:       snapshot.Status.Hash,
		NodeUID:            types.UID("node-uid"),
		PodUID:             types.UID("pod-uid"),
		Index:              0,
	}
	receiptBytes, err := json.Marshal(receipt)
	require.NoError(t, err)
	receiptDir := filepath.Join(manager.configFilesRoot, string(cd.UID))
	require.NoError(t, os.MkdirAll(receiptDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(receiptDir, "snapshot-receipt.json"), receiptBytes, 0o600))

	return manager, cd, snapshot, pod
}

func cacheIndexersForComputeDomainUID() cache.Indexers {
	return cache.Indexers{
		"computeDomainUID": uidIndexer[*nvapi.ComputeDomain],
	}
}

func assertListScope(t *testing.T, actions []k8stesting.Action, resource, namespace, labelSelector, fieldSelector string) {
	t.Helper()
	for _, action := range actions {
		listAction, ok := action.(k8stesting.ListAction)
		if !ok || action.GetResource().Resource != resource {
			continue
		}
		assert.Equal(t, namespace, action.GetNamespace())
		assert.Equal(t, labelSelector, listAction.GetListRestrictions().Labels.String())
		assert.Equal(t, fieldSelector, listAction.GetListRestrictions().Fields.String())
		return
	}
	t.Fatalf("no list action found for %s", resource)
}

func assertWatchScope(t *testing.T, actions []k8stesting.Action, resource, namespace, labelSelector, fieldSelector string) {
	t.Helper()
	for _, action := range actions {
		watchAction, ok := action.(k8stesting.WatchAction)
		if !ok || action.GetResource().Resource != resource {
			continue
		}
		assert.Equal(t, namespace, action.GetNamespace())
		assert.Equal(t, labelSelector, watchAction.GetWatchRestrictions().Labels.String())
		assert.Equal(t, fieldSelector, watchAction.GetWatchRestrictions().Fields.String())
		return
	}
	t.Fatalf("no watch action found for %s", resource)
}

type recordedCoreCall struct {
	verb, resource, namespace string
	options                   metav1.ListOptions
}

type recordingCoreClient struct {
	kubernetes.Interface
	mu    sync.Mutex
	calls []recordedCoreCall
	nodes map[string]*corev1.Node
}

func (c *recordingCoreClient) CoreV1() coretyped.CoreV1Interface {
	return &recordingCoreV1{client: c}
}

func (c *recordingCoreClient) record(call recordedCoreCall) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *recordingCoreClient) assertInformerScope(t *testing.T, resource, namespace, labelSelector, fieldSelector string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, call := range c.calls {
		if call.resource != resource || (call.verb != "list" && call.verb != "watch") {
			continue
		}
		assert.Equal(t, namespace, call.namespace)
		assert.Equal(t, labelSelector, call.options.LabelSelector)
		assert.Equal(t, fieldSelector, call.options.FieldSelector)
		return
	}
	t.Fatalf("no core informer call found for %s", resource)
}

func (c *recordingCoreClient) getNodeCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, call := range c.calls {
		if call.verb == "get" && call.resource == "nodes" {
			count++
		}
	}
	return count
}

type recordingCoreV1 struct {
	coretyped.CoreV1Interface
	client *recordingCoreClient
}

func (c *recordingCoreV1) Pods(namespace string) coretyped.PodInterface {
	return &recordingPods{client: c.client, namespace: namespace}
}

func (c *recordingCoreV1) Nodes() coretyped.NodeInterface {
	return &recordingNodes{client: c.client}
}

type recordingPods struct {
	coretyped.PodInterface
	client    *recordingCoreClient
	namespace string
}

func (p *recordingPods) List(_ context.Context, options metav1.ListOptions) (*corev1.PodList, error) {
	p.client.record(recordedCoreCall{verb: "list", resource: "pods", namespace: p.namespace, options: options})
	return &corev1.PodList{}, nil
}

func (p *recordingPods) Watch(_ context.Context, options metav1.ListOptions) (watch.Interface, error) {
	p.client.record(recordedCoreCall{verb: "watch", resource: "pods", namespace: p.namespace, options: options})
	return initialEventsEndWatch(&corev1.Pod{}), nil
}

type recordingNodes struct {
	coretyped.NodeInterface
	client *recordingCoreClient
}

func (n *recordingNodes) List(_ context.Context, options metav1.ListOptions) (*corev1.NodeList, error) {
	n.client.record(recordedCoreCall{verb: "list", resource: "nodes", options: options})
	return &corev1.NodeList{}, nil
}

func (n *recordingNodes) Watch(_ context.Context, options metav1.ListOptions) (watch.Interface, error) {
	n.client.record(recordedCoreCall{verb: "watch", resource: "nodes", options: options})
	return initialEventsEndWatch(&corev1.Node{}), nil
}

func (n *recordingNodes) Get(_ context.Context, name string, _ metav1.GetOptions) (*corev1.Node, error) {
	n.client.record(recordedCoreCall{verb: "get", resource: "nodes"})
	if node := n.client.nodes[name]; node != nil {
		return node.DeepCopy(), nil
	}
	return nil, apierrors.NewNotFound(corev1.Resource("nodes"), name)
}

func (n *recordingNodes) Patch(_ context.Context, name string, _ types.PatchType, data []byte, _ metav1.PatchOptions, _ ...string) (*corev1.Node, error) {
	n.client.record(recordedCoreCall{verb: "patch", resource: "nodes"})
	node := n.client.nodes[name]
	if node == nil {
		return nil, apierrors.NewNotFound(corev1.Resource("nodes"), name)
	}
	var patch struct {
		Metadata struct {
			Labels map[string]*string `json:"labels"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(data, &patch); err != nil {
		return nil, err
	}
	for key, value := range patch.Metadata.Labels {
		if value == nil {
			delete(node.Labels, key)
			continue
		}
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		node.Labels[key] = *value
	}
	return node.DeepCopy(), nil
}

func initialEventsEndWatch(bookmark runtime.Object) watch.Interface {
	accessor, err := apiMeta.Accessor(bookmark)
	if err != nil {
		panic(err)
	}
	accessor.SetResourceVersion("1")
	accessor.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
	watcher := watch.NewRaceFreeFake()
	watcher.Action(watch.Bookmark, bookmark)
	return watcher
}
