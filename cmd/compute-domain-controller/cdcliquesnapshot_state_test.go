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
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/cdclique"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

type observedFakeAPIAction struct {
	verb        string
	resource    string
	subresource string
	result      string
	mutated     bool
}

type persistentAgentHarness struct {
	manager    *PersistentAgentManager
	nvidia     *nvfake.Clientset
	observed   []observedFakeAPIAction
	observedMu sync.Mutex
	cd         *nvapi.ComputeDomain
	daemonSet  *appsv1.DaemonSet
	key        string
	cliqueID   string
	nextObject int
}

func setTestNodeAttestation(t testing.TB, node *corev1.Node, cdUID, podUID string) {
	t.Helper()
	encoded, err := json.Marshal(computeDomainNodeAttestation{
		ComputeDomainUID: types.UID(cdUID),
		NodeUID:          node.UID,
		PodUID:           types.UID(podUID),
		ResourceClaimUID: types.UID("claim-" + podUID),
	})
	require.NoError(t, err)
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	node.Annotations[computeDomainAttestationAnnotationKey] = string(encoded)
}

func newPersistentAgentHarness(t *testing.T, expectedNodes int) *persistentAgentHarness {
	t.Helper()
	cd := &nvapi.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "domain",
			Namespace: "workload",
			UID:       types.UID("cd-uid"),
			Annotations: map[string]string{
				nvapi.ComputeDomainCliqueProtocolAnnotation: string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1),
			},
		},
		Spec: nvapi.ComputeDomainSpec{
			NumNodes: expectedNodes,
			Channel: &nvapi.ComputeDomainChannelSpec{
				ResourceClaimTemplate: nvapi.ComputeDomainResourceClaimTemplate{Name: "workload-template"},
			},
		},
	}
	nvidia := nvfake.NewSimpleClientset(cd.DeepCopy())
	config := &ManagerConfig{
		driverNamespace:       "driver",
		imageName:             "example.invalid/daemon:test",
		maxNodesPerIMEXDomain: max(expectedNodes, 18),
		clientsets: flags.ClientSets{
			Nvidia: nvidia,
		},
	}
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      persistentAgentDaemonSetName,
			Namespace: "driver",
			UID:       types.UID("persistent-agent-ds-uid"),
			Labels:    map[string]string{persistentAgentLabelKey: "true"},
		},
		Spec: appsv1.DaemonSetSpec{
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{persistentAgentLabelKey: "true"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: persistentAgentServiceAccountName,
					Containers:         []corev1.Container{{Name: "compute-domain-daemon", Command: []string{"compute-domain-daemon", "run", "--persistent-agent"}}},
				},
			},
		},
	}

	h := &persistentAgentHarness{
		nvidia:     nvidia,
		cd:         cd,
		daemonSet:  ds,
		cliqueID:   "clique-a",
		nextObject: 1,
	}
	h.manager = NewPersistentAgentManager(config)
	// This harness isolates the snapshot state machine. The full live
	// Pod->claim->template attestation chain is exercised separately in
	// nodeattestation_test.go.
	h.manager.liveAttestationCheck = func(*corev1.Node) bool { return true }
	require.NoError(t, h.manager.addInformerIndexes())
	require.NoError(t, h.manager.computeDomainInformer.GetStore().Add(cd.DeepCopy()))
	require.NoError(t, h.manager.daemonSetInformer.GetStore().Add(ds.DeepCopy()))
	h.key = config.driverNamespace + "/" + cdclique.SnapshotName(string(cd.UID), h.cliqueID)
	h.manager.pendingScopes[h.key] = snapshotScope{computeDomainUID: string(cd.UID), cliqueID: h.cliqueID}
	t.Cleanup(func() {
		h.manager.queue.ShutDown()
		h.manager.attestationQueue.ShutDown()
	})

	objectReaction := clienttesting.ObjectReaction(nvidia.Tracker())
	nvidia.PrependReactor("*", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		h.observedMu.Lock()
		defer h.observedMu.Unlock()
		if objectAction, ok := action.(interface{ GetObject() runtime.Object }); ok && (action.GetVerb() == "create" || action.GetVerb() == "update") {
			accessor, accessorErr := apiMeta.Accessor(objectAction.GetObject())
			require.NoError(t, accessorErr)
			accessor.SetResourceVersion(strconv.Itoa(h.nextObject))
			if accessor.GetUID() == "" {
				accessor.SetUID(types.UID(fmt.Sprintf("fake-uid-%d", h.nextObject)))
			}
			h.nextObject++
		}
		_, result, reactionErr := objectReaction(action)
		outcome := "success"
		if apierrors.IsAlreadyExists(reactionErr) {
			outcome = "already_exists"
		} else if reactionErr != nil {
			outcome = "error"
		}
		mutated := reactionErr == nil && (action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "delete" || action.GetVerb() == "patch")
		h.observed = append(h.observed, observedFakeAPIAction{
			verb:        action.GetVerb(),
			resource:    action.GetResource().Resource,
			subresource: action.GetSubresource(),
			result:      outcome,
			mutated:     mutated,
		})
		return true, result, reactionErr
	})
	return h
}

func (h *persistentAgentHarness) addNodeAndPod(t *testing.T, ordinal int) {
	t.Helper()
	controller := true
	nodeName := fmt.Sprintf("node-%02d", ordinal)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: nodeName,
		UID:  types.UID(fmt.Sprintf("node-uid-%02d", ordinal)),
		Labels: map[string]string{
			computeDomainLabelKey:            string(h.cd.UID),
			gpuCliqueNodeLabelKey:            h.cliqueID,
			persistentAgentIsolationLabelKey: string(h.cd.UID),
		},
		Annotations: map[string]string{
			computeDomainCliqueStartupAnnotationKey:    h.cliqueID,
			computeDomainCliqueCapabilityAnnotationKey: string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1),
		},
	}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: fmt.Sprintf("boot-%02d", ordinal)}}}
	setTestNodeAttestation(t, node, string(h.cd.UID), fmt.Sprintf("pod-uid-%02d", ordinal))
	podLabels := map[string]string{computeDomainLabelKey: string(h.cd.UID)}
	if h.cd.Annotations[nvapi.ComputeDomainCliqueProtocolAnnotation] == string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1) {
		podLabels = map[string]string{persistentAgentLabelKey: "true"}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "daemon-" + nodeName,
			Namespace: "driver",
			UID:       types.UID(fmt.Sprintf("pod-uid-%02d", ordinal)),
			Labels:    podLabels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: appsv1.SchemeGroupVersion.String(),
				Kind:       "DaemonSet",
				Name:       h.daemonSet.Name,
				UID:        h.daemonSet.UID,
				Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: fmt.Sprintf("10.0.0.%d", ordinal+1),
		},
	}
	require.NoError(t, h.manager.nodeInformer.GetStore().Add(node))
	require.NoError(t, h.manager.podInformer.GetStore().Add(pod))
	h.manager.rebuildSelectedNodeState()
}

func (h *persistentAgentHarness) syncSnapshotInformer(t *testing.T) *nvapi.ComputeDomainCliqueSnapshot {
	t.Helper()
	obj, err := h.nvidia.Tracker().Get(
		nvapi.SchemeGroupVersion.WithResource("computedomaincliquesnapshots"),
		"driver",
		cdclique.SnapshotName(string(h.cd.UID), h.cliqueID),
	)
	require.NoError(t, err)
	snapshot, ok := obj.(*nvapi.ComputeDomainCliqueSnapshot)
	require.True(t, ok)
	key := snapshot.Namespace + "/" + snapshot.Name
	_, exists, err := h.manager.snapshotInformer.GetStore().GetByKey(key)
	require.NoError(t, err)
	if exists {
		require.NoError(t, h.manager.snapshotInformer.GetStore().Update(snapshot.DeepCopy()))
	} else {
		require.NoError(t, h.manager.snapshotInformer.GetStore().Add(snapshot.DeepCopy()))
	}
	return snapshot.DeepCopy()
}

func fakeActionNames(actions []observedFakeAPIAction) []string {
	names := make([]string, len(actions))
	for i := range actions {
		names[i] = actions[i].verb + ":" + actions[i].resource
		if actions[i].subresource != "" {
			names[i] += "/" + actions[i].subresource
		}
		if actions[i].result != "success" {
			names[i] += ":" + actions[i].result
		}
	}
	return names
}

func countFakeMutations(actions []observedFakeAPIAction, resource string) int {
	count := 0
	for _, action := range actions {
		if action.resource == resource && action.mutated {
			count++
		}
	}
	return count
}

func TestIndexedReconcileInputsAreCliqueScoped(t *testing.T) {
	nodes := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		computeDomainCliqueIndex: nodeComputeDomainCliqueIndexKeys,
	})
	pods := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		persistentAgentPodNodeIndex: persistentAgentPodNodeIndexKeys,
	})
	for clique := range 2 {
		cliqueID := fmt.Sprintf("clique-%d", clique)
		for member := range 3 {
			nodeName := fmt.Sprintf("node-%d-%d", clique, member)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				UID:  types.UID("uid-" + nodeName),
				Labels: map[string]string{
					computeDomainLabelKey:            "cd",
					gpuCliqueNodeLabelKey:            cliqueID,
					persistentAgentIsolationLabelKey: "cd",
				},
			}}
			setTestNodeAttestation(t, node, "cd", "pod-"+nodeName)
			require.NoError(t, nodes.Add(node))
			require.NoError(t, pods.Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-" + nodeName,
				Namespace: "driver",
				Labels:    map[string]string{persistentAgentLabelKey: "true"},
			}, Spec: corev1.PodSpec{NodeName: nodeName}}))
		}
	}
	require.NoError(t, nodes.Add(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "unattested-spoof",
		Labels: map[string]string{
			computeDomainLabelKey: "cd",
			gpuCliqueNodeLabelKey: "clique-1",
		},
	}}))

	selected, err := selectedNodesForClique(nodes, "cd", "clique-1")
	require.NoError(t, err)
	require.Len(t, selected, 3)
	candidates, err := (snapshotDaemonProvider{}).candidatePods(pods, "cd", selected)
	require.NoError(t, err)
	require.Len(t, candidates, 3)
	for _, candidate := range candidates {
		require.Contains(t, candidate.Spec.NodeName, "node-1-")
	}
	require.True(t, expectedSetReadyForSummary(selectedNodeSummary{selected: 6, topologyReady: 6}, 6))
	require.False(t, expectedSetReadyForSummary(selectedNodeSummary{selected: 6, topologyReady: 5}, 6))
}

func TestNodeEventsBroadcastOnlyOnExpectedSetTransition(t *testing.T) {
	cd := &nvapi.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{Name: "domain", Namespace: "workload", UID: types.UID("cd")},
		Spec:       nvapi.ComputeDomainSpec{NumNodes: 4},
	}
	m := NewPersistentAgentManager(&ManagerConfig{
		driverNamespace: "driver",
		clientsets:      flags.ClientSets{Nvidia: nvfake.NewSimpleClientset(cd.DeepCopy())},
	})
	require.NoError(t, m.addInformerIndexes())
	require.NoError(t, m.computeDomainInformer.GetStore().Add(cd.DeepCopy()))
	t.Cleanup(func() {
		m.queue.ShutDown()
		m.attestationQueue.ShutDown()
	})

	for _, cliqueID := range []string{"clique-a", "clique-b"} {
		require.NoError(t, m.snapshotInformer.GetStore().Add(&nvapi.ComputeDomainCliqueSnapshot{
			ObjectMeta: metav1.ObjectMeta{Namespace: "driver", Name: cdclique.SnapshotName(string(cd.UID), cliqueID)},
			Spec: nvapi.ComputeDomainCliqueSnapshotSpec{
				ComputeDomainUID: cd.UID,
				CliqueID:         cliqueID,
			},
		}))
	}
	var changed *corev1.Node
	for clique := range 2 {
		cliqueID := fmt.Sprintf("clique-%c", 'a'+clique)
		for member := range 2 {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("node-%d-%d", clique, member),
				Labels: map[string]string{
					computeDomainLabelKey:            string(cd.UID),
					gpuCliqueNodeLabelKey:            cliqueID,
					persistentAgentIsolationLabelKey: string(cd.UID),
				},
				Annotations: map[string]string{
					computeDomainCliqueStartupAnnotationKey:    cliqueID,
					computeDomainCliqueCapabilityAnnotationKey: string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1),
				},
			}}
			setTestNodeAttestation(t, node, string(cd.UID), "pod-"+node.Name)
			require.NoError(t, m.nodeInformer.GetStore().Add(node))
			if clique == 0 && member == 0 {
				changed = node
			}
		}
	}
	m.rebuildSelectedNodeState()
	require.True(t, m.expectedSetReady(string(cd.UID), cd.Spec.NumNodes))

	drain := func() []string {
		keys := make([]string, 0, m.queue.Len())
		for m.queue.Len() > 0 {
			key, shutdown := m.queue.Get()
			require.False(t, shutdown)
			keys = append(keys, key)
			m.queue.Done(key)
		}
		slices.Sort(keys)
		return keys
	}

	// An unrelated Node update touches only its clique, even though another
	// clique snapshot exists for the same ComputeDomain.
	unrelated := changed.DeepCopy()
	unrelated.Annotations["example.com/unrelated"] = "changed"
	m.handleNodeEvent(changed, unrelated)
	require.Equal(t, []string{"driver/" + cdclique.SnapshotName(string(cd.UID), "clique-a")}, drain())

	// Losing topology readiness crosses the domain-wide expected-set barrier,
	// so all existing clique snapshots must be re-evaluated exactly once.
	notReady := unrelated.DeepCopy()
	delete(notReady.Annotations, computeDomainCliqueCapabilityAnnotationKey)
	m.handleNodeEvent(unrelated, notReady)
	wantBroadcast := []string{
		"driver/" + cdclique.SnapshotName(string(cd.UID), "clique-a"),
		"driver/" + cdclique.SnapshotName(string(cd.UID), "clique-b"),
	}
	slices.Sort(wantBroadcast)
	require.Equal(t, wantBroadcast, drain())
}

func TestPersistentAgentExpectedSetFormationActionSequence(t *testing.T) {
	h := newPersistentAgentHarness(t, 2)
	h.addNodeAndPod(t, 0)

	// Creation establishes physical ownership and the Pending snapshot, but an
	// incomplete expected set must not publish assignments or status.
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	require.Equal(t, []string{
		"create:computedomaincliquereservations",
		"create:computedomaincliquesnapshots",
	}, fakeActionNames(h.observed))
	snapshot := h.syncSnapshotInformer(t)
	require.Zero(t, snapshot.Status.Generation)
	require.Empty(t, snapshot.Finalizers)

	actionsBeforeIncompleteReconcile := len(h.observed)
	h.manager.pendingScopes[h.key] = snapshotScope{computeDomainUID: string(h.cd.UID), cliqueID: h.cliqueID}
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	require.Len(t, h.observed, actionsBeforeIncompleteReconcile, "an incomplete expected set must not issue an API call")

	// The final Node makes the exact expected set ready. Expire the test's
	// in-memory batch timer so the next reconcile reaches the API immediately.
	h.addNodeAndPod(t, 1)
	h.manager.batchStarted[h.key] = time.Now().Add(-snapshotHardDeadline)
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	require.Equal(t, []string{
		"create:computedomaincliquereservations",
		"create:computedomaincliquesnapshots",
		"update:computedomaincliquesnapshots",
	}, fakeActionNames(h.observed))
	snapshot = h.syncSnapshotInformer(t)
	require.Contains(t, snapshot.Finalizers, nvapi.ComputeDomainCliqueSnapshotFinalizer)
	require.Zero(t, snapshot.Status.Generation, "the finalizer must commit before Active status")

	// The finalizer is a separate mutation. Expire the restarted initial-status
	// batch as well so this deterministic harness does not wait on wall time.
	h.manager.batchStarted[h.key] = time.Now().Add(-snapshotHardDeadline)
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	require.Equal(t, []string{
		"create:computedomaincliquereservations",
		"create:computedomaincliquesnapshots",
		"update:computedomaincliquesnapshots",
		"get:computedomaincliquereservations",
		"update:computedomaincliquereservations/status",
		"update:computedomaincliquesnapshots/status",
	}, fakeActionNames(h.observed))
	snapshot = h.syncSnapshotInformer(t)
	require.Equal(t, nvapi.ComputeDomainCliqueSnapshotPhaseActive, snapshot.Status.Phase)
	require.EqualValues(t, 1, snapshot.Status.Generation)
	require.Len(t, snapshot.Status.Assignments, 2)
	require.Len(t, snapshot.Status.Members, 2)

	// Formation has exactly three snapshot mutations plus the cluster-scoped
	// reservation Create and activation-status write. The activation identity
	// makes a missing snapshot distinguishable from a never-published stream.
	require.Equal(t, 3, countFakeMutations(h.observed, "computedomaincliquesnapshots"))
	require.Equal(t, 2, countFakeMutations(h.observed, "computedomaincliquereservations"))
	require.Len(t, h.observed, 6)

	// A semantic no-op uses the immutable reservation cache and makes no API call.
	writesBeforeNoop := countFakeMutations(h.observed, "computedomaincliquesnapshots") + countFakeMutations(h.observed, "computedomaincliquereservations")
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	require.Equal(t, writesBeforeNoop, countFakeMutations(h.observed, "computedomaincliquesnapshots")+countFakeMutations(h.observed, "computedomaincliquereservations"))
	require.Empty(t, h.observed[6:])
}

func TestPersistentAgentExpectedSetUsesSharedStateMachine(t *testing.T) {
	h := newPersistentAgentHarness(t, 2)
	h.addNodeAndPod(t, 0)
	h.addNodeAndPod(t, 1)

	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	snapshot := h.syncSnapshotInformer(t)
	require.Equal(t, nvapi.ComputeDomainCliqueProtocolPersistentAgentV1, snapshot.Spec.Protocol)
	require.Empty(t, snapshot.OwnerReferences, "installation-scoped agent deletion must not garbage-collect a published snapshot")
	h.manager.batchStarted[h.key] = time.Now().Add(-snapshotHardDeadline)
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	h.syncSnapshotInformer(t)
	h.manager.batchStarted[h.key] = time.Now().Add(-snapshotHardDeadline)
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	snapshot = h.syncSnapshotInformer(t)

	require.Equal(t, nvapi.ComputeDomainCliqueSnapshotPhaseActive, snapshot.Status.Phase)
	require.Len(t, snapshot.Status.Members, 2)
	require.False(t, h.manager.persistentComputeDomainReady(h.cd), "an Active snapshot without exact agent acknowledgments is not globally Ready")
	for i := range snapshot.Status.Members {
		member := &snapshot.Status.Members[i]
		object, exists, err := h.manager.podInformer.GetStore().GetByKey(snapshot.Namespace + "/" + member.PodName)
		require.NoError(t, err)
		require.True(t, exists)
		pod, ok := object.(*corev1.Pod)
		require.True(t, ok)
		pod = pod.DeepCopy()
		receipt, err := json.Marshal(nvapi.ComputeDomainCliqueSnapshotReceipt{
			SnapshotUID: snapshot.UID, SnapshotGeneration: snapshot.Status.Generation, SnapshotHash: snapshot.Status.Hash,
			NodeUID: member.NodeUID, PodUID: member.PodUID, Index: member.Index,
		})
		require.NoError(t, err)
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[nvapi.ComputeDomainCliqueSnapshotAppliedAnnotation] = string(receipt)
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		require.NoError(t, h.manager.podInformer.GetStore().Update(pod))
	}
	require.True(t, h.manager.persistentComputeDomainReady(h.cd))
	const concurrentStatusReconciles = 16
	errors := make(chan error, concurrentStatusReconciles)
	var group sync.WaitGroup
	group.Add(concurrentStatusReconciles)
	for range concurrentStatusReconciles {
		go func() {
			defer group.Done()
			errors <- h.manager.updatePersistentComputeDomainStatusForKey(context.Background(), h.key)
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	statusActions := make([]observedFakeAPIAction, 0, 2)
	for _, action := range h.observed {
		if action.resource == "computedomains" {
			statusActions = append(statusActions, action)
		}
	}
	require.Equal(t, []string{"get:computedomains", "update:computedomains/status"}, fakeActionNames(statusActions), "concurrent clique completions must coalesce to one domain status transition")
	require.Equal(t, 3, countFakeMutations(h.observed, "computedomaincliquesnapshots"))
	require.Equal(t, 2, countFakeMutations(h.observed, "computedomaincliquereservations"))
	for _, action := range h.observed {
		require.NotEqual(t, "daemonsets", action.resource)
		require.NotEqual(t, "resourceclaimtemplates", action.resource)
	}
}

func TestPublishedMemberPreservesActivationBootIDAcrossQuarantineRecovery(t *testing.T) {
	h := newPersistentAgentHarness(t, 1)
	h.addNodeAndPod(t, 0)

	// Create the reservation and snapshot, add the fence finalizer, then
	// publish the first complete Active generation.
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	h.syncSnapshotInformer(t)
	h.manager.batchStarted[h.key] = time.Now().Add(-snapshotHardDeadline)
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	h.syncSnapshotInformer(t)
	h.manager.batchStarted[h.key] = time.Now().Add(-snapshotHardDeadline)
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	active := h.syncSnapshotInformer(t)
	require.Equal(t, nvapi.ComputeDomainCliqueSnapshotPhaseActive, active.Status.Phase)
	require.EqualValues(t, 1, active.Status.Generation)
	require.Len(t, active.Status.Members, 1)
	require.Equal(t, "boot-00", active.Status.Members[0].NodeBootID)
	originalHash := active.Status.Hash
	originalPodUID := active.Status.Members[0].PodUID

	// Model the observation gap seen during a real reboot. The published Pod is
	// temporarily absent, so the assignment quarantines and the last authorized
	// member map remains frozen.
	podObjects := h.manager.podInformer.GetStore().List()
	require.Len(t, podObjects, 1)
	publishedPod, ok := podObjects[0].(*corev1.Pod)
	require.True(t, ok)
	pod := publishedPod.DeepCopy()
	require.NoError(t, h.manager.podInformer.GetStore().Delete(pod))
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	quarantined := h.syncSnapshotInformer(t)
	require.Equal(t, nvapi.ComputeDomainCliqueAssignmentStateQuarantined, quarantined.Status.Assignments[0].State)
	require.Equal(t, "boot-00", quarantined.Status.Members[0].NodeBootID)
	require.Equal(t, originalHash, quarantined.Status.Hash)

	// Kubernetes may preserve the Pod UID while restarting its container after
	// the Node boots. Recovery to Bound is safe for the exact incumbent, but it
	// must not rewrite the activation epoch or roll the authorized generation.
	nodeObject, exists, err := h.manager.nodeInformer.GetStore().GetByKey("node-00")
	require.NoError(t, err)
	require.True(t, exists)
	publishedNode, ok := nodeObject.(*corev1.Node)
	require.True(t, ok)
	rebootedNode := publishedNode.DeepCopy()
	rebootedNode.Status.NodeInfo.BootID = "boot-after-reboot"
	require.NoError(t, h.manager.nodeInformer.GetStore().Update(rebootedNode))
	require.NoError(t, h.manager.podInformer.GetStore().Add(pod))
	h.manager.rebuildSelectedNodeState()
	require.NoError(t, h.manager.reconcile(context.Background(), h.key))
	recovered := h.syncSnapshotInformer(t)
	require.Equal(t, nvapi.ComputeDomainCliqueAssignmentStateBound, recovered.Status.Assignments[0].State)
	require.Equal(t, originalPodUID, recovered.Status.Members[0].PodUID)
	require.Equal(t, "boot-00", recovered.Status.Members[0].NodeBootID)
	require.Equal(t, originalHash, recovered.Status.Hash)
	require.EqualValues(t, 1, recovered.Status.Generation)
}
