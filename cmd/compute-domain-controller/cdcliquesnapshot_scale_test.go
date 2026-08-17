/*
Copyright The Kubernetes Authors

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
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clienttesting "k8s.io/client-go/testing"
	draclient "k8s.io/dynamic-resource-allocation/client"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/cdclique"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	nvfake "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned/fake"
)

type persistentAgentScaleStats struct {
	nodes          int
	cliques        int
	actions        int
	writes         int
	requestBytes   int
	watchBytes     int
	attestation    time.Duration
	snapshotCreate time.Duration
	finalizer      time.Duration
	activation     time.Duration
	noop           time.Duration
	actionsByKind  map[string]int
}

type persistentAgentScaleHarness struct {
	manager   *PersistentAgentManager
	core      *attestationCoreClient
	nvidia    *nvfake.Clientset
	cd        *nvapi.ComputeDomain
	cliqueIDs []string
	nodeNames []string
	stats     persistentAgentScaleStats
	nextRV    int
}

func newPersistentAgentScaleHarness(tb testing.TB, cliques, membersPerClique int) *persistentAgentScaleHarness {
	tb.Helper()
	const (
		driverNamespace   = "driver"
		workloadNamespace = "workload"
	)
	cdUID := types.UID("scale-compute-domain")
	templateName := "scale-channel"
	channel := nvapi.DefaultComputeDomainChannelConfig()
	channel.DomainID = string(cdUID)
	channel.AllocationMode = nvapi.ComputeDomainChannelAllocationModeSingle
	channel.Protocol = nvapi.ComputeDomainCliqueProtocolPersistentAgentV1
	configBytes, err := json.Marshal(channel)
	if err != nil {
		tb.Fatal(err)
	}
	claimSpec := resourceapi.ResourceClaimSpec{Devices: resourceapi.DeviceClaim{
		Requests: []resourceapi.DeviceRequest{{Name: "channel", Exactly: &resourceapi.ExactDeviceRequest{
			DeviceClassName: computeDomainDefaultChannelDeviceClass,
			AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
			Count:           1,
		}}},
		Config: []resourceapi.DeviceClaimConfiguration{{Requests: []string{"channel"}, DeviceConfiguration: resourceapi.DeviceConfiguration{
			Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: DriverName, Parameters: runtime.RawExtension{Raw: configBytes}},
		}}},
	}}
	cd := &nvapi.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "scale-domain",
			Namespace: workloadNamespace,
			UID:       cdUID,
			Annotations: map[string]string{
				nvapi.ComputeDomainCliqueProtocolAnnotation: string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1),
			},
		},
		Spec: nvapi.ComputeDomainSpec{
			NumNodes: cliques * membersPerClique,
			Channel: &nvapi.ComputeDomainChannelSpec{
				ResourceClaimTemplate: nvapi.ComputeDomainResourceClaimTemplate{Name: templateName},
				AllocationMode:        nvapi.ComputeDomainChannelAllocationModeSingle,
			},
		},
	}
	rct := &resourceapi.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      templateName,
			Namespace: workloadNamespace,
			Labels: map[string]string{
				computeDomainLabelKey:                            string(cdUID),
				computeDomainResourceClaimTemplateTargetLabelKey: computeDomainResourceClaimTemplateTargetWorkload,
			},
		},
		Spec: resourceapi.ResourceClaimTemplateSpec{Spec: claimSpec},
	}
	dsController := true
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      persistentAgentDaemonSetName,
			Namespace: driverNamespace,
			UID:       "scale-agent-daemonset",
			Labels:    map[string]string{persistentAgentLabelKey: "true"},
		},
		Spec: appsv1.DaemonSetSpec{
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{persistentAgentLabelKey: "true"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: persistentAgentServiceAccountName,
					Containers: []corev1.Container{{
						Name: "compute-domain-daemon", Command: []string{"compute-domain-daemon", "run", "--persistent-agent"},
					}},
				},
			},
		},
	}

	coreClient := &attestationCoreClient{nodes: make(map[string]*corev1.Node)}
	nvidia := nvfake.NewSimpleClientset(cd.DeepCopy())
	manager := NewPersistentAgentManager(&ManagerConfig{
		driverNamespace:       driverNamespace,
		maxNodesPerIMEXDomain: membersPerClique,
		clientsets: flags.ClientSets{
			Core: coreClient, Resource: draclient.New(coreClient), Nvidia: nvidia,
		},
	})
	if err := manager.addInformerIndexes(); err != nil {
		tb.Fatal(err)
	}
	h := &persistentAgentScaleHarness{
		manager: manager, core: coreClient, nvidia: nvidia, cd: cd,
		cliqueIDs: make([]string, cliques),
		nodeNames: make([]string, 0, cliques*membersPerClique),
		nextRV:    1,
		stats: persistentAgentScaleStats{
			nodes: cliques * membersPerClique, cliques: cliques, actionsByKind: make(map[string]int),
		},
	}
	coreClient.nodeUpdate = func(node *corev1.Node) {
		h.recordMutation(tb, "nodes:update", node)
	}
	h.observeNvidiaAPI(tb)
	if err := manager.computeDomainInformer.GetStore().Add(cd.DeepCopy()); err != nil {
		tb.Fatal(err)
	}
	if err := manager.claimTemplateInformer.GetStore().Add(rct.DeepCopy()); err != nil {
		tb.Fatal(err)
	}
	if err := manager.daemonSetInformer.GetStore().Add(ds.DeepCopy()); err != nil {
		tb.Fatal(err)
	}

	for clique := range cliques {
		cliqueID := fmt.Sprintf("clique-%03d", clique)
		h.cliqueIDs[clique] = cliqueID
		key := driverNamespace + "/" + cdclique.SnapshotName(string(cdUID), cliqueID)
		manager.pendingScopes[key] = snapshotScope{computeDomainUID: string(cdUID), cliqueID: cliqueID}
		for member := range membersPerClique {
			ordinal := clique*membersPerClique + member
			suffix := fmt.Sprintf("%03d-%03d", clique, member)
			nodeName := "node-" + suffix
			podName := "workload-" + suffix
			podUID := types.UID("workload-uid-" + suffix)
			claimName := "claim-" + suffix
			claimUID := types.UID("claim-uid-" + suffix)
			h.nodeNames = append(h.nodeNames, nodeName)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				UID:  types.UID("node-uid-" + suffix),
				Labels: map[string]string{
					gpuCliqueNodeLabelKey:            cliqueID,
					persistentAgentIsolationLabelKey: string(cdUID),
				},
				Annotations: map[string]string{
					computeDomainCliqueStartupAnnotationKey:    cliqueID,
					computeDomainCliqueCapabilityAnnotationKey: string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1),
				},
			}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{BootID: "boot-" + suffix}}}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: workloadNamespace, UID: podUID},
				Spec: corev1.PodSpec{NodeName: nodeName, ResourceClaims: []corev1.PodResourceClaim{{
					Name: "channel", ResourceClaimTemplateName: &templateName,
				}}},
				Status: corev1.PodStatus{
					Phase:      corev1.PodPending,
					Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}},
					ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{
						Name: "channel", ResourceClaimName: &claimName,
					}},
				},
			}
			claimController := true
			claim := &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: claimName, Namespace: workloadNamespace, UID: claimUID,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "v1", Kind: "Pod", Name: podName, UID: podUID, Controller: &claimController,
					}},
				},
				Spec: claimSpec,
				Status: resourceapi.ResourceClaimStatus{
					ReservedFor: []resourceapi.ResourceClaimConsumerReference{{Resource: "pods", Name: podName, UID: podUID}},
					Allocation: &resourceapi.AllocationResult{
						NodeSelector: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchFields: []corev1.NodeSelectorRequirement{{
								Key: metav1.ObjectNameField, Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName},
							}},
						}}},
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{{
								Request: "channel", Driver: DriverName, Pool: nodeName, Device: "channel-0",
							}},
							Config: []resourceapi.DeviceAllocationConfiguration{{
								Source:   resourceapi.AllocationConfigSourceClaim,
								Requests: []string{"channel"},
								DeviceConfiguration: resourceapi.DeviceConfiguration{Opaque: &resourceapi.OpaqueDeviceConfiguration{
									Driver: DriverName, Parameters: runtime.RawExtension{Raw: configBytes},
								}},
							}},
						},
					},
				},
			}
			agent := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "agent-" + suffix,
					Namespace: driverNamespace,
					UID:       types.UID("agent-uid-" + suffix),
					Labels:    map[string]string{persistentAgentLabelKey: "true"},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: appsv1.SchemeGroupVersion.String(), Kind: "DaemonSet", Name: ds.Name, UID: ds.UID, Controller: &dsController,
					}},
				},
				Spec: corev1.PodSpec{NodeName: nodeName},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					PodIP: fmt.Sprintf("10.%d.%d.%d", 1+(ordinal/65025)%254, (ordinal/255)%255, 1+ordinal%254),
				},
			}
			coreClient.putNode(node)
			for _, add := range []struct {
				store  interface{ Add(any) error }
				object any
			}{
				{manager.nodeInformer.GetStore(), node.DeepCopy()},
				{manager.workloadPodInformer.GetStore(), pod.DeepCopy()},
				{manager.resourceClaimInformer.GetStore(), claim.DeepCopy()},
				{manager.podInformer.GetStore(), agent.DeepCopy()},
			} {
				if err := add.store.Add(add.object); err != nil {
					tb.Fatal(err)
				}
			}
		}
	}
	return h
}

func (h *persistentAgentScaleHarness) close() {
	h.manager.queue.ShutDown()
	h.manager.attestationQueue.ShutDown()
}

func (h *persistentAgentScaleHarness) recordMutation(tb testing.TB, kind string, object any) {
	tb.Helper()
	body, err := json.Marshal(object)
	if err != nil {
		tb.Fatal(err)
	}
	h.stats.actions++
	h.stats.writes++
	h.stats.requestBytes += len(body)
	h.stats.watchBytes += len(body)
	h.stats.actionsByKind[kind]++
}

func (h *persistentAgentScaleHarness) observeNvidiaAPI(tb testing.TB) {
	tb.Helper()
	objectReaction := clienttesting.ObjectReaction(h.nvidia.Tracker())
	h.nvidia.PrependReactor("*", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		var requestBytes int
		if objectAction, ok := action.(interface{ GetObject() runtime.Object }); ok {
			body, err := json.Marshal(objectAction.GetObject())
			if err != nil {
				tb.Fatal(err)
			}
			requestBytes = len(body)
			if action.GetVerb() == "create" || action.GetVerb() == "update" {
				accessor, err := apiMeta.Accessor(objectAction.GetObject())
				if err != nil {
					tb.Fatal(err)
				}
				accessor.SetResourceVersion(strconv.Itoa(h.nextRV))
				if accessor.GetUID() == "" {
					accessor.SetUID(types.UID(fmt.Sprintf("scale-uid-%d", h.nextRV)))
				}
				h.nextRV++
			}
		}
		_, result, reactionErr := objectReaction(action)
		h.stats.actions++
		kind := action.GetResource().Resource
		if action.GetSubresource() != "" {
			kind += "/" + action.GetSubresource()
		}
		h.stats.actionsByKind[kind+":"+action.GetVerb()]++
		h.stats.requestBytes += requestBytes
		if reactionErr == nil && (action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "delete" || action.GetVerb() == "patch") {
			h.stats.writes++
			if result != nil {
				body, err := json.Marshal(result)
				if err != nil {
					tb.Fatal(err)
				}
				h.stats.watchBytes += len(body)
			}
		}
		return true, result, reactionErr
	})
}

func (h *persistentAgentScaleHarness) syncSnapshot(tb testing.TB, cliqueID string) *nvapi.ComputeDomainCliqueSnapshot {
	tb.Helper()
	name := cdclique.SnapshotName(string(h.cd.UID), cliqueID)
	object, err := h.nvidia.Tracker().Get(nvapi.SchemeGroupVersion.WithResource("computedomaincliquesnapshots"), "driver", name)
	if err != nil {
		tb.Fatal(err)
	}
	snapshot, ok := object.(*nvapi.ComputeDomainCliqueSnapshot)
	if !ok {
		tb.Fatalf("unexpected snapshot object %T", object)
	}
	key := snapshot.Namespace + "/" + snapshot.Name
	_, exists, err := h.manager.snapshotInformer.GetStore().GetByKey(key)
	if err != nil {
		tb.Fatal(err)
	}
	if exists {
		err = h.manager.snapshotInformer.GetStore().Update(snapshot.DeepCopy())
	} else {
		err = h.manager.snapshotInformer.GetStore().Add(snapshot.DeepCopy())
	}
	if err != nil {
		tb.Fatal(err)
	}
	return snapshot.DeepCopy()
}

func (h *persistentAgentScaleHarness) run(tb testing.TB) persistentAgentScaleStats {
	tb.Helper()
	ctx := context.Background()
	started := time.Now()
	for _, nodeName := range h.nodeNames {
		if err := h.manager.reconcileNodeAttestation(ctx, nodeName); err != nil {
			tb.Fatal(err)
		}
		node, err := h.core.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			tb.Fatal(err)
		}
		if err := h.manager.nodeInformer.GetStore().Update(node.DeepCopy()); err != nil {
			tb.Fatal(err)
		}
	}
	h.stats.attestation = time.Since(started)
	h.manager.rebuildSelectedNodeState()

	started = time.Now()
	for _, cliqueID := range h.cliqueIDs {
		key := "driver/" + cdclique.SnapshotName(string(h.cd.UID), cliqueID)
		if err := h.manager.reconcile(ctx, key); err != nil {
			tb.Fatal(err)
		}
		h.syncSnapshot(tb, cliqueID)
	}
	h.stats.snapshotCreate = time.Since(started)

	started = time.Now()
	for _, cliqueID := range h.cliqueIDs {
		key := "driver/" + cdclique.SnapshotName(string(h.cd.UID), cliqueID)
		h.manager.batchStarted[key] = time.Now().Add(-snapshotHardDeadline)
		if err := h.manager.reconcile(ctx, key); err != nil {
			tb.Fatal(err)
		}
		h.syncSnapshot(tb, cliqueID)
	}
	h.stats.finalizer = time.Since(started)

	started = time.Now()
	for _, cliqueID := range h.cliqueIDs {
		key := "driver/" + cdclique.SnapshotName(string(h.cd.UID), cliqueID)
		h.manager.batchStarted[key] = time.Now().Add(-snapshotHardDeadline)
		if err := h.manager.reconcile(ctx, key); err != nil {
			tb.Fatal(err)
		}
		snapshot := h.syncSnapshot(tb, cliqueID)
		if snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseActive {
			tb.Fatalf("clique %s phase %q, want Active", cliqueID, snapshot.Status.Phase)
		}
	}
	h.stats.activation = time.Since(started)

	actionsBeforeNoop := h.stats.actions
	started = time.Now()
	for _, cliqueID := range h.cliqueIDs {
		key := "driver/" + cdclique.SnapshotName(string(h.cd.UID), cliqueID)
		if err := h.manager.reconcile(ctx, key); err != nil {
			tb.Fatal(err)
		}
	}
	h.stats.noop = time.Since(started)
	if h.stats.actions != actionsBeforeNoop {
		tb.Fatalf("steady-state no-op issued %d API actions", h.stats.actions-actionsBeforeNoop)
	}
	return h.stats
}

func TestPersistentAgentScaleHarnessAccountsForFormation(t *testing.T) {
	h := newPersistentAgentScaleHarness(t, 2, 2)
	defer h.close()
	stats := h.run(t)
	requireScaleEqual(t, stats.actions, stats.nodes+6*stats.cliques, "API actions")
	requireScaleEqual(t, stats.writes, stats.nodes+5*stats.cliques, "confirmed writes")
	expectedActions := map[string]int{
		"nodes:update":                                  stats.nodes,
		"computedomaincliquereservations:create":        stats.cliques,
		"computedomaincliquereservations:get":           stats.cliques,
		"computedomaincliquereservations/status:update": stats.cliques,
		"computedomaincliquesnapshots:create":           stats.cliques,
		"computedomaincliquesnapshots:update":           stats.cliques,
		"computedomaincliquesnapshots/status:update":    stats.cliques,
	}
	if diff := cmp.Diff(expectedActions, stats.actionsByKind); diff != "" {
		t.Fatalf("API actions differ (-want +got):\n%s", diff)
	}
	if stats.requestBytes == 0 || stats.watchBytes == 0 {
		t.Fatalf("byte accounting is empty: request=%d watch=%d", stats.requestBytes, stats.watchBytes)
	}
}

func requireScaleEqual(tb testing.TB, got, want int, what string) {
	tb.Helper()
	if got != want {
		tb.Fatalf("%s = %d, want %d", what, got, want)
	}
}

func benchmarkPersistentAgentFormation(b *testing.B, cliques, membersPerClique int) {
	var totals persistentAgentScaleStats
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		h := newPersistentAgentScaleHarness(b, cliques, membersPerClique)
		b.StartTimer()
		stats := h.run(b)
		b.StopTimer()
		h.close()
		requireScaleEqual(b, stats.actions, stats.nodes+6*stats.cliques, "API actions")
		requireScaleEqual(b, stats.writes, stats.nodes+5*stats.cliques, "confirmed writes")
		totals.actions += stats.actions
		totals.writes += stats.writes
		totals.requestBytes += stats.requestBytes
		totals.watchBytes += stats.watchBytes
		totals.attestation += stats.attestation
		totals.snapshotCreate += stats.snapshotCreate
		totals.finalizer += stats.finalizer
		totals.activation += stats.activation
		totals.noop += stats.noop
		b.StartTimer()
	}
	b.StopTimer()
	operations := float64(b.N)
	b.ReportMetric(float64(cliques*membersPerClique), "nodes/op")
	b.ReportMetric(float64(cliques), "cliques/op")
	b.ReportMetric(float64(totals.actions)/operations, "api-actions/op")
	b.ReportMetric(float64(totals.writes)/operations, "api-writes/op")
	b.ReportMetric(float64(totals.requestBytes)/operations, "fixture-request-bytes/op")
	b.ReportMetric(float64(totals.watchBytes)/operations, "fixture-watch-bytes/op")
	b.ReportMetric(float64(totals.attestation.Nanoseconds())/operations, "attestation-ns/op")
	b.ReportMetric(float64(totals.snapshotCreate.Nanoseconds())/operations, "snapshot-create-ns/op")
	b.ReportMetric(float64(totals.finalizer.Nanoseconds())/operations, "finalizer-ns/op")
	b.ReportMetric(float64(totals.activation.Nanoseconds())/operations, "activation-ns/op")
	b.ReportMetric(float64(totals.noop.Nanoseconds())/operations, "noop-ns/op")
}

func BenchmarkPersistentAgentFormation18(b *testing.B) {
	benchmarkPersistentAgentFormation(b, 1, 18)
}

func BenchmarkPersistentAgentFormation144(b *testing.B) {
	benchmarkPersistentAgentFormation(b, 1, 144)
}

func BenchmarkPersistentAgentFormation280x18(b *testing.B) {
	benchmarkPersistentAgentFormation(b, 280, 18)
}
