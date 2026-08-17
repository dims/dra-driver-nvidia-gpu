//go:build integration

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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	draclient "k8s.io/dynamic-resource-allocation/client"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/cdclique"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
	drivermetrics "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/metrics"
	nvclientset "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/clientset/versioned"
)

type realAPIScaleTransport struct {
	requests      atomic.Int64
	requestBytes  atomic.Int64
	responseBytes atomic.Int64
	watchBytes    atomic.Int64
	mu            sync.Mutex
	byKind        map[string]int64
}

type realAPIScaleTransportSnapshot struct {
	Requests      int64            `json:"requests"`
	RequestBytes  int64            `json:"requestBodyBytes"`
	ResponseBytes int64            `json:"responseBodyBytes"`
	WatchBytes    int64            `json:"watchBodyBytes"`
	ByKind        map[string]int64 `json:"byKind"`
}

type realAPIScaleResult struct {
	Nodes                  int                           `json:"nodes"`
	Cliques                int                           `json:"cliques"`
	ComputeDomains         int                           `json:"computeDomains"`
	FixtureSetupSeconds    float64                       `json:"fixtureSetupSeconds"`
	StartToActiveSeconds   float64                       `json:"startToActiveSeconds"`
	StartToReadySeconds    float64                       `json:"startToReadySeconds"`
	CliqueActions          int64                         `json:"cliqueActions"`
	CliqueWrites           int64                         `json:"cliqueWrites"`
	ComputeDomainActions   int64                         `json:"computeDomainActions"`
	ComputeDomainWrites    int64                         `json:"computeDomainWrites"`
	TotalControllerActions int64                         `json:"totalControllerActions"`
	TotalControllerWrites  int64                         `json:"totalControllerWrites"`
	Conflicts              int64                         `json:"conflicts"`
	Throttled              int64                         `json:"throttled"`
	Transport              realAPIScaleTransportSnapshot `json:"transport"`
}

type countingReadCloser struct {
	io.ReadCloser
	total *atomic.Int64
	watch *atomic.Int64
}

func (r *countingReadCloser) Read(buffer []byte) (int, error) {
	n, err := r.ReadCloser.Read(buffer)
	r.total.Add(int64(n))
	if r.watch != nil {
		r.watch.Add(int64(n))
	}
	return n, err
}

type scaleRoundTripper struct {
	next  http.RoundTripper
	stats *realAPIScaleTransport
}

func (t *scaleRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	t.stats.requests.Add(1)
	if request.Body != nil {
		request.Body = &countingReadCloser{ReadCloser: request.Body, total: &t.stats.requestBytes}
	}
	response, err := t.next.RoundTrip(request)
	if err != nil {
		t.stats.record(request.Method, scaleAPIKind(request.URL.Path, request.URL.RawQuery), 0)
		return nil, err
	}
	watchCounter := (*atomic.Int64)(nil)
	if request.URL.Query().Get("watch") == "true" {
		watchCounter = &t.stats.watchBytes
	}
	if response.Body != nil {
		response.Body = &countingReadCloser{ReadCloser: response.Body, total: &t.stats.responseBytes, watch: watchCounter}
	}
	t.stats.record(request.Method, scaleAPIKind(request.URL.Path, request.URL.RawQuery), response.StatusCode)
	return response, nil
}

func (s *realAPIScaleTransport) record(method, kind string, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKind[fmt.Sprintf("%s %s %d", method, kind, status)]++
}

func (s *realAPIScaleTransport) snapshot() realAPIScaleTransportSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKind := make(map[string]int64, len(s.byKind))
	for kind, count := range s.byKind {
		byKind[kind] = count
	}
	return realAPIScaleTransportSnapshot{
		Requests: s.requests.Load(), RequestBytes: s.requestBytes.Load(),
		ResponseBytes: s.responseBytes.Load(), WatchBytes: s.watchBytes.Load(), ByKind: byKind,
	}
}

func scaleAPIKind(path, rawQuery string) string {
	for _, resource := range []string{
		"computedomaincliquereservations", "computedomaincliquesnapshots",
		"computedomains", "resourceclaimtemplates", "resourceclaims",
		"daemonsets", "nodes", "pods",
	} {
		marker := "/" + resource
		position := strings.Index(path, marker)
		if position == -1 {
			continue
		}
		suffix := strings.TrimPrefix(path[position+len(marker):], "/")
		kind := resource
		if suffix != "" {
			parts := strings.Split(suffix, "/")
			kind += "/{name}"
			if len(parts) > 1 {
				kind += "/" + parts[1]
			}
		}
		if strings.Contains(rawQuery, "watch=true") {
			kind += "?watch=true"
		}
		return kind
	}
	return path
}

func TestPersistentAgentRealAPIScale(t *testing.T) {
	if os.Getenv("PERSISTENT_AGENT_REAL_API_SCALE") != "1" {
		t.Skip("set PERSISTENT_AGENT_REAL_API_SCALE=1 against a disposable cluster")
	}
	cliques := scaleEnvInt(t, "SCALE_CLIQUES", 1)
	members := scaleEnvInt(t, "SCALE_MEMBERS_PER_CLIQUE", 18)
	totalNodes := cliques * members
	timeout := time.Duration(scaleEnvInt(t, "SCALE_TIMEOUT_SECONDS", 300)) * time.Second
	runID := os.Getenv("SCALE_RUN_ID")
	if runID == "" {
		runID = fmt.Sprintf("pa-scale-%d", time.Now().UnixNano())
	}
	runID = strings.ToLower(runID)
	namespace := truncateScaleName(runID, 50)

	baseConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{},
	).ClientConfig()
	require.NoError(t, err)
	baseConfig.QPS = 1000
	baseConfig.Burst = 2000

	adminConfig := rest.CopyConfig(baseConfig)
	adminConfig.UserAgent = "persistent-agent-scale-fixture"
	adminCore, err := kubernetes.NewForConfig(adminConfig)
	require.NoError(t, err)
	adminNvidia, err := nvclientset.NewForConfig(adminConfig)
	require.NoError(t, err)

	transport := &realAPIScaleTransport{byKind: make(map[string]int64)}
	managerConfig := rest.CopyConfig(baseConfig)
	managerConfig.UserAgent = "persistent-agent-scale-manager"
	previousWrap := managerConfig.WrapTransport
	managerConfig.WrapTransport = func(roundTripper http.RoundTripper) http.RoundTripper {
		if previousWrap != nil {
			roundTripper = previousWrap(roundTripper)
		}
		return &scaleRoundTripper{next: roundTripper, stats: transport}
	}
	managerCore, err := kubernetes.NewForConfig(managerConfig)
	require.NoError(t, err)
	managerNvidia, err := nvclientset.NewForConfig(managerConfig)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	fixtureStarted := time.Now()
	fixture := newRealAPIScaleFixture(t, ctx, adminCore, adminNvidia, namespace, runID, cliques, members)
	fixtureSetup := time.Since(fixtureStarted)
	t.Cleanup(func() { cleanupRealAPIScaleFixture(context.Background(), adminCore, adminNvidia, fixture) })

	manager := NewPersistentAgentManager(&ManagerConfig{
		driverName: DriverName, driverNamespace: namespace, imageName: "registry.k8s.io/pause:3.10",
		maxNodesPerIMEXDomain: members,
		clientsets: flags.ClientSets{
			Core: managerCore, Resource: draclient.New(managerCore), Nvidia: managerNvidia,
		},
	})
	started := time.Now()
	require.NoError(t, manager.Start(ctx))
	stopped := false
	defer func() {
		if !stopped {
			manager.Stop()
		}
	}()

	require.NoError(t, wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		return realAPIScaleSnapshotsActive(ctx, adminCore, adminNvidia, fixture)
	}))
	startToActive := time.Since(started)
	require.NoError(t, applyRealAPIScaleReceipts(ctx, adminCore, adminNvidia, fixture, scaleEnvInt(t, "SCALE_FIXTURE_WORKERS", 32)))
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		return realAPIScaleComputeDomainReady(ctx, adminNvidia, fixture)
	}))
	startToReady := time.Since(started)
	formationTransport := transport.snapshot()

	steadyState := time.Duration(scaleEnvInt(t, "SCALE_STEADY_STATE_SECONDS", 2)) * time.Second
	if steadyState > 0 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(steadyState):
		}
	}

	metricsBody := scrapeRealAPIScaleMetrics(t)
	manager.Stop()
	stopped = true
	steadyTransport := transport.snapshot()
	result := summarizeRealAPIScale(t, steadyTransport, totalNodes, cliques, 1, fixtureSetup, startToActive, startToReady)
	require.Equal(t, countRealAPIControllerActions(formationTransport), countRealAPIControllerActions(steadyTransport), "steady-state window issued controller API actions")
	assertRealAPIScaleMetrics(t, metricsBody, result)

	artifactDir := os.Getenv("SCALE_ARTIFACTS")
	if artifactDir != "" {
		require.NoError(t, os.MkdirAll(artifactDir, 0o755))
		encoded, encodeErr := json.MarshalIndent(result, "", "  ")
		require.NoError(t, encodeErr)
		require.NoError(t, os.WriteFile(filepath.Join(artifactDir, "result.json"), append(encoded, '\n'), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(artifactDir, "controller-metrics.prom"), []byte(metricsBody), 0o644))
	}
	t.Logf("REAL_API_SCALE_RESULT nodes=%d cliques=%d fixture=%.3fs active=%.3fs ready=%.3fs clique_actions=%d clique_writes=%d total_actions=%d total_writes=%d request_body_bytes=%d response_body_bytes=%d watch_body_bytes=%d",
		result.Nodes, result.Cliques, result.FixtureSetupSeconds, result.StartToActiveSeconds, result.StartToReadySeconds, result.CliqueActions, result.CliqueWrites,
		result.TotalControllerActions, result.TotalControllerWrites,
		result.Transport.RequestBytes, result.Transport.ResponseBytes, result.Transport.WatchBytes)
}

type realAPIScaleFixture struct {
	namespace         string
	runID             string
	computeDomainUID  types.UID
	computeDomainName string
	cliqueIDs         []string
	nodeNames         []string
}

func newRealAPIScaleFixture(t *testing.T, ctx context.Context, core *kubernetes.Clientset, nvidia *nvclientset.Clientset, namespace, runID string, cliques, members int) *realAPIScaleFixture {
	t.Helper()
	_, err := core.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = core.CoreV1().ServiceAccounts(namespace).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: persistentAgentServiceAccountName},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	templateName := "scale-channel"
	cd, err := nvidia.ResourceV1beta1().ComputeDomains(namespace).Create(ctx, &nvapi.ComputeDomain{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scale-domain",
			Annotations: map[string]string{
				nvapi.ComputeDomainCliqueProtocolAnnotation: string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1),
			},
		},
		Spec: nvapi.ComputeDomainSpec{
			NumNodes: cliques * members,
			Channel: &nvapi.ComputeDomainChannelSpec{
				ResourceClaimTemplate: nvapi.ComputeDomainResourceClaimTemplate{Name: templateName},
				AllocationMode:        nvapi.ComputeDomainChannelAllocationModeSingle,
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	cd.Status.Status = nvapi.ComputeDomainStatusNotReady
	cd, err = nvidia.ResourceV1beta1().ComputeDomains(namespace).UpdateStatus(ctx, cd, metav1.UpdateOptions{})
	require.NoError(t, err)
	claimSpec, configBytes := realAPIScaleClaimSpec(t, cd.UID)
	_, err = core.ResourceV1().ResourceClaimTemplates(namespace).Create(ctx, &resourceapi.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: templateName,
			Labels: map[string]string{
				computeDomainLabelKey: string(cd.UID), computeDomainResourceClaimTemplateTargetLabelKey: computeDomainResourceClaimTemplateTargetWorkload,
			},
		},
		Spec: resourceapi.ResourceClaimTemplateSpec{Spec: claimSpec},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	ds, err := core.AppsV1().DaemonSets(namespace).Create(ctx, &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: persistentAgentDaemonSetName, Labels: map[string]string{persistentAgentLabelKey: "true"}},
		Spec: appsv1.DaemonSetSpec{
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{persistentAgentLabelKey: "true"}},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{persistentAgentLabelKey: "true"}},
				Spec: corev1.PodSpec{
					ServiceAccountName: persistentAgentServiceAccountName,
					NodeSelector:       map[string]string{"scale.resource.nvidia.com/run": runID},
					Tolerations:        []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
					Containers: []corev1.Container{{
						Name: "compute-domain-daemon", Image: "registry.k8s.io/pause:3.10",
						Command: []string{"compute-domain-daemon", "run", "--persistent-agent"},
					}},
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	fixture := &realAPIScaleFixture{
		namespace: namespace, runID: runID, computeDomainUID: cd.UID, computeDomainName: cd.Name,
		cliqueIDs: make([]string, cliques), nodeNames: make([]string, 0, cliques*members),
	}
	for clique := range cliques {
		fixture.cliqueIDs[clique] = truncateScaleName(fmt.Sprintf("%s-clique-%03d", runID, clique), 63)
		for member := range members {
			fixture.nodeNames = append(fixture.nodeNames, realAPIScaleNodeName(runID, clique, member))
		}
	}
	workers := scaleEnvInt(t, "SCALE_FIXTURE_WORKERS", 32)
	require.NoError(t, runRealAPIScaleParallel(ctx, len(fixture.nodeNames), workers, func(ordinal int) error {
		clique := ordinal / members
		member := ordinal % members
		return createRealAPIScaleNode(ctx, core, runID, ordinal, clique, member, fixture.cliqueIDs[clique], cd.UID)
	}))
	agentPods, err := prepareRealAPIScaleAgentPods(ctx, core, namespace, fixture.nodeNames, ds, workers)
	require.NoError(t, err)
	require.Len(t, agentPods, len(fixture.nodeNames))
	require.NoError(t, runRealAPIScaleParallel(ctx, len(fixture.nodeNames), workers, func(ordinal int) error {
		clique := ordinal / members
		member := ordinal % members
		suffix := fmt.Sprintf("%03d-%03d", clique, member)
		return createRealAPIScaleWorkload(ctx, core, namespace, suffix, fixture.nodeNames[ordinal], templateName, configBytes)
	}))
	return fixture
}

func realAPIScaleClaimSpec(t *testing.T, cdUID types.UID) (resourceapi.ResourceClaimSpec, []byte) {
	t.Helper()
	channel := nvapi.DefaultComputeDomainChannelConfig()
	channel.DomainID = string(cdUID)
	channel.AllocationMode = nvapi.ComputeDomainChannelAllocationModeSingle
	channel.Protocol = nvapi.ComputeDomainCliqueProtocolPersistentAgentV1
	configBytes, err := json.Marshal(channel)
	require.NoError(t, err)
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
	return claimSpec, configBytes
}

func realAPIScaleNodeName(runID string, clique, member int) string {
	return truncateScaleName(fmt.Sprintf("%s-node-%03d-%03d", runID, clique, member), 63)
}

func createRealAPIScaleNode(ctx context.Context, client *kubernetes.Clientset, runID string, ordinal, clique, member int, cliqueID string, cdUID types.UID) error {
	nodeName := realAPIScaleNodeName(runID, clique, member)
	podCIDR := fmt.Sprintf("10.%d.%d.0/24", 1+(ordinal/256)%254, ordinal%256)
	_, err := client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: nodeName,
		Labels: map[string]string{
			gpuCliqueNodeLabelKey: cliqueID, persistentAgentIsolationLabelKey: string(cdUID), "scale.resource.nvidia.com/run": runID,
		},
		Annotations: map[string]string{
			computeDomainCliqueStartupAnnotationKey:    cliqueID,
			computeDomainCliqueCapabilityAnnotationKey: string(nvapi.ComputeDomainCliqueProtocolPersistentAgentV1),
		},
	}, Spec: corev1.NodeSpec{PodCIDR: podCIDR, PodCIDRs: []string{podCIDR}}}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	suffix := fmt.Sprintf("%03d-%03d", clique, member)
	return wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		current, getErr := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if getErr != nil {
			return false, getErr
		}
		current.Status.NodeInfo.BootID = "boot-" + suffix
		current.Status.Capacity = corev1.ResourceList{corev1.ResourcePods: resource.MustParse("10")}
		current.Status.Allocatable = corev1.ResourceList{corev1.ResourcePods: resource.MustParse("10")}
		current.Status.Conditions = []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now(),
		}}
		_, updateErr := client.CoreV1().Nodes().UpdateStatus(ctx, current, metav1.UpdateOptions{})
		if apierrors.IsConflict(updateErr) {
			return false, nil
		}
		return updateErr == nil, updateErr
	})
}

type realAPIScaleAgentPod struct {
	name     string
	nodeName string
	ordinal  int
}

func prepareRealAPIScaleAgentPods(ctx context.Context, client *kubernetes.Clientset, namespace string, nodeNames []string, ds *appsv1.DaemonSet, workers int) ([]realAPIScaleAgentPod, error) {
	nodeOrdinals := make(map[string]int, len(nodeNames))
	for ordinal, nodeName := range nodeNames {
		nodeOrdinals[nodeName] = ordinal
	}
	agentPods := make([]realAPIScaleAgentPod, len(nodeNames))
	err := wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: persistentAgentLabelKey + "=true"})
		if err != nil {
			return false, err
		}
		next := make([]realAPIScaleAgentPod, len(nodeNames))
		found := 0
		for i := range pods.Items {
			candidate := &pods.Items[i]
			if !metav1.IsControlledBy(candidate, ds) {
				continue
			}
			nodeName := realAPIScalePodTargetNode(candidate)
			ordinal, exists := nodeOrdinals[nodeName]
			if !exists || next[ordinal].name != "" {
				continue
			}
			next[ordinal] = realAPIScaleAgentPod{name: candidate.Name, nodeName: nodeName, ordinal: ordinal}
			found++
		}
		agentPods = next
		return found == len(nodeNames), nil
	})
	if err != nil {
		return nil, err
	}
	err = runRealAPIScaleParallel(ctx, len(agentPods), workers, func(ordinal int) error {
		pod := agentPods[ordinal]
		current, getErr := client.CoreV1().Pods(namespace).Get(ctx, pod.name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if current.Spec.NodeName == "" {
			bindErr := client.CoreV1().Pods(namespace).Bind(ctx, &corev1.Binding{
				ObjectMeta: metav1.ObjectMeta{Name: current.Name},
				Target:     corev1.ObjectReference{Kind: "Node", Name: pod.nodeName},
			}, metav1.CreateOptions{})
			if bindErr != nil && !apierrors.IsConflict(bindErr) {
				return bindErr
			}
		}
		return wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			current, getErr := client.CoreV1().Pods(namespace).Get(ctx, pod.name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}
			if current.Spec.NodeName != pod.nodeName {
				return false, nil
			}
			current.Status.Phase = corev1.PodRunning
			current.Status.PodIP = fmt.Sprintf("10.%d.%d.%d", 1+(ordinal/65025)%254, (ordinal/255)%255, 1+ordinal%254)
			_, updateErr := client.CoreV1().Pods(namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{})
			if apierrors.IsConflict(updateErr) {
				return false, nil
			}
			return updateErr == nil, updateErr
		})
	})
	return agentPods, err
}

func realAPIScalePodTargetNode(pod *corev1.Pod) string {
	if pod.Spec.NodeName != "" {
		return pod.Spec.NodeName
	}
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, field := range term.MatchFields {
			if field.Key == metav1.ObjectNameField && field.Operator == corev1.NodeSelectorOpIn && len(field.Values) == 1 {
				return field.Values[0]
			}
		}
	}
	return ""
}

func createRealAPIScaleWorkload(ctx context.Context, client *kubernetes.Clientset, namespace, suffix, nodeName, templateName string, configBytes []byte) error {
	pod, err := client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "workload-" + suffix, Labels: map[string]string{"scale.resource.nvidia.com/workload": "true"}},
		Spec: corev1.PodSpec{
			NodeName: nodeName, RestartPolicy: corev1.RestartPolicyNever,
			Containers:     []corev1.Container{{Name: "workload", Image: "registry.k8s.io/pause:3.10"}},
			ResourceClaims: []corev1.PodResourceClaim{{Name: "channel", ResourceClaimTemplateName: &templateName}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	pod, claimName, err := scheduleRealAPIScalePod(ctx, client, pod.Namespace, pod.Name)
	if err != nil {
		return err
	}
	var claim *resourceapi.ResourceClaim
	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		var getErr error
		claim, getErr = client.ResourceV1().ResourceClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return false, nil
		}
		return getErr == nil, getErr
	})
	if err != nil {
		return err
	}
	claim.Status = resourceapi.ResourceClaimStatus{
		ReservedFor: []resourceapi.ResourceClaimConsumerReference{{Resource: "pods", Name: pod.Name, UID: pod.UID}},
		Allocation: &resourceapi.AllocationResult{
			NodeSelector: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchFields: []corev1.NodeSelectorRequirement{{Key: metav1.ObjectNameField, Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName}}},
			}}},
			Devices: resourceapi.DeviceAllocationResult{
				Results: []resourceapi.DeviceRequestAllocationResult{{Request: "channel", Driver: DriverName, Pool: nodeName, Device: "channel-0"}},
				Config: []resourceapi.DeviceAllocationConfiguration{{
					Source: resourceapi.AllocationConfigSourceClaim, Requests: []string{"channel"},
					DeviceConfiguration: resourceapi.DeviceConfiguration{Opaque: &resourceapi.OpaqueDeviceConfiguration{
						Driver: DriverName, Parameters: runtime.RawExtension{Raw: configBytes},
					}},
				}},
			},
		},
	}
	_, err = client.ResourceV1().ResourceClaims(namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
	return err
}

func scheduleRealAPIScalePod(ctx context.Context, client *kubernetes.Clientset, namespace, name string) (*corev1.Pod, string, error) {
	var updated *corev1.Pod
	var claimName string
	err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		claimName = generatedClaimName(pod, "channel")
		if claimName == "" {
			return false, nil
		}
		pod.Status.Phase = corev1.PodPending
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
		updated, err = client.CoreV1().Pods(namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			return false, nil
		}
		return err == nil, err
	})
	return updated, claimName, err
}

func runRealAPIScaleParallel(ctx context.Context, count, workers int, work func(int) error) error {
	if workers > count {
		workers = count
	}
	jobs := make(chan int)
	errors := make(chan error, 1)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for ordinal := range jobs {
				if err := work(ordinal); err != nil {
					select {
					case errors <- err:
					default:
					}
					return
				}
			}
		}()
	}
	for ordinal := range count {
		select {
		case <-ctx.Done():
			close(jobs)
			group.Wait()
			return ctx.Err()
		case err := <-errors:
			close(jobs)
			group.Wait()
			return err
		case jobs <- ordinal:
		}
	}
	close(jobs)
	group.Wait()
	select {
	case err := <-errors:
		return err
	default:
		return nil
	}
}

func realAPIScaleSnapshotsActive(ctx context.Context, core *kubernetes.Clientset, nvidia *nvclientset.Clientset, fixture *realAPIScaleFixture) (bool, error) {
	for _, nodeName := range fixture.nodeNames {
		node, err := core.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if node.Labels[computeDomainLabelKey] != string(fixture.computeDomainUID) || node.Annotations[computeDomainAttestationAnnotationKey] == "" {
			return false, nil
		}
	}
	for _, cliqueID := range fixture.cliqueIDs {
		reservation, err := nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(ctx, cdclique.ReservationName(cliqueID), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if reservation.Status.Phase != nvapi.ComputeDomainCliqueReservationPhaseActive {
			return false, nil
		}
		snapshot, err := nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(fixture.namespace).Get(ctx, cdclique.SnapshotName(string(fixture.computeDomainUID), cliqueID), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseActive {
			return false, nil
		}
	}
	return true, nil
}

func applyRealAPIScaleReceipts(ctx context.Context, core *kubernetes.Clientset, nvidia *nvclientset.Clientset, fixture *realAPIScaleFixture, workers int) error {
	type receiptUpdate struct {
		podName string
		value   string
	}
	updates := make([]receiptUpdate, 0, len(fixture.nodeNames))
	for _, cliqueID := range fixture.cliqueIDs {
		snapshot, err := nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(fixture.namespace).Get(ctx, cdclique.SnapshotName(string(fixture.computeDomainUID), cliqueID), metav1.GetOptions{})
		if err != nil {
			return err
		}
		for i := range snapshot.Status.Members {
			member := &snapshot.Status.Members[i]
			receipt, err := json.Marshal(nvapi.ComputeDomainCliqueSnapshotReceipt{
				SnapshotUID: snapshot.UID, SnapshotGeneration: snapshot.Status.Generation, SnapshotHash: snapshot.Status.Hash,
				NodeUID: member.NodeUID, PodUID: member.PodUID, Index: member.Index,
			})
			if err != nil {
				return err
			}
			updates = append(updates, receiptUpdate{podName: member.PodName, value: string(receipt)})
		}
	}
	return runRealAPIScaleParallel(ctx, len(updates), workers, func(ordinal int) error {
		return wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			pod, err := core.CoreV1().Pods(fixture.namespace).Get(ctx, updates[ordinal].podName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			pod.Annotations[nvapi.ComputeDomainCliqueSnapshotAppliedAnnotation] = updates[ordinal].value
			_, err = core.CoreV1().Pods(fixture.namespace).Update(ctx, pod, metav1.UpdateOptions{})
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return err == nil, err
		})
	})
}

func realAPIScaleComputeDomainReady(ctx context.Context, nvidia *nvclientset.Clientset, fixture *realAPIScaleFixture) (bool, error) {
	computeDomain, err := nvidia.ResourceV1beta1().ComputeDomains(fixture.namespace).Get(ctx, fixture.computeDomainName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if computeDomain.Status.Status != nvapi.ComputeDomainStatusReady {
		return false, nil
	}
	return true, nil
}

func summarizeRealAPIScale(t *testing.T, transport realAPIScaleTransportSnapshot, nodes, cliques, computeDomains int, fixtureSetup, startToActive, startToReady time.Duration) realAPIScaleResult {
	t.Helper()
	var cliqueActions, cliqueWrites, computeDomainActions, computeDomainWrites, conflicts, throttled int64
	for kind, count := range transport.ByKind {
		fields := strings.Fields(kind)
		if len(fields) != 3 {
			continue
		}
		method, resource := fields[0], fields[1]
		status, err := strconv.Atoi(fields[2])
		require.NoError(t, err)
		isMutation := method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete
		isCliqueResource := strings.HasPrefix(resource, "nodes/{name}") || strings.HasPrefix(resource, "computedomaincliquereservations") || strings.HasPrefix(resource, "computedomaincliquesnapshots")
		isReservationGet := method == http.MethodGet && strings.HasPrefix(resource, "computedomaincliquereservations/{name}")
		if isCliqueResource && (isMutation || isReservationGet) {
			cliqueActions += count
		}
		if isCliqueResource && isMutation && status >= 200 && status < 300 {
			cliqueWrites += count
		}
		isComputeDomainGet := method == http.MethodGet && strings.HasPrefix(resource, "computedomains/{name}")
		isComputeDomainStatusWrite := method == http.MethodPut && strings.HasPrefix(resource, "computedomains/{name}/status")
		if isComputeDomainGet || isComputeDomainStatusWrite {
			computeDomainActions += count
		}
		if isComputeDomainStatusWrite && status >= 200 && status < 300 {
			computeDomainWrites += count
		}
		if status == http.StatusConflict {
			conflicts += count
		}
		if status == http.StatusTooManyRequests {
			throttled += count
		}
	}
	require.Equal(t, int64(nodes+6*cliques), cliqueActions, transport.ByKind)
	require.Equal(t, int64(nodes+5*cliques), cliqueWrites, transport.ByKind)
	require.Equal(t, int64(2*computeDomains), computeDomainActions, transport.ByKind)
	require.Equal(t, int64(computeDomains), computeDomainWrites, transport.ByKind)
	require.Zero(t, conflicts, transport.ByKind)
	require.Zero(t, throttled, transport.ByKind)
	return realAPIScaleResult{
		Nodes: nodes, Cliques: cliques, ComputeDomains: computeDomains,
		FixtureSetupSeconds: fixtureSetup.Seconds(), StartToActiveSeconds: startToActive.Seconds(), StartToReadySeconds: startToReady.Seconds(),
		CliqueActions: cliqueActions, CliqueWrites: cliqueWrites,
		ComputeDomainActions: computeDomainActions, ComputeDomainWrites: computeDomainWrites,
		TotalControllerActions: cliqueActions + computeDomainActions,
		TotalControllerWrites:  cliqueWrites + computeDomainWrites,
		Conflicts:              conflicts, Throttled: throttled,
		Transport: transport,
	}
}

func countRealAPIControllerActions(transport realAPIScaleTransportSnapshot) int64 {
	var actions int64
	for kind, count := range transport.ByKind {
		fields := strings.Fields(kind)
		if len(fields) != 3 {
			continue
		}
		method, resource := fields[0], fields[1]
		isMutation := method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete
		isCliqueResource := strings.HasPrefix(resource, "nodes/{name}") || strings.HasPrefix(resource, "computedomaincliquereservations") || strings.HasPrefix(resource, "computedomaincliquesnapshots")
		isReservationGet := method == http.MethodGet && strings.HasPrefix(resource, "computedomaincliquereservations/{name}")
		isComputeDomainGet := method == http.MethodGet && strings.HasPrefix(resource, "computedomains/{name}")
		isComputeDomainStatusWrite := method == http.MethodPut && strings.HasPrefix(resource, "computedomains/{name}/status")
		if (isCliqueResource && (isMutation || isReservationGet)) || isComputeDomainGet || isComputeDomainStatusWrite {
			actions += count
		}
	}
	return actions
}

func assertRealAPIScaleMetrics(t *testing.T, body string, result realAPIScaleResult) {
	t.Helper()
	for _, expected := range []string{
		fmt.Sprintf(`nvidia_dra_cdc_api_writes_total{operation="attestation_update",protocol="persistent-agent-v1",resource="node"} %d`, result.Nodes),
		fmt.Sprintf(`nvidia_dra_cdc_api_writes_total{operation="create",protocol="persistent-agent-v1",resource="reservation"} %d`, result.Cliques),
		fmt.Sprintf(`nvidia_dra_cdc_api_writes_total{operation="create",protocol="persistent-agent-v1",resource="snapshot"} %d`, result.Cliques),
		fmt.Sprintf(`nvidia_dra_cdc_api_writes_total{operation="finalizer_add",protocol="persistent-agent-v1",resource="snapshot"} %d`, result.Cliques),
		fmt.Sprintf(`nvidia_dra_cdc_api_writes_total{operation="status_update",protocol="persistent-agent-v1",resource="reservation"} %d`, result.Cliques),
		fmt.Sprintf(`nvidia_dra_cdc_api_writes_total{operation="status_update",protocol="persistent-agent-v1",resource="snapshot"} %d`, result.Cliques),
		fmt.Sprintf(`nvidia_dra_cdc_api_writes_total{operation="status_update",protocol="persistent-agent-v1",resource="compute_domain"} %d`, result.ComputeDomainWrites),
		fmt.Sprintf(`nvidia_dra_cdc_api_actions_total{operation="get",protocol="persistent-agent-v1",resource="reservation",result="success"} %d`, result.Cliques),
		fmt.Sprintf(`nvidia_dra_cdc_api_actions_total{operation="get",protocol="persistent-agent-v1",resource="compute_domain",result="success"} %d`, result.ComputeDomains),
	} {
		require.Contains(t, body, expected)
	}
}

func scrapeRealAPIScaleMetrics(t *testing.T) string {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	drivermetrics.NewLegacyPrometheusHandler().ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)
	return recorder.Body.String()
}

func cleanupRealAPIScaleFixture(ctx context.Context, core *kubernetes.Clientset, nvidia *nvclientset.Clientset, fixture *realAPIScaleFixture) {
	for _, cliqueID := range fixture.cliqueIDs {
		name := cdclique.SnapshotName(string(fixture.computeDomainUID), cliqueID)
		if snapshot, err := nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(fixture.namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
			snapshot.Finalizers = nil
			_, _ = nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(fixture.namespace).Update(ctx, snapshot, metav1.UpdateOptions{})
			_ = nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(fixture.namespace).Delete(ctx, name, metav1.DeleteOptions{})
		}
		_ = nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Delete(ctx, cdclique.ReservationName(cliqueID), metav1.DeleteOptions{})
	}
	for _, nodeName := range fixture.nodeNames {
		_ = core.CoreV1().Nodes().Delete(ctx, nodeName, metav1.DeleteOptions{})
	}
	_ = core.CoreV1().Namespaces().Delete(ctx, fixture.namespace, metav1.DeleteOptions{})
}

func scaleEnvInt(t *testing.T, name string, defaultValue int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	require.NoError(t, err)
	require.Positive(t, parsed)
	return parsed
}

func truncateScaleName(value string, limit int) string {
	value = strings.Trim(value, "-")
	if len(value) <= limit {
		return value
	}
	return strings.Trim(value[:limit], "-")
}
