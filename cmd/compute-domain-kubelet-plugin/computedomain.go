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
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	cdiapi "tags.cncf.io/container-device-interface/pkg/cdi"
	cdispec "tags.cncf.io/container-device-interface/specs-go"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/internal/common"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/featuregates"
	nvinformers "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/informers/externalversions"
	nvlisters "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/nvidia.com/listers/resource/v1beta1"
)

const (
	computeDomainLabelKey                      = "resource.nvidia.com/computeDomain"
	computeDomainAttestationAnnotationKey      = "resource.nvidia.com/computeDomainAttestation"
	controllerOwnedCliqueIsolationLabelKey     = "resource.nvidia.com/controllerOwnedComputeDomain"
	computeDomainCliqueLabelKey                = "resource.nvidia.com/computeDomain.cliqueID"
	computeDomainCliqueStartupAnnotationKey    = "resource.nvidia.com/computeDomainCliqueStartupID"
	computeDomainCliqueCapabilityAnnotationKey = "resource.nvidia.com/computeDomainCliqueProtocolCapability"

	// gpuCliqueLabelKey sets the node label historically set by
	// gpu-feature-discovery (see
	// https://github.com/NVIDIA/k8s-device-plugin/blob/main/docs/gpu-feature-discovery/README.md#generated-labels).
	// gpu-feature-discovery that is bundled with the k8s-device-plugin is being deprecated,
	// so the kubelet plugin now owns setting it on systems where the DRA driver is enabled.
	gpuCliqueLabelKey = "nvidia.com/gpu.clique"

	gpuCliqueLabelRefreshInterval = 10 * time.Minute

	informerResyncPeriod     = 10 * time.Minute
	cleanupInterval          = 10 * time.Minute
	topologyRecoveryInterval = 5 * time.Second

	ComputeDomainDaemonConfigFilesDirName = "domains"
	ComputeDomainDaemonConfigTemplatePath = "/templates/compute-domain-daemon-config.tmpl.cfg"
)

type ComputeDomainManager struct {
	config        *Config
	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc

	computeDomainFactory nvinformers.SharedInformerFactory
	informer             cache.SharedIndexInformer
	snapshotFactory      nvinformers.SharedInformerFactory
	snapshotInformer     cache.SharedIndexInformer
	snapshotLister       nvlisters.ComputeDomainCliqueSnapshotLister
	snapshotAPIAvailable bool
	controllerReadersOn  bool
	podFactory           informers.SharedInformerFactory
	podInformer          cache.SharedIndexInformer
	podLister            corev1listers.PodLister
	nodeFactory          informers.SharedInformerFactory
	nodeInformer         cache.SharedIndexInformer
	nodeLister           corev1listers.NodeLister

	configFilesRoot string

	cliqueIDMu      sync.RWMutex
	cliqueID        string
	topologyInvalid bool

	getCliqueIDFunc        func() (string, error)
	controllerOwnedEnabled func() bool
}

type ComputeDomainDaemonSettings struct {
	manager         *ComputeDomainManager
	domainID        string
	rootDir         string
	configTmplPath  string
	nodesConfigPath string
	recoveryOnly    bool
}

func NewComputeDomainManager(config *Config, getCliqueIDFunc func() (string, error)) (*ComputeDomainManager, error) {
	cliqueID, err := getCliqueIDFunc()
	if err != nil {
		return nil, fmt.Errorf("error getting cliqueID: %w", err)
	}
	computeDomainFactory := nvinformers.NewSharedInformerFactory(config.clientsets.Nvidia, informerResyncPeriod)
	informer := computeDomainFactory.Resource().V1beta1().ComputeDomains().Informer()
	snapshotFactory := nvinformers.NewSharedInformerFactoryWithOptions(
		config.clientsets.Nvidia,
		informerResyncPeriod,
		nvinformers.WithNamespace(config.flags.namespace),
		nvinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			// A node needs only snapshots for its immutable startup hardware
			// clique. Avoid broadcasting every full snapshot in the driver
			// namespace to every kubelet plugin in the cluster.
			options.LabelSelector = labels.SelectorFromSet(labels.Set{computeDomainCliqueLabelKey: cliqueID}).String()
		}),
	)
	snapshotInformer := snapshotFactory.Resource().V1beta1().ComputeDomainCliqueSnapshots().Informer()
	podFactory := informers.NewSharedInformerFactoryWithOptions(
		config.clientsets.Core,
		informerResyncPeriod,
		informers.WithNamespace(config.flags.namespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = computeDomainLabelKey
			options.FieldSelector = "spec.nodeName=" + config.flags.nodeName
		}),
	)
	nodeFactory := informers.NewSharedInformerFactoryWithOptions(
		config.clientsets.Core,
		informerResyncPeriod,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = "metadata.name=" + config.flags.nodeName
		}),
	)
	configFilesRoot := filepath.Join(config.DriverPluginPath(), ComputeDomainDaemonConfigFilesDirName)

	m := &ComputeDomainManager{
		config:               config,
		computeDomainFactory: computeDomainFactory,
		informer:             informer,
		snapshotFactory:      snapshotFactory,
		snapshotInformer:     snapshotInformer,
		snapshotLister:       snapshotFactory.Resource().V1beta1().ComputeDomainCliqueSnapshots().Lister(),
		podFactory:           podFactory,
		podInformer:          podFactory.Core().V1().Pods().Informer(),
		podLister:            podFactory.Core().V1().Pods().Lister(),
		nodeFactory:          nodeFactory,
		nodeInformer:         nodeFactory.Core().V1().Nodes().Informer(),
		nodeLister:           nodeFactory.Core().V1().Nodes().Lister(),
		configFilesRoot:      configFilesRoot,
		cliqueID:             cliqueID,
		getCliqueIDFunc:      getCliqueIDFunc,
		controllerOwnedEnabled: func() bool {
			return featuregates.Enabled(featuregates.ControllerOwnedCDCliques)
		},
	}

	return m, nil
}

// CliqueID returns the most recently known GPU clique ID. Safe for
// concurrent use.
func (m *ComputeDomainManager) CliqueID() string {
	m.cliqueIDMu.RLock()
	defer m.cliqueIDMu.RUnlock()
	return m.cliqueID
}

// setCliqueID stores a newly observed GPU clique ID.
func (m *ComputeDomainManager) setCliqueID(cliqueID string) {
	m.cliqueIDMu.Lock()
	defer m.cliqueIDMu.Unlock()
	m.cliqueID = cliqueID
}

func (m *ComputeDomainManager) assertTopologyValid() error {
	m.cliqueIDMu.RLock()
	defer m.cliqueIDMu.RUnlock()
	if m.topologyInvalid {
		return fmt.Errorf("GPU clique topology changed after plugin startup; drain and fence this node before preparing more ComputeDomain resources")
	}
	return nil
}

func (m *ComputeDomainManager) topologyIsInvalid() bool {
	m.cliqueIDMu.RLock()
	defer m.cliqueIDMu.RUnlock()
	return m.topologyInvalid
}

func (m *ComputeDomainManager) retirementRecoveryAllowed(cd *nvapi.ComputeDomain, protocol nvapi.ComputeDomainCliqueProtocol) bool {
	return cd != nil && cd.DeletionTimestamp != nil &&
		nvapi.EffectiveComputeDomainCliqueProtocol(protocol) == nvapi.ComputeDomainCliqueProtocolControllerV1 &&
		m.topologyIsInvalid()
}

func (m *ComputeDomainManager) Start(ctx context.Context) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel

	defer func() {
		if rerr != nil {
			if err := m.Stop(); err != nil {
				klog.Errorf("error stopping ComputeDomainManager: %v", err)
			}
		}
	}()

	err := m.informer.AddIndexers(cache.Indexers{
		"computeDomainUID": uidIndexer[*nvapi.ComputeDomain],
	})
	if err != nil {
		return fmt.Errorf("error adding indexer for UIDs: %w", err)
	}

	m.computeDomainFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), m.informer.HasSynced) {
		return fmt.Errorf("ComputeDomain informer cache sync failed")
	}

	// A legacy-only node does not need three additional Pod, Node, and full
	// snapshot LIST/WATCH streams. Start controller-v1 readers only when the
	// gate admits new controller-v1 domains or the cache proves that persisted
	// controller-v1 state must keep working after the gate is disabled.
	m.controllerReadersOn = m.controllerOwnedEnabled() || m.hasPersistedControllerV1()
	if m.controllerReadersOn {
		m.snapshotAPIAvailable, err = m.probeSnapshotAPI(ctx)
		if err != nil {
			return err
		}
		m.podFactory.Start(ctx.Done())
		m.nodeFactory.Start(ctx.Done())
		if m.snapshotAPIAvailable {
			m.snapshotFactory.Start(ctx.Done())
		}
	}

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.periodicCleanup(ctx)
	}()

	if m.controllerReadersOn {
		syncs := []cache.InformerSynced{m.podInformer.HasSynced, m.nodeInformer.HasSynced}
		if m.snapshotAPIAvailable {
			syncs = append(syncs, m.snapshotInformer.HasSynced)
		}
		if !cache.WaitForCacheSync(ctx.Done(), syncs...) {
			return fmt.Errorf("controller-v1 readiness informer cache sync failed")
		}
	}

	if m.config.flags.gpuCliqueLabelEnabled {
		if err := m.SetGPUCliqueLabel(ctx); err != nil {
			if !m.controllerReadersOn || !m.topologyIsInvalid() {
				return fmt.Errorf("error setting %s node label: %w", gpuCliqueLabelKey, err)
			}
			klog.Errorf("starting in retirement-recovery-only mode after topology validation failed: %v", err)
		}
	}

	if m.getCliqueIDFunc != nil {
		m.waitGroup.Add(1)
		go func() {
			defer m.waitGroup.Done()
			m.periodicGPUCliqueIDRefresh(ctx)
		}()
		m.waitGroup.Add(1)
		go func() {
			defer m.waitGroup.Done()
			m.periodicTopologyRecovery(ctx)
		}()
	}

	return nil
}

func (m *ComputeDomainManager) hasPersistedControllerV1() bool {
	for _, object := range m.informer.GetStore().List() {
		cd, ok := object.(*nvapi.ComputeDomain)
		if ok && nvapi.EffectiveComputeDomainCliqueProtocol(nvapi.ComputeDomainCliqueProtocol(cd.Annotations[nvapi.ComputeDomainCliqueProtocolAnnotation])) == nvapi.ComputeDomainCliqueProtocolControllerV1 {
			return true
		}
	}
	return false
}

// probeSnapshotAPI lets legacy-only installations start before the snapshot
// CRD is installed. It intentionally does not consult the feature gate: a
// persisted controller-v1 ComputeDomain remains controller-v1 across later
// changes to the process-wide default.
func (m *ComputeDomainManager) probeSnapshotAPI(ctx context.Context) (bool, error) {
	_, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueSnapshots(m.config.flags.namespace).List(ctx, metav1.ListOptions{Limit: 1})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("probe ComputeDomainCliqueSnapshot API: %w", err)
	}
	return true, nil
}

//nolint:contextcheck
func (m *ComputeDomainManager) Stop() error {
	if m.cancelContext != nil {
		m.cancelContext()
	}
	m.waitGroup.Wait()
	// Preserve the last verified topology label across an ordinary plugin
	// rollout. A transient removal can delete the daemon Pod and quarantine an
	// already-published controller-v1 slot. Validation paths still remove it
	// when the actual hardware identity changes or becomes unverifiable.
	return nil
}

func (m *ComputeDomainManager) NewSettings(domainID string) *ComputeDomainDaemonSettings {
	return &ComputeDomainDaemonSettings{
		manager:         m,
		domainID:        domainID,
		rootDir:         fmt.Sprintf("%s/%s", m.configFilesRoot, domainID),
		configTmplPath:  fmt.Sprintf("%s/%s/%s", m.configFilesRoot, domainID, "imexd.cfg.tmpl"),
		nodesConfigPath: fmt.Sprintf("%s/%s/%s", m.configFilesRoot, domainID, "nodes.cfg"),
	}
}

func (m *ComputeDomainManager) GetComputeDomainChannelContainerEdits(devRoot string, info *common.NVcapDeviceInfo) *cdiapi.ContainerEdits {
	return &cdiapi.ContainerEdits{
		ContainerEdits: &cdispec.ContainerEdits{
			DeviceNodes: []*cdispec.DeviceNode{info.CDICharDevNode()},
		},
	}
}

// GetCDIContainerEditsCommon() returns the CDI spec edits always required for
// launching the CD Daemon (whether or not it tries to launch an IMEX daemon
// internally).
func (s *ComputeDomainDaemonSettings) GetCDIContainerEditsCommon(ctx context.Context) (*cdiapi.ContainerEdits, error) {
	cd, err := s.manager.GetComputeDomain(ctx, s.domainID)
	if err != nil {
		return nil, fmt.Errorf("error getting compute domain %s: %w", s.domainID, err)
	}
	if cd == nil {
		return nil, fmt.Errorf("compute domain not found: %s", s.domainID)
	}

	cliqueID := s.manager.CliqueID()
	if s.recoveryOnly {
		// A recovery daemon must not join either the old or newly discovered
		// fabric. The empty scope starts only the retirement snapshot reader.
		cliqueID = ""
	}
	edits := &cdiapi.ContainerEdits{
		ContainerEdits: &cdispec.ContainerEdits{
			Env: []string{
				fmt.Sprintf("CLIQUE_ID=%s", cliqueID),
				fmt.Sprintf("COMPUTE_DOMAIN_UUID=%s", cd.UID),
				fmt.Sprintf("COMPUTE_DOMAIN_NAME=%s", cd.Name),
				fmt.Sprintf("COMPUTE_DOMAIN_NAMESPACE=%s", cd.Namespace),
			},
			Mounts: []*cdispec.Mount{
				{
					// imexDaemonConfigDirPath   = "/imexd"
					ContainerPath: "/imexd",
					HostPath:      s.rootDir,
					Options:       []string{"rw", "nosuid", "nodev", "bind"},
				},
			},
		},
	}
	return edits, nil
}

func (s *ComputeDomainDaemonSettings) GetDomainID() string {
	return s.domainID
}

// GetCDIContainerEditsForImex() returns the CDI spec edits only required for
// launching the CD daemon when it actually wraps an IMEX daemon.
func (s *ComputeDomainDaemonSettings) GetCDIContainerEditsForImex(ctx context.Context, devRoot string, info *common.NVcapDeviceInfo) *cdiapi.ContainerEdits {
	edits := &cdiapi.ContainerEdits{
		ContainerEdits: &cdispec.ContainerEdits{
			DeviceNodes: []*cdispec.DeviceNode{info.CDICharDevNode()},
		},
	}
	return edits
}

func (s *ComputeDomainDaemonSettings) Prepare(ctx context.Context) error {
	if err := os.MkdirAll(s.rootDir, 0755); err != nil {
		return fmt.Errorf("error creating directory %v: %w", s.rootDir, err)
	}

	if err := s.WriteConfigFile(ctx); err != nil {
		return fmt.Errorf("error writing config file %v: %w", s.configTmplPath, err)
	}

	return nil
}

func (s *ComputeDomainDaemonSettings) Unprepare(ctx context.Context) error {
	// TODO: Only actually remove this directory once the ComputeDomain has
	// been deleted. There is a (rare) chance when a pod gets force deleted
	// and a new pod associated with the same compute domain gets started
	// that deleting this here will occur *after* the creation from the new
	// pod, rendering this directory invalid when trying to be used by the
	// new pod. For now, just wait for the cleanup loop to take care of
	// this, but in the future let's do this cleanup on ComputeDomain
	// deletion (in addition to the cleanup loop).
	// err := os.RemoveAll(s.rootDir); err != nil {
	//	return fmt.Errorf("error removing directory %v: %w", s.rootDir, err)
	//}
	return nil
}

func (s *ComputeDomainDaemonSettings) WriteConfigFile(ctx context.Context) error {
	configBytes, err := os.ReadFile(ComputeDomainDaemonConfigTemplatePath)
	if err != nil {
		return fmt.Errorf("error reading template file: %w", err)
	}

	if err := os.WriteFile(s.configTmplPath, configBytes, 0644); err != nil {
		return fmt.Errorf("error writing config file %v: %w", s.configTmplPath, err)
	}

	return nil
}

func (m *ComputeDomainManager) AssertComputeDomainReady(ctx context.Context, cdUID string, protocol nvapi.ComputeDomainCliqueProtocol) error {
	if err := m.assertTopologyValid(); err != nil {
		return err
	}
	cd, err := m.GetComputeDomain(ctx, cdUID)
	if err != nil {
		return fmt.Errorf("error getting ComputeDomain: %w", err)
	}
	if cd == nil {
		return fmt.Errorf("ComputeDomain not found: %s", cdUID)
	}

	if err := nvapi.ValidateComputeDomainCliqueProtocol(protocol); err != nil {
		return err
	}
	protocol = nvapi.EffectiveComputeDomainCliqueProtocol(protocol)
	persistedProtocol := nvapi.ComputeDomainCliqueProtocol(cd.Annotations[nvapi.ComputeDomainCliqueProtocolAnnotation])
	if err := nvapi.ValidateComputeDomainCliqueProtocol(persistedProtocol); err != nil {
		return fmt.Errorf("invalid persisted ComputeDomain clique protocol: %w", err)
	}
	persistedProtocol = nvapi.EffectiveComputeDomainCliqueProtocol(persistedProtocol)
	if protocol != persistedProtocol {
		return fmt.Errorf("claim clique protocol %q does not match ComputeDomain protocol %q", protocol, persistedProtocol)
	}
	if protocol == nvapi.ComputeDomainCliqueProtocolControllerV1 {
		return m.assertCurrentNodeReadyInSnapshot(ctx, cd)
	}

	// Marker-less and explicit legacy-v1 claims retain the compatibility path.
	if !m.isCurrentNodeReady(ctx, cd) {
		return fmt.Errorf("current node not ready in ComputeDomain")
	}

	return nil
}

func (m *ComputeDomainManager) assertCurrentNodeReadyInSnapshot(ctx context.Context, cd *nvapi.ComputeDomain) error {
	// This quorum-backed read pairs with the controller's live workload-Pod
	// inventory to close the destructive retirement barrier. If Prepare wins
	// before deletion, its Pod already exists and the controller inventory sees
	// it. If deletion wins first, this read observes the deletion timestamp and
	// refuses release. Informer freshness alone cannot establish that ordering.
	live, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomains(cd.Namespace).Get(ctx, cd.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("live-read controller-owned ComputeDomain before release: %w", err)
	}
	if live.UID != cd.UID {
		return fmt.Errorf("controller-owned ComputeDomain identity changed before release")
	}
	if live.DeletionTimestamp != nil {
		return fmt.Errorf("controller-owned ComputeDomain is deleting")
	}
	cliqueID := m.CliqueID()
	if cliqueID == "" {
		return fmt.Errorf("controller-owned readiness requires a non-empty GPU clique ID")
	}
	if !m.snapshotAPIAvailable {
		return fmt.Errorf("ComputeDomainCliqueSnapshot API is unavailable")
	}
	digest := sha256.Sum256([]byte(cliqueID))
	name := fmt.Sprintf("%s.%s", cd.UID, hex.EncodeToString(digest[:8]))
	snapshot, err := m.snapshotLister.ComputeDomainCliqueSnapshots(m.config.flags.namespace).Get(name)
	if err != nil {
		return fmt.Errorf("get controller-owned snapshot: %w", err)
	}
	if snapshot.DeletionTimestamp != nil {
		return fmt.Errorf("controller-owned snapshot is deleting")
	}
	if snapshot.Spec.Protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 ||
		snapshot.Spec.ComputeDomainUID != cd.UID ||
		snapshot.Spec.ComputeDomainName != cd.Name ||
		snapshot.Spec.ComputeDomainNamespace != cd.Namespace ||
		snapshot.Spec.CliqueID != cliqueID ||
		snapshot.Status.Phase != nvapi.ComputeDomainCliqueSnapshotPhaseActive ||
		snapshot.Status.Generation < 1 ||
		snapshot.Status.Hash == "" {
		return fmt.Errorf("controller-owned snapshot is not active for this ComputeDomain")
	}
	if err := validateSnapshotStructure(snapshot); err != nil {
		return fmt.Errorf("validate controller-owned snapshot: %w", err)
	}

	var local *nvapi.ComputeDomainCliqueMember
	for i := range snapshot.Status.Members {
		member := &snapshot.Status.Members[i]
		if member.NodeName == m.config.flags.nodeName {
			if local != nil {
				return fmt.Errorf("active snapshot has multiple members for current node")
			}
			local = member
		}
	}
	if local == nil {
		return fmt.Errorf("active snapshot has no member for current node")
	}
	node, err := m.nodeLister.Get(m.config.flags.nodeName)
	if err != nil {
		return fmt.Errorf("get current Node identity from local cache: %w", err)
	}
	if local.NodeUID != node.UID {
		return fmt.Errorf("snapshot Node UID does not match current Node")
	}
	pod, err := m.podLister.Pods(snapshot.Namespace).Get(local.PodName)
	if err != nil {
		return fmt.Errorf("get current daemon Pod: %w", err)
	}
	if pod.DeletionTimestamp != nil || podTerminal(pod.Status.Phase) {
		return fmt.Errorf("current daemon Pod is deleting or terminal")
	}
	if pod.Spec.NodeName != local.NodeName || pod.UID != local.PodUID || pod.Status.PodIP != local.PodIP || !podControlledByUID(pod.OwnerReferences, local.DaemonSetUID) || !podReady(pod.Status.Conditions) {
		return fmt.Errorf("current daemon Pod identity, ownership, address, or readiness does not match snapshot")
	}

	receiptPath := filepath.Join(m.configFilesRoot, string(cd.UID), "snapshot-receipt.json")
	receiptBytes, err := os.ReadFile(receiptPath)
	if err != nil {
		return fmt.Errorf("read installed snapshot receipt: %w", err)
	}
	var receipt nvapi.ComputeDomainCliqueSnapshotReceipt
	if err := json.Unmarshal(receiptBytes, &receipt); err != nil {
		return fmt.Errorf("decode installed snapshot receipt: %w", err)
	}
	if receipt.ComputeDomainUID != cd.UID || receipt.SnapshotUID != snapshot.UID || receipt.SnapshotGeneration != snapshot.Status.Generation || receipt.SnapshotHash != snapshot.Status.Hash || receipt.NodeUID != local.NodeUID || receipt.PodUID != local.PodUID || receipt.Index != local.Index {
		return fmt.Errorf("installed snapshot receipt is stale or belongs to another identity")
	}
	return nil
}

func validateSnapshotStructure(snapshot *nvapi.ComputeDomainCliqueSnapshot) error {
	if snapshot.Status.MemberCount != len(snapshot.Status.Members) {
		return fmt.Errorf("memberCount %d does not match %d members", snapshot.Status.MemberCount, len(snapshot.Status.Members))
	}
	controller := metav1.GetControllerOf(snapshot)
	if controller == nil || controller.APIVersion != "apps/v1" || controller.Kind != "DaemonSet" ||
		controller.Name != snapshot.Spec.DaemonSetName || controller.UID != snapshot.Spec.DaemonSetUID {
		return fmt.Errorf("snapshot controller owner does not match its DaemonSet scope")
	}
	assignments := make(map[types.UID]nvapi.ComputeDomainCliqueAssignment, len(snapshot.Status.Assignments))
	indices := make(map[int]struct{}, len(snapshot.Status.Assignments))
	assignmentNames := make(map[string]struct{}, len(snapshot.Status.Assignments))
	assignmentPods := make(map[types.UID]struct{}, len(snapshot.Status.Assignments))
	for _, assignment := range snapshot.Status.Assignments {
		if assignment.NodeUID == "" || assignment.NodeName == "" || assignment.Index < 0 || assignment.Index >= snapshot.Spec.Capacity {
			return fmt.Errorf("assignment identity or index is invalid")
		}
		if _, duplicate := assignments[assignment.NodeUID]; duplicate {
			return fmt.Errorf("duplicate assignment Node UID %q", assignment.NodeUID)
		}
		if _, duplicate := indices[assignment.Index]; duplicate {
			return fmt.Errorf("duplicate assignment index %d", assignment.Index)
		}
		if assignment.State != nvapi.ComputeDomainCliqueAssignmentStateBound && assignment.State != nvapi.ComputeDomainCliqueAssignmentStateQuarantined {
			return fmt.Errorf("assignment state %q is invalid", assignment.State)
		}
		if _, duplicate := assignmentNames[assignment.NodeName]; duplicate {
			return fmt.Errorf("duplicate assignment Node name %q", assignment.NodeName)
		}
		if assignment.CurrentPodUID != "" {
			if _, duplicate := assignmentPods[assignment.CurrentPodUID]; duplicate {
				return fmt.Errorf("duplicate assignment current Pod UID %q", assignment.CurrentPodUID)
			}
			assignmentPods[assignment.CurrentPodUID] = struct{}{}
		}
		assignments[assignment.NodeUID] = assignment
		indices[assignment.Index] = struct{}{}
		assignmentNames[assignment.NodeName] = struct{}{}
	}
	members := slices.Clone(snapshot.Status.Members)
	slices.SortFunc(members, func(a, b nvapi.ComputeDomainCliqueMember) int { return cmp.Compare(a.Index, b.Index) })
	canonical, err := json.Marshal(members)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	if hex.EncodeToString(digest[:]) != snapshot.Status.Hash {
		return fmt.Errorf("snapshot hash does not match canonical membership")
	}
	seenNodes := make(map[types.UID]struct{}, len(members))
	seenNodeNames := make(map[string]struct{}, len(members))
	seenPods := make(map[types.UID]struct{}, len(members))
	seenPodNames := make(map[string]struct{}, len(members))
	seenIPs := make(map[netip.Addr]struct{}, len(members))
	seenMemberIndices := make(map[int]struct{}, len(members))
	for _, member := range members {
		if member.NodeUID == "" || member.PodUID == "" || member.NodeName == "" || member.PodName == "" ||
			member.DaemonSetUID != snapshot.Spec.DaemonSetUID || member.Index < 0 || member.Index >= snapshot.Spec.Capacity {
			return fmt.Errorf("member identity, owner, or index is invalid")
		}
		ip, err := netip.ParseAddr(member.PodIP)
		if err != nil || ip.IsUnspecified() {
			return fmt.Errorf("member Pod IP %q is invalid", member.PodIP)
		}
		ip = ip.Unmap()
		if _, duplicate := seenNodes[member.NodeUID]; duplicate {
			return fmt.Errorf("duplicate member Node UID %q", member.NodeUID)
		}
		if _, duplicate := seenPods[member.PodUID]; duplicate {
			return fmt.Errorf("duplicate member Pod UID %q", member.PodUID)
		}
		if _, duplicate := seenNodeNames[member.NodeName]; duplicate {
			return fmt.Errorf("duplicate member Node name %q", member.NodeName)
		}
		if _, duplicate := seenPodNames[member.PodName]; duplicate {
			return fmt.Errorf("duplicate member Pod name %q", member.PodName)
		}
		if _, duplicate := seenIPs[ip]; duplicate {
			return fmt.Errorf("duplicate member Pod IP %q", member.PodIP)
		}
		if _, duplicate := seenMemberIndices[member.Index]; duplicate {
			return fmt.Errorf("duplicate member index %d", member.Index)
		}
		assignment, found := assignments[member.NodeUID]
		if !found || assignment.State != nvapi.ComputeDomainCliqueAssignmentStateBound ||
			assignment.Index != member.Index || assignment.NodeName != member.NodeName || assignment.CurrentPodUID != member.PodUID {
			return fmt.Errorf("member at index %d does not match its bound assignment", member.Index)
		}
		seenNodes[member.NodeUID] = struct{}{}
		seenNodeNames[member.NodeName] = struct{}{}
		seenPods[member.PodUID] = struct{}{}
		seenPodNames[member.PodName] = struct{}{}
		seenIPs[ip] = struct{}{}
		seenMemberIndices[member.Index] = struct{}{}
	}
	return nil
}

func podControlledByUID(ownerReferences []metav1.OwnerReference, uid types.UID) bool {
	for _, owner := range ownerReferences {
		if owner.Controller != nil && *owner.Controller && owner.UID == uid {
			return true
		}
	}
	return false
}

func podTerminal(phase corev1.PodPhase) bool {
	return phase == corev1.PodSucceeded || phase == corev1.PodFailed
}

func podReady(conditions []corev1.PodCondition) bool {
	for _, condition := range conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// isCurrentNodeReady checks if the current node is marked as ready in the ComputeDomain.
// When the feature gate is enabled, we check both the clique and the status to ensure
// that compute domains started before the feature gate was enabled continue to work
// even after the feature gate is enabled.
func (m *ComputeDomainManager) isCurrentNodeReady(ctx context.Context, cd *nvapi.ComputeDomain) bool {
	if featuregates.Enabled(featuregates.ComputeDomainCliques) {
		if m.isCurrentNodeReadyInClique(ctx, cd) {
			return true
		}
	}
	return m.isCurrentNodeReadyInStatus(cd)
}

// isCurrentNodeReadyInStatus checks if the current node is marked as ready in the ComputeDomain status.
func (m *ComputeDomainManager) isCurrentNodeReadyInStatus(cd *nvapi.ComputeDomain) bool {
	for _, node := range cd.Status.Nodes {
		if node.Name == m.config.flags.nodeName {
			return node.Status == nvapi.ComputeDomainStatusReady
		}
	}
	return false
}

// isCurrentNodeReadyInClique checks if the current node is marked as ready in the ComputeDomainClique.
func (m *ComputeDomainManager) isCurrentNodeReadyInClique(ctx context.Context, cd *nvapi.ComputeDomain) bool {
	cliqueName := fmt.Sprintf("%s.%s", cd.UID, m.CliqueID())

	clique, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliques(m.config.flags.namespace).Get(ctx, cliqueName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("error getting ComputeDomainClique %s: %v", cliqueName, err)
		return false
	}

	for _, daemon := range clique.Daemons {
		if daemon.NodeName == m.config.flags.nodeName {
			return daemon.Status == nvapi.ComputeDomainStatusReady
		}
	}
	return false
}

func (m *ComputeDomainManager) AssertComputeDomainNamespace(ctx context.Context, claimNamespace, cdUID string) error {
	cd, err := m.GetComputeDomain(ctx, cdUID)
	if err != nil {
		return fmt.Errorf("error getting ComputeDomain: %w", err)
	}
	if cd == nil {
		return fmt.Errorf("ComputeDomain not found: %s", cdUID)
	}

	if cd.Namespace != claimNamespace {
		return fmt.Errorf("the ResourceClaim's namespace is different than the ComputeDomain's namespace")
	}

	return nil
}

// AddNodeLabel publishes the legacy-v1 routing projection. controller-v1 uses
// the controller's claim-derived attestation instead, so a kubelet can never
// authorize itself into a controller-owned snapshot.
func (m *ComputeDomainManager) AddNodeLabel(ctx context.Context, cdUID string) error {
	if err := m.assertTopologyValid(); err != nil {
		return err
	}
	if err := m.AssertPhysicalCliqueAvailable(ctx, cdUID); err != nil {
		return err
	}
	node, err := m.config.clientsets.Core.CoreV1().Nodes().Get(ctx, m.config.flags.nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error retrieving Node: %w", err)
	}
	if node.Annotations[computeDomainAttestationAnnotationKey] != "" {
		return fmt.Errorf("controller-owned ComputeDomain attestation already exists on Node")
	}
	if owner := node.Labels[controllerOwnedCliqueIsolationLabelKey]; owner != "" {
		return fmt.Errorf("physical clique is isolated for controller-owned ComputeDomain %q", owner)
	}
	if current, exists := node.Labels[computeDomainLabelKey]; exists {
		if current != cdUID {
			return fmt.Errorf("label already exists for a different ComputeDomain")
		}
		return nil
	}
	updated := node.DeepCopy()
	if updated.Labels == nil {
		updated.Labels = map[string]string{}
	}
	updated.Labels[computeDomainLabelKey] = cdUID
	if _, err := m.config.clientsets.Core.CoreV1().Nodes().Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("error updating Node with label: %w", err)
	}
	return nil
}

// AssertPhysicalCliqueAvailable prevents legacy-v1 from entering a physical
// clique retained by an unfenced controller-v1 stream. Legacy formation does
// not create a permanent reservation of its own: brownfield racks must remain
// reusable, and controller-v1 canaries are restricted to virgin or externally
// quiesced whole cliques until a verified legacy retirement protocol exists.
func (m *ComputeDomainManager) AssertPhysicalCliqueAvailable(ctx context.Context, cdUID string) error {
	if err := m.assertTopologyValid(); err != nil {
		return err
	}
	cliqueID := m.CliqueID()
	if cliqueID == "" {
		return nil
	}
	digest := sha256.Sum256([]byte(cliqueID))
	reservationName := "clique-" + hex.EncodeToString(digest[:])
	reservation, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomainCliqueReservations().Get(ctx, reservationName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check physical clique reservation: %w", err)
	}
	if reservation.Spec.CliqueID != cliqueID {
		return fmt.Errorf("physical clique reservation %q does not match local clique %q", reservation.Spec.CliqueID, cliqueID)
	}
	if reservation.Spec.ComputeDomainUID != types.UID(cdUID) {
		return fmt.Errorf("physical clique %q remains reserved by unfenced ComputeDomain %s/%s UID %q", cliqueID, reservation.Spec.ComputeDomainNamespace, reservation.Spec.ComputeDomainName, reservation.Spec.ComputeDomainUID)
	}
	return nil
}

// RemoveNodeLabel removes only a legacy routing projection. A controller
// attestation is controller-owned and is deliberately left untouched.
func (m *ComputeDomainManager) RemoveNodeLabel(ctx context.Context, cdUID string) error {
	node, err := m.config.clientsets.Core.CoreV1().Nodes().Get(ctx, m.config.flags.nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error retrieving Node: %w", err)
	}
	if node.Annotations[computeDomainAttestationAnnotationKey] != "" || node.Labels[computeDomainLabelKey] != cdUID {
		return nil
	}
	updated := node.DeepCopy()
	delete(updated.Labels, computeDomainLabelKey)
	if _, err := m.config.clientsets.Core.CoreV1().Nodes().Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("error updating Node to remove label: %w", err)
	}
	return nil
}

// SetGPUCliqueLabel sets the nvidia.com/gpu.clique node label based on the
// clique ID discovered from NVML at plugin startup. If no clique ID was
// discovered (e.g. fabric not attached), the label is simply left unset.
func (m *ComputeDomainManager) SetGPUCliqueLabel(ctx context.Context) error {
	cliqueID := m.CliqueID()
	if cliqueID == "" {
		node, err := m.config.clientsets.Core.CoreV1().Nodes().Get(ctx, m.config.flags.nodeName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read Node while startup GPU clique is absent: %w", err)
		}
		if persisted := node.Annotations[computeDomainCliqueStartupAnnotationKey]; persisted != "" || node.Labels[gpuCliqueLabelKey] != "" {
			m.cliqueIDMu.Lock()
			m.topologyInvalid = true
			m.cliqueIDMu.Unlock()
			if node.Labels[gpuCliqueLabelKey] != "" {
				if err := m.RemoveGPUCliqueLabel(ctx); err != nil {
					return fmt.Errorf("startup GPU clique disappeared and removing stale label failed: %w", err)
				}
			}
			return fmt.Errorf("startup GPU clique is absent but Node retains startup identity %q; drain and fence the node before clearing it", persisted)
		}
		return nil
	}
	node, err := m.config.clientsets.Core.CoreV1().Nodes().Get(ctx, m.config.flags.nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read Node startup topology: %w", err)
	}
	if startupCliqueID := node.Annotations[computeDomainCliqueStartupAnnotationKey]; startupCliqueID != "" && startupCliqueID != cliqueID {
		m.cliqueIDMu.Lock()
		m.topologyInvalid = true
		m.cliqueIDMu.Unlock()
		return fmt.Errorf("GPU clique ID %q does not match immutable Node startup clique %q; drain and fence the node before clearing the startup topology annotation", cliqueID, startupCliqueID)
	}

	patch := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]string{
				gpuCliqueLabelKey: cliqueID,
			},
			"annotations": map[string]string{
				computeDomainCliqueStartupAnnotationKey:    cliqueID,
				computeDomainCliqueCapabilityAnnotationKey: string(nvapi.ComputeDomainCliqueProtocolControllerV1),
			},
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	if _, err := m.config.clientsets.Core.CoreV1().Nodes().Patch(ctx, m.config.flags.nodeName, types.MergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("error patching node with label %s: %w", gpuCliqueLabelKey, err)
	}

	return nil
}

// RemoveGPUCliqueLabel removes the nvidia.com/gpu.clique node label, e.g. on
// plugin shutdown.
func (m *ComputeDomainManager) RemoveGPUCliqueLabel(ctx context.Context) error {
	patch := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]any{
				gpuCliqueLabelKey: nil,
			},
			// Keep the startup topology annotation as an immutable fence for
			// already-created daemon Pods. A different topology epoch requires
			// explicit node drain/reset, not an in-place label rewrite.
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	if _, err := m.config.clientsets.Core.CoreV1().Nodes().Patch(ctx, m.config.flags.nodeName, types.MergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("error removing Node label %s: %w", gpuCliqueLabelKey, err)
	}

	return nil
}

// periodicGPUCliqueIDRefresh periodically re-reads the GPU clique ID from
// NVML and updates the nvidia.com/gpu.clique node label if it changed.
func (m *ComputeDomainManager) periodicGPUCliqueIDRefresh(ctx context.Context) {
	ticker := time.NewTicker(gpuCliqueLabelRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.refreshGPUCliqueID(ctx); err != nil {
				klog.Errorf("error refreshing %s node label: %v", gpuCliqueLabelKey, err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// periodicTopologyRecovery consumes the controller's one-shot fence marker
// after a controller-v1 ComputeDomain has fully retired. Until that marker is
// present, topologyInvalid continues to reject all workload Prepare calls.
func (m *ComputeDomainManager) periodicTopologyRecovery(ctx context.Context) {
	ticker := time.NewTicker(topologyRecoveryInterval)
	defer ticker.Stop()
	for {
		if err := m.tryRecoverTopologyIdentity(ctx); err != nil {
			klog.Errorf("topology retirement recovery is still blocked: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *ComputeDomainManager) tryRecoverTopologyIdentity(ctx context.Context) error {
	if !m.topologyIsInvalid() {
		return nil
	}
	node, err := m.config.clientsets.Core.CoreV1().Nodes().Get(ctx, m.config.flags.nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read Node for topology recovery: %w", err)
	}
	fencedCD := node.Annotations[nvapi.ComputeDomainCliqueRetirementFencedAnnotation]
	if fencedCD == "" {
		return nil
	}
	if node.Labels[computeDomainLabelKey] != "" || node.Labels[controllerOwnedCliqueIsolationLabelKey] != "" ||
		node.Annotations[computeDomainAttestationAnnotationKey] != "" {
		return fmt.Errorf("controller fence %q is present but ComputeDomain route, isolation, or attestation still exists", fencedCD)
	}
	newCliqueID, err := m.getCliqueIDFunc()
	if err != nil {
		return fmt.Errorf("rediscover GPU clique after retirement fence: %w", err)
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]any{gpuCliqueLabelKey: nil},
			"annotations": map[string]any{
				computeDomainCliqueStartupAnnotationKey:             nil,
				computeDomainCliqueCapabilityAnnotationKey:          nil,
				nvapi.ComputeDomainCliqueRetirementFencedAnnotation: nil,
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if _, err := m.config.clientsets.Core.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("consume topology retirement fence: %w", err)
	}
	m.cliqueIDMu.Lock()
	m.cliqueID = newCliqueID
	m.topologyInvalid = false
	m.cliqueIDMu.Unlock()
	if m.config.flags.gpuCliqueLabelEnabled && newCliqueID != "" {
		if err := m.SetGPUCliqueLabel(ctx); err != nil {
			return fmt.Errorf("publish rediscovered GPU clique after retirement: %w", err)
		}
	}
	klog.Infof("consumed retirement fence %q and reset startup topology identity to %q", fencedCD, newCliqueID)
	return nil
}

// refreshGPUCliqueID re-reads the GPU clique ID. Topology is fixed for this
// plugin process because the snapshot watch and any running daemon were scoped
// at startup; every change, including empty-to-nonempty discovery, therefore
// requires a drain/fence and restart instead of in-place adoption.
func (m *ComputeDomainManager) refreshGPUCliqueID(ctx context.Context) error {
	newCliqueID, err := m.getCliqueIDFunc()
	if err != nil {
		// Once a non-empty topology has been published, losing the ability to
		// verify it is a safety event rather than merely an observability
		// failure. Continuing to admit new ComputeDomain work would authorize
		// it from stale topology. Keep the immutable startup annotation as the
		// fence, remove the routable label, and require an explicit drain/reset.
		if oldCliqueID := m.CliqueID(); oldCliqueID != "" {
			m.cliqueIDMu.Lock()
			m.topologyInvalid = true
			m.cliqueIDMu.Unlock()
			if m.config.flags.gpuCliqueLabelEnabled {
				if removeErr := m.RemoveGPUCliqueLabel(ctx); removeErr != nil {
					return fmt.Errorf("verifying GPU clique ID %q failed (%v) and removing the stale Node label failed: %w", oldCliqueID, err, removeErr)
				}
			}
			return fmt.Errorf("verifying GPU clique ID %q failed: %w; refusing new ComputeDomain work until the node and IMEX fabric are drained and fenced", oldCliqueID, err)
		}
		return fmt.Errorf("error getting cliqueID: %w", err)
	}

	oldCliqueID := m.CliqueID()
	if oldCliqueID == "" && newCliqueID != "" && !m.controllerReadersOn {
		// Preserve legacy late fabric registration. With no controller-v1
		// snapshot/Pod readers, no informer or daemon identity was scoped to the
		// empty startup value, so the historical dynamic adoption is safe.
		m.setCliqueID(newCliqueID)
	} else if newCliqueID != oldCliqueID {
		m.cliqueIDMu.Lock()
		m.topologyInvalid = true
		m.cliqueIDMu.Unlock()
		if m.config.flags.gpuCliqueLabelEnabled {
			if removeErr := m.RemoveGPUCliqueLabel(ctx); removeErr != nil {
				return fmt.Errorf("GPU clique ID changed from %q to %q and removing the stale Node label failed: %w", oldCliqueID, newCliqueID, removeErr)
			}
		}
		return fmt.Errorf("GPU clique ID changed from %q to %q; refusing in-place topology migration until the node and IMEX fabric are drained and fenced", oldCliqueID, newCliqueID)
	}

	if m.config.flags.gpuCliqueLabelEnabled {
		if err := m.SetGPUCliqueLabel(ctx); err != nil {
			return fmt.Errorf("error updating %s node label: %w", gpuCliqueLabelKey, err)
		}
	}

	return nil
}

func (m *ComputeDomainManager) GetComputeDomain(ctx context.Context, cdUID string) (*nvapi.ComputeDomain, error) {
	cds, err := m.informer.GetIndexer().ByIndex("computeDomainUID", cdUID)
	if err != nil {
		return nil, fmt.Errorf("error retrieving ComputeDomain by UID: %w", err)
	}
	if len(cds) == 0 {
		return nil, nil
	}
	if len(cds) != 1 {
		return nil, fmt.Errorf("multiple ComputeDomains with the same UID")
	}
	cd, ok := cds[0].(*nvapi.ComputeDomain)
	if !ok {
		return nil, fmt.Errorf("failed to cast to ComputeDomain")
	}
	return cd, nil
}

func (m *ComputeDomainManager) periodicCleanup(ctx context.Context) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			klog.V(6).Infof("Running periodic cleanup to remove stale ComputeDomain artifacts")

			_, err := os.Stat(m.configFilesRoot)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				klog.Errorf("error checking for existence of directory '%s': %v", m.configFilesRoot, err)
				continue
			}

			entries, err := os.ReadDir(m.configFilesRoot)
			if err != nil {
				klog.Errorf("error reading entries under directory '%s': %v", m.configFilesRoot, err)
				continue
			}

			for _, e := range entries {
				if !e.IsDir() {
					continue
				}

				// Convention: per-CD directory with CD UID as basename
				uid := e.Name()
				path := filepath.Join(m.configFilesRoot, e.Name())

				computeDomain, err := m.GetComputeDomain(ctx, uid)
				if err != nil {
					klog.Errorf("error getting ComputeDomain: %v", err)
					continue
				}

				// CD still exists, do not clean up
				if computeDomain != nil {
					continue
				}

				klog.V(6).Infof("Stale directory found for ComputeDomain '%s', running cleanup", uid)

				if err := os.RemoveAll(path); err != nil {
					klog.Errorf("error removing artifacts directory for ComputeDomain '%s': %v", uid, err)
					continue
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
