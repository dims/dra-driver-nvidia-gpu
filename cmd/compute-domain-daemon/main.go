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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"text/template"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/urfave/cli/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/internal/common"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/bootid"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/featuregates"
	pkgflags "sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
)

const (
	imexDaemonConfigDirPath   = "/imexd"
	imexDaemonConfigPath      = imexDaemonConfigDirPath + "/imexd.cfg"
	imexDaemonConfigTmplPath  = imexDaemonConfigDirPath + "/imexd.cfg.tmpl"
	imexDaemonNodesConfigPath = imexDaemonConfigDirPath + "/nodes.cfg"
	imexDaemonBinaryName      = "nvidia-imex"
	imexCtlBinaryName         = "nvidia-imex-ctl"
)

type Flags struct {
	cliqueID               string
	computeDomainUUID      string
	computeDomainName      string
	computeDomainNamespace string
	nodeName               string
	podIP                  string
	podUID                 string
	podName                string
	podNamespace           string
	maxNodesPerIMEXDomain  int
	protocol               string
	persistentAgent        bool
	persistentAgentCDI     bool
	httpEndpoint           string
	metricsPath            string
	klogVerbosity          int
}

type IMEXConfigTemplateData struct {
	IMEXCmdBindInterfaceIP    string
	IMEXDaemonNodesConfigPath string
}

func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newApp() *cli.App {
	loggingConfig := pkgflags.NewLoggingConfig()
	featureGateConfig := pkgflags.NewFeatureGateConfig()
	flags := &Flags{}

	// Create a wrapper that will be used to gracefully shut down all subcommands
	wrapper := func(ctx context.Context, f func(ctx context.Context, cancel context.CancelFunc, flags *Flags) error) error {
		// Create a cancelable context from the one passed in
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGTERM)
		go func() {
			<-sigChan
			klog.Infof("Received SIGTERM, initiate shutdown")
			cancel()
		}()

		// Call the wrapped function
		return f(ctx, cancel, flags)
	}

	cliFlags := []cli.Flag{
		&cli.StringFlag{
			Name:        "cliqueid",
			Usage:       "The clique ID for this node.",
			EnvVars:     []string{"CLIQUE_ID"},
			Destination: &flags.cliqueID,
		},
		&cli.StringFlag{
			Name:        "compute-domain-uuid",
			Usage:       "The UUID of the ComputeDomain to manage.",
			EnvVars:     []string{"COMPUTE_DOMAIN_UUID"},
			Destination: &flags.computeDomainUUID,
		},
		&cli.StringFlag{
			Name:        "compute-domain-name",
			Usage:       "The name of the ComputeDomain to manage.",
			EnvVars:     []string{"COMPUTE_DOMAIN_NAME"},
			Destination: &flags.computeDomainName,
		},
		&cli.StringFlag{
			Name:        "compute-domain-namespace",
			Usage:       "The namespace of the ComputeDomain to manage.",
			Value:       "default",
			EnvVars:     []string{"COMPUTE_DOMAIN_NAMESPACE"},
			Destination: &flags.computeDomainNamespace,
		},
		&cli.StringFlag{
			Name:        "node-name",
			Usage:       "The name of this Kubernetes node.",
			EnvVars:     []string{"NODE_NAME"},
			Destination: &flags.nodeName,
		},
		&cli.StringFlag{
			Name:        "pod-ip",
			Usage:       "The IP address of this pod.",
			EnvVars:     []string{"POD_IP"},
			Destination: &flags.podIP,
		},
		&cli.StringFlag{
			Name:        "pod-uid",
			Usage:       "The UID of this pod.",
			EnvVars:     []string{"POD_UID"},
			Destination: &flags.podUID,
		},
		&cli.StringFlag{
			Name:        "pod-name",
			Usage:       "The name of this pod.",
			EnvVars:     []string{"POD_NAME"},
			Destination: &flags.podName,
		},
		&cli.StringFlag{
			Name:        "pod-namespace",
			Usage:       "The namespace of this pod.",
			EnvVars:     []string{"POD_NAMESPACE"},
			Destination: &flags.podNamespace,
		},
		&cli.IntFlag{
			Name:        "max-nodes-per-imex-domain",
			Usage:       "The maximum number of possible nodes per IMEX domain",
			EnvVars:     []string{"MAX_NODES_PER_IMEX_DOMAIN"},
			Destination: &flags.maxNodesPerIMEXDomain,
		},
		&cli.StringFlag{
			Name:        "cdc-protocol",
			Usage:       "Immutable clique ownership protocol for this daemon.",
			Value:       string(nvapi.ComputeDomainCliqueProtocolLegacyV1),
			EnvVars:     []string{"CDC_PROTOCOL"},
			Destination: &flags.protocol,
		},
		&cli.BoolFlag{
			Name:        "persistent-agent",
			Usage:       "Run as the installation-scoped persistent ComputeDomain agent.",
			EnvVars:     []string{"PERSISTENT_AGENT"},
			Destination: &flags.persistentAgent,
		},
		&cli.BoolFlag{
			Name:        "persistent-agent-cdi",
			Usage:       "Internal proof that persistent-agent CDI edits were applied.",
			EnvVars:     []string{"PERSISTENT_AGENT_CDI"},
			Destination: &flags.persistentAgentCDI,
			Hidden:      true,
		},
	}
	cliFlags = append(cliFlags, featureGateConfig.Flags()...)
	cliFlags = append(cliFlags, loggingConfig.Flags()...)

	// Create the app
	app := &cli.App{
		Name:  "compute-domain-daemon",
		Usage: "compute-domain-daemon manages the IMEX daemon for NVIDIA compute domains.",
		Flags: cliFlags,
		Before: func(c *cli.Context) error {
			// `loggingConfig` must be applied before doing any logging
			err := loggingConfig.Apply()

			// Store klog's log verbosity setting in this program's config for
			// later runtime inspection (it's otherwise not accessible anymore
			// because we do not expose the raw `cliFlags`.
			flags.klogVerbosity = int(loggingConfig.Config.Verbosity)
			return err
		},
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "Run the compute domain daemon",
				Before: func(c *cli.Context) error {
					// `check` (e.g. startupProbe) does not use this hook — avoid noisy logs on every probe.
					pkgflags.LogStartupConfig(flags, loggingConfig)
					return nil
				},
				Action: func(c *cli.Context) error {
					return wrapper(c.Context, run)
				},
			},
			{
				Name:  "check",
				Usage: "Check if the node is IMEX capable and if the IMEX daemon is ready",
				Action: func(c *cli.Context) error {
					return wrapper(c.Context, check)
				},
			},
		},
	}

	return app
}

// Run invokes the IMEX daemon and manages its lifecycle.
func run(ctx context.Context, cancel context.CancelFunc, flags *Flags) error {
	// Verify that CDI container edits were applied by the container runtime by
	// checking for COMPUTE_DOMAIN_UUID, which is always injected as part of the
	// CDI edits. If it is missing, CDI is likely disabled and the daemon cannot
	// function correctly (e.g. the /imexd mount will be missing).
	if flags.computeDomainUUID == "" && !flags.persistentAgent {
		return fmt.Errorf("CDI container edits did not apply -- is CDI enabled in your container runtime?")
	}
	if flags.computeDomainUUID != "" && flags.persistentAgent {
		return fmt.Errorf("persistent agent CDI configuration must not contain a ComputeDomain UUID")
	}
	if flags.persistentAgent && !flags.persistentAgentCDI {
		return fmt.Errorf("persistent agent CDI container edits did not apply -- is CDI enabled in your container runtime?")
	}

	common.StartDebugSignalHandlers()

	// Validate feature gate dependencies
	if err := featuregates.ValidateFeatureGates(); err != nil {
		return fmt.Errorf("feature gate validation failed: %w", err)
	}
	if flags.persistentAgent {
		if !featuregates.Enabled(featuregates.PersistentComputeDomainAgents) {
			return fmt.Errorf("persistent agent requires feature gate %s", featuregates.PersistentComputeDomainAgents)
		}
		if err := writePersistentAgentState("starting"); err != nil {
			return fmt.Errorf("initialize persistent-agent supervisor state: %w", err)
		}
	}
	protocol := nvapi.ComputeDomainCliqueProtocol(flags.protocol)
	if err := nvapi.ValidateComputeDomainCliqueProtocol(protocol); err != nil {
		return err
	}
	protocol = nvapi.EffectiveComputeDomainCliqueProtocol(protocol)
	if protocol == nvapi.ComputeDomainCliqueProtocolControllerV1 {
		// Protocol is persisted per ComputeDomain and survives changes to the
		// process-wide default feature gates. A rollback may disable the alpha
		// admission gate, but it must not silently disable capabilities required
		// by an already-created controller-v1 daemon.
		if !featuregates.Enabled(featuregates.ComputeDomainCliques) || !featuregates.Enabled(featuregates.IMEXDaemonsWithDNSNames) {
			return fmt.Errorf("persisted controller-v1 daemon requires feature gates %s and %s to remain enabled", featuregates.ComputeDomainCliques, featuregates.IMEXDaemonsWithDNSNames)
		}
	}

	// Create clientsets for Kubernetes API access
	kubeConfig := &pkgflags.KubeClientConfig{}
	clientsets, err := kubeConfig.NewClientSets()
	if err != nil {
		return fmt.Errorf("failed to create client sets: %w", err)
	}
	if flags.persistentAgent {
		bootID, err := bootid.GetCurrentBootID()
		if err != nil {
			return fmt.Errorf("read persistent-agent kernel boot ID: %w", err)
		}
		if bootID == "" {
			return fmt.Errorf("persistent agent requires a nonempty kernel boot ID")
		}
		return runPersistentAgent(ctx, cancel, flags, clientsets, bootID)
	}

	// Legacy daemons still publish their discovered clique on their own Pod.
	// Controller-v1 daemons use the read-only ServiceAccount; the controller
	// derives clique membership from trusted Node topology instead.
	if protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 {
		if err := addComputeDomainCliqueLabel(ctx, clientsets, flags); err != nil {
			return fmt.Errorf("failed to add compute domain clique label to pod: %w", err)
		}
	}

	// A controller-v1 daemon must keep its exact snapshot reader alive even when
	// local hardware discovery reports no clique. It will reject Active
	// membership because its scope is empty and will never start IMEX, but it can
	// still observe Retiring and attest the already-stopped state. Legacy mode
	// retains the historical no-clique idle path.
	if flags.cliqueID == "" && protocol != nvapi.ComputeDomainCliqueProtocolControllerV1 {
		klog.Infof("no cliqueID: skipping controller and IMEX daemon management")
		// Just wait for shutdown signal
		<-ctx.Done()
		klog.Infof("Exiting")
		return nil
	}
	if flags.cliqueID == "" {
		klog.Infof("no cliqueID: starting controller-v1 retirement-capable snapshot reader with IMEX disabled")
	}
	bootID, err := bootid.GetCurrentBootID()
	if err != nil {
		return fmt.Errorf("read kernel boot ID: %w", err)
	}
	if protocol == nvapi.ComputeDomainCliqueProtocolControllerV1 && bootID == "" {
		return fmt.Errorf("controller-v1 requires a nonempty kernel boot ID")
	}

	config := &ControllerConfig{
		httpEndpoint:           flags.httpEndpoint,
		metricsPath:            flags.metricsPath,
		clientsets:             clientsets,
		cliqueID:               flags.cliqueID,
		computeDomainUUID:      flags.computeDomainUUID,
		computeDomainName:      flags.computeDomainName,
		computeDomainNamespace: flags.computeDomainNamespace,
		nodeName:               flags.nodeName,
		podIP:                  flags.podIP,
		podUID:                 flags.podUID,
		podName:                flags.podName,
		podNamespace:           flags.podNamespace,
		bootID:                 bootID,
		maxNodesPerIMEXDomain:  flags.maxNodesPerIMEXDomain,
		protocol:               protocol,
	}

	// Render and write the IMEX daemon config with the current pod IP
	if err := writeIMEXConfig(flags.podIP); err != nil {
		return fmt.Errorf("writeIMEXConfig failed: %w", err)
	}

	// Prepare IMEX daemon process manager (not invoking the process yet).
	var dnsNameManager *DNSNameManager
	if featuregates.Enabled(featuregates.IMEXDaemonsWithDNSNames) {
		// Prepare DNS name manager
		dnsNameManager = NewDNSNameManager(flags.cliqueID, flags.maxNodesPerIMEXDomain, imexDaemonNodesConfigPath)

		// Create static nodes config file with DNS names
		if err := dnsNameManager.WriteNodesConfig(); err != nil {
			return fmt.Errorf("failed to create static nodes config: %w", err)
		}
	}

	// Prepare IMEX daemon process manager.
	daemonCommandLine := []string{imexDaemonBinaryName, "-c", imexDaemonConfigPath}
	processManager := NewProcessManager(daemonCommandLine)
	processCtx, stopProcess := context.WithCancel(ctx)
	processDone := make(chan struct{})
	var retireProcessOnce sync.Once
	var retireProcessErr error
	retireProcess := func() error {
		retireProcessOnce.Do(func() {
			stopProcess()
			<-processDone
		})
		return retireProcessErr
	}

	// Prepare controller with CD manager (not invoking the controller yet).
	controller, err := NewController(config)
	if err != nil {
		return fmt.Errorf("error creating controller: %w", err)
	}

	var wg sync.WaitGroup

	// Start controller in goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := controller.Run(ctx); err != nil {
			klog.Errorf("controller failed, initiate shutdown: %s", err)
			cancel()
		}
		klog.Infof("Terminated: controller task")
	}()

	// Start IMEX daemon update loop in goroutine (watches for CD status
	// changes and manages IMEX daemon updates).
	wg.Add(1)
	go func() {
		defer wg.Done()
		switch {
		case protocol == nvapi.ComputeDomainCliqueProtocolControllerV1:
			if err := IMEXDaemonUpdateLoopWithControllerSnapshot(ctx, controller, processManager, dnsNameManager, retireProcess); err != nil {
				klog.Errorf("controller snapshot update loop failed, initiate shutdown: %s", err)
				cancel()
			}
		case featuregates.Enabled(featuregates.IMEXDaemonsWithDNSNames):
			// Use new DNS name-based functionality
			if err := IMEXDaemonUpdateLoopWithDNSNames(ctx, controller, processManager, dnsNameManager); err != nil {
				klog.Errorf("IMEXDaemonUpdateLoop failed, initiate shutdown: %s", err)
				cancel()
			}
		default:
			// Use original IP-based functionality
			if err := IMEXDaemonUpdateLoopWithIPs(ctx, controller, flags.cliqueID, processManager); err != nil {
				klog.Errorf("IMEXDaemonUpdateLoop failed, initiate shutdown: %s", err)
				cancel()
			}
		}
		klog.Infof("Terminated: IMEX daemon update task")
	}()

	// Start child process watchdog in goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Watchdog restarts the IMEX daemon upon unexpected termination, and
		// shuts it down gracefully upon our own shutdown.
		err := processManager.Watchdog(processCtx)
		retireProcessErr = err
		close(processDone)
		if err != nil {
			klog.Errorf("watch failed, initiate shutdown: %s", err)
			cancel()
		}
		klog.Infof("Terminated: process manager")
	}()

	wg.Wait()

	// Let's not yet try to make exit code promises.
	klog.Infof("Exiting")
	return nil
}

// IMEXDaemonUpdateLoopWithIPs reacts to ComputeDomain status changes by updating the
// IMEX daemon nodes config file and (re)starting the IMEX daemon process.
func IMEXDaemonUpdateLoopWithIPs(ctx context.Context, controller *Controller, cliqueID string, pm *ProcessManager) error {
	for {
		klog.V(1).Infof("Wait for updated ComputeDomainDaemonInfo list")
		select {
		case <-ctx.Done():
			klog.Infof("shutdown: stop IMEXDaemonUpdateLoopWithIPs")
			return nil
		case daemons := <-controller.GetDaemonInfoUpdateChan():
			if err := writeDaemonsConfig(cliqueID, daemons); err != nil {
				return fmt.Errorf("writeDaemonsConfig failed: %w", err)
			}

			if cliqueID == "" {
				klog.V(1).Infof("empty cliqueID: do not start IMEX daemon")
				break
			}

			klog.Infof("Got update, (re)start IMEX daemon")
			if err := pm.Restart(); err != nil {
				// This might be a permanent problem, and retrying upon next update
				// might be pointless. Terminate us.
				return fmt.Errorf("error (re)starting IMEX daemon: %w", err)
			}
		}
	}
}

// IMEXDaemonUpdateLoopWithDNSNames reacts to ComputeDomain status changes by
// updating the /etc/hosts file with IP to DNS name mappings. This relies on
// the IMEX daemon to pick up these changes automatically (and quickly) --
// which it seems to do via grpc-based health-checking of individual
// connections. We only restart the IMEX daemon if it crashes (both
// unexpectedly and expectedly).
func IMEXDaemonUpdateLoopWithDNSNames(ctx context.Context, controller *Controller, processManager *ProcessManager, dnsNameManager *DNSNameManager) error {
	for {
		klog.V(1).Infof("Wait for updated ComputeDomainDaemonInfo list")

		select {
		case <-ctx.Done():
			klog.Infof("shutdown: stop IMEXDaemonUpdateLoopWithDNSNames")
			return nil
		case daemons := <-controller.GetDaemonInfoUpdateChan():
			updated, err := dnsNameManager.UpdateDNSNameMappings(daemons)
			if err != nil {
				return fmt.Errorf("failed to update DNS name => IP mappings: %w", err)
			}

			if dnsNameManager.cliqueID == "" {
				klog.V(1).Infof("empty cliqueID: do not start IMEX daemon")
				break
			}

			fresh, err := processManager.EnsureStarted()
			if err != nil {
				return fmt.Errorf("failed to ensure IMEX daemon is started: %w", err)
			}
			dnsNameManager.LogDNSNameMappings()

			// Skip sending SIGUSR1 when the process is fresh (has newly been
			// created) or when this was a noop update. TODO: review skipping
			// this also if the new set of IP addresses only strictly removes
			// addresses compared to the old set (then we don't need to force
			// the daemon to re-resolve & re-connect).
			if !updated || fresh {
				break
			}

			// Actively ask the IMEX daemon to re-read its config and to
			// re-connect to its peers (involving DNS name re-resolution).
			klog.Infof("updated DNS/IP mapping, old process: send SIGUSR1")
			if err := processManager.Signal(syscall.SIGUSR1); err != nil {
				// Only log (ignore this error for now: if the process went away
				// unexpectedly, the process manager will handle that. If any
				// other error resulted in bad signal delivery, we may get away
				// with it).
				klog.Errorf("failed to send SIGUSR1 to child process: %s", err)
				break
			}
		}
	}
}

type controllerSnapshotApplyState struct {
	desired         *ControllerSnapshotDesiredState
	restartRequired bool
}

type controllerSnapshotApplyOperations struct {
	selectRuntime           func(*ControllerSnapshotDesiredState) error
	updateHosts             func([]*nvapi.ComputeDomainDaemonInfo) (bool, error)
	ensureIMEX              func() (bool, error)
	restartIMEX             func() error
	checkIMEX               func() error
	writeReceipt            func(*nvapi.ComputeDomainCliqueSnapshotReceipt) error
	writeAppliedState       func(*ControllerSnapshotDesiredState) error
	retireIMEX              func() error
	writeRetirementEvidence func(*ControllerSnapshotDesiredState) error
	clearAppliedState       func(*ControllerSnapshotDesiredState) error
}

// IMEXDaemonUpdateLoopWithControllerSnapshot retries local installation
// without waiting for another API event. A newer desired snapshot supersedes
// the one being retried. The manager is told that a generation was applied
// only after hosts installation, an IMEX start/restart, a READY observation,
// and receipt persistence all succeed.
func IMEXDaemonUpdateLoopWithControllerSnapshot(ctx context.Context, controller *Controller, processManager *ProcessManager, dnsNameManager *DNSNameManager, retireIMEX func() error) error {
	if dnsNameManager == nil {
		return fmt.Errorf("controller-v1 requires DNS name-based IMEX configuration")
	}
	ops := controllerSnapshotApplyOperations{
		updateHosts: dnsNameManager.UpdateDNSNameMappings,
		ensureIMEX:  processManager.EnsureStarted,
		restartIMEX: processManager.Restart,
		checkIMEX: func() error {
			checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			return checkIMEXReady(checkCtx)
		},
		writeReceipt: writeSnapshotReceipt,
		retireIMEX:   retireIMEX,
		writeRetirementEvidence: func(state *ControllerSnapshotDesiredState) error {
			return controller.PublishSnapshotRetirementEvidence(ctx, state)
		},
	}
	return runControllerSnapshotApplyLoop(ctx, controller, ops)
}

func runControllerSnapshotApplyLoop(ctx context.Context, controller *Controller, ops controllerSnapshotApplyOperations) error {
	desiredStateChan := controller.GetSnapshotDesiredStateChan()
	if desiredStateChan == nil {
		return fmt.Errorf("controller-owned snapshot manager is unavailable")
	}
	var pending controllerSnapshotApplyState
	for {
		if pending.desired == nil {
			select {
			case <-ctx.Done():
				return nil
			case pending.desired = <-desiredStateChan:
			}
		}

		// Collapse any burst to the latest committed desired snapshot before
		// starting local I/O. restartRequired is retained because a previous
		// hosts write may still require an existing process to be restarted.
		for draining := true; draining; {
			select {
			case pending.desired = <-desiredStateChan:
			default:
				draining = false
			}
		}

		if err := applyControllerSnapshot(&pending, ops); err == nil {
			if pending.desired.RetirementEvidence != nil {
				controller.MarkSnapshotRetired(pending.desired)
			} else {
				controller.MarkSnapshotApplied(pending.desired)
			}
			pending.desired = nil
			continue
		} else {
			identity := pending.desired.identity()
			klog.Errorf("failed to apply controller snapshot %s/%d: %v; retrying", identity.uid, identity.generation, err)
		}

		retry := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			retry.Stop()
			return nil
		case pending.desired = <-desiredStateChan:
			retry.Stop()
		case <-retry.C:
		}
	}
}

func applyControllerSnapshot(state *controllerSnapshotApplyState, ops controllerSnapshotApplyOperations) error {
	if ops.selectRuntime != nil {
		if err := ops.selectRuntime(state.desired); err != nil {
			return fmt.Errorf("failed to select snapshot runtime: %w", err)
		}
	}
	if state.desired.RetirementEvidence != nil {
		if ops.retireIMEX == nil || ops.writeRetirementEvidence == nil {
			return fmt.Errorf("retirement operations are unavailable")
		}
		if err := ops.retireIMEX(); err != nil {
			return fmt.Errorf("failed to stop and reap IMEX daemon: %w", err)
		}
		if err := ops.writeRetirementEvidence(state.desired); err != nil {
			return fmt.Errorf("failed to publish durable retirement evidence: %w", err)
		}
		if ops.clearAppliedState != nil {
			if err := ops.clearAppliedState(state.desired); err != nil {
				return fmt.Errorf("failed to clear applied snapshot state: %w", err)
			}
		}
		state.restartRequired = false
		return nil
	}
	updated, err := ops.updateHosts(state.desired.Members)
	if err != nil {
		return fmt.Errorf("failed to update DNS name mappings: %w", err)
	}
	state.restartRequired = state.restartRequired || updated

	fresh, err := ops.ensureIMEX()
	if err != nil {
		return fmt.Errorf("failed to ensure IMEX daemon is started: %w", err)
	}
	if fresh {
		// A newly started process reads the already-installed mapping. Starting
		// a fresh process, rather than merely delivering a reload signal, is the
		// causal boundary used by controller-v1 before it acknowledges a map.
		state.restartRequired = false
	} else if state.restartRequired {
		if err := ops.restartIMEX(); err != nil {
			return fmt.Errorf("failed to restart IMEX daemon with new mapping: %w", err)
		}
		state.restartRequired = false
	}

	// Do not publish a receipt merely because a signal was delivered or a
	// process was spawned. The new process must answer READY after the mapping
	// was installed. A future IMEX generation/digest acknowledgement can make
	// this proof stronger without changing the snapshot protocol.
	if err := ops.checkIMEX(); err != nil {
		return fmt.Errorf("IMEX daemon did not become ready after applying snapshot: %w", err)
	}

	if err := ops.writeReceipt(state.desired.Receipt); err != nil {
		return fmt.Errorf("failed to write installed snapshot receipt: %w", err)
	}
	if ops.writeAppliedState != nil {
		if err := ops.writeAppliedState(state.desired); err != nil {
			return fmt.Errorf("failed to publish applied snapshot state: %w", err)
		}
	}
	return nil
}

func writeSnapshotReceipt(receipt *nvapi.ComputeDomainCliqueSnapshotReceipt) error {
	return writeSnapshotReceiptAt(imexDaemonConfigDirPath, receipt)
}

func writeSnapshotReceiptAt(directory string, receipt *nvapi.ComputeDomainCliqueSnapshotReceipt) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".snapshot-receipt-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, filepath.Join(directory, "snapshot-receipt.json"))
}

// check verifies if the node is IMEX capable and if so, checks if the IMEX daemon is ready.
// It returns an error if any step fails.
func check(ctx context.Context, cancel context.CancelFunc, flags *Flags) error {
	if flags.persistentAgent {
		if !flags.persistentAgentCDI {
			return fmt.Errorf("persistent agent CDI container edits did not apply")
		}
		if err := checkPersistentAgentState(); err != nil {
			return err
		}
		fmt.Println("check succeeded (persistent agent supervisor is healthy)")
		return nil
	}
	if flags.cliqueID == "" {
		fmt.Println("check succeeded (noop, clique ID is empty)")
		return nil
	}

	return checkIMEXReady(ctx)
}

// checkIMEXReady probes the local daemon process. It intentionally does not
// claim that IMEX exposes a configuration-generation acknowledgement: the
// controller-v1 apply path pairs this check with a process start/restart that
// happens strictly after the new peer map is installed.
func checkIMEXReady(ctx context.Context) error {
	return checkIMEXReadyAt(ctx, imexDaemonConfigPath)
}

func checkIMEXReadyAt(ctx context.Context, configPath string) error {
	// -q is documented with "Query the status of the IMEX daemon once and
	// return". This probes if the local IMEX daemon is ready (not the entire
	// domain). Reference:
	// https://docs.nvidia.com/multi-node-nvlink-systems/imex-guide/cmdservice.html
	cmd := exec.CommandContext(ctx, imexCtlBinaryName, "-c", configPath, "-q")

	// Spawn child, collect standard streams.
	outerr, err := cmd.CombinedOutput()
	if err != nil {
		klog.Errorf("%s failed (%s), stdout/err: %s", imexCtlBinaryName, err, outerr)
		return fmt.Errorf("IMEX daemon check failed: error running %s: %w", imexCtlBinaryName, err)
	}

	if string(outerr) != "READY\n" {
		return fmt.Errorf("IMEX daemon not ready: %s", string(outerr))
	}

	return nil
}

// writeIMEXConfig renders the config template with the pod IP and writes it to the final config file.
func writeIMEXConfig(podIP string) error {
	return writeIMEXConfigAt(imexDaemonConfigTmplPath, imexDaemonConfigPath, imexDaemonNodesConfigPath, podIP)
}

func writeIMEXConfigAt(templatePath, configPath, nodesConfigPath, podIP string) error {
	configTemplateData := IMEXConfigTemplateData{
		IMEXCmdBindInterfaceIP:    podIP,
		IMEXDaemonNodesConfigPath: nodesConfigPath,
	}

	tmpl, err := template.ParseFiles(templatePath)
	if err != nil {
		return fmt.Errorf("error parsing template file: %w", err)
	}

	var configFile bytes.Buffer
	if err := tmpl.Execute(&configFile, configTemplateData); err != nil {
		return fmt.Errorf("error executing template: %w", err)
	}

	// Ensure the directory exists
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	if err := os.WriteFile(configPath, configFile.Bytes(), 0644); err != nil {
		return fmt.Errorf("error writing config file %v: %w", configPath, err)
	}

	klog.Infof("Rendered IMEX daemon config file with: %v", configTemplateData)
	return nil
}

// writeNodesConfig creates a nodesConfig file with IPs for nodes in the same clique.
func writeDaemonsConfig(cliqueID string, daemons []*nvapi.ComputeDomainDaemonInfo) error {
	// Ensure the directory exists
	dir := filepath.Dir(imexDaemonNodesConfigPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Create or overwrite the nodesConfig file
	f, err := os.Create(imexDaemonNodesConfigPath)
	if err != nil {
		return fmt.Errorf("failed to create nodes config file: %w", err)
	}
	defer f.Close()

	// Write IPs for daemons in the same clique
	//
	// Note(JP): do we need to apply this type of filtering also in the logic
	// that checks if an IMEX daemon restart is required?
	for _, daemon := range daemons {
		if daemon.CliqueID == cliqueID {
			if _, err := fmt.Fprintf(f, "%s\n", daemon.IPAddress); err != nil {
				return fmt.Errorf("failed to write to nodes config file: %w", err)
			}
		}
	}

	if err := logNodesConfig(); err != nil {
		return fmt.Errorf("logNodesConfig failed: %w", err)
	}
	return nil
}

// Read and log the contents of the nodes configuration file. Return an error if
// the file cannot be read.
func logNodesConfig() error {
	content, err := os.ReadFile(imexDaemonNodesConfigPath)
	if err != nil {
		return fmt.Errorf("failed to read nodes config: %w", err)
	}
	klog.Infof("Current %s:\n%s", imexDaemonNodesConfigPath, string(content))
	return nil
}

// addComputeDomainCliqueLabel adds the compute domain clique label to this daemon pod.
func addComputeDomainCliqueLabel(ctx context.Context, clientsets pkgflags.ClientSets, flags *Flags) error {
	patch := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]string{
				computeDomainCliqueLabelKey: flags.cliqueID,
			},
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch: %w", err)
	}

	_, err = clientsets.Core.CoreV1().Pods(flags.podNamespace).Patch(
		ctx,
		flags.podName,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to patch pod: %w", err)
	}

	return nil
}
