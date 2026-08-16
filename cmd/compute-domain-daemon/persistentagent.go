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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"k8s.io/klog/v2"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/flags"
)

const (
	persistentAgentStatePath      = imexDaemonConfigDirPath + "/persistent-agent-state"
	persistentAgentConfigTmplPath = "/templates/compute-domain-daemon-config.tmpl.cfg"
)

type persistentAgentRuntime struct {
	processManager *ProcessManager
	podIP          string
	cliqueID       string
	capacity       int
	root           string
	templatePath   string
	writeState     func(string) error

	computeDomainUID string
	directory        string
	configPath       string
	dnsNameManager   *DNSNameManager
}

func (r *persistentAgentRuntime) selectSnapshot(state *ControllerSnapshotDesiredState) error {
	if state == nil || state.Protocol != nvapi.ComputeDomainCliqueProtocolPersistentAgentV1 || state.ComputeDomainUID == "" || state.CliqueID != r.cliqueID {
		return fmt.Errorf("snapshot does not identify this persistent-agent runtime")
	}
	uid := string(state.ComputeDomainUID)
	writeState := r.writeState
	if writeState == nil {
		writeState = writePersistentAgentState
	}
	if uid == r.computeDomainUID {
		return writeState(snapshotRuntimeState(state))
	}
	root := r.root
	if root == "" {
		root = imexDaemonConfigDirPath
	}
	templatePath := r.templatePath
	if templatePath == "" {
		templatePath = persistentAgentConfigTmplPath
	}
	directory, err := persistentAgentDomainDirectoryAt(root, uid)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return err
	}
	configPath := filepath.Join(directory, "imexd.cfg")
	nodesPath := filepath.Join(directory, "nodes.cfg")
	if err := writeIMEXConfigAt(templatePath, configPath, nodesPath, r.podIP); err != nil {
		return err
	}
	dnsNameManager := NewDNSNameManager(r.cliqueID, r.capacity, nodesPath)
	if err := dnsNameManager.WriteNodesConfig(); err != nil {
		return err
	}
	if err := r.processManager.SetCommand([]string{imexDaemonBinaryName, "-c", configPath}); err != nil {
		return err
	}
	if r.computeDomainUID != "" {
		if err := os.RemoveAll(r.directory); err != nil {
			return fmt.Errorf("remove fenced persistent-agent state %q: %w", r.directory, err)
		}
	}
	r.computeDomainUID = uid
	r.directory = directory
	r.configPath = configPath
	r.dnsNameManager = dnsNameManager
	return writeState(snapshotRuntimeState(state))
}

func snapshotRuntimeState(state *ControllerSnapshotDesiredState) string {
	if state.RetirementEvidence != nil {
		return "retiring"
	}
	return "assigning"
}

func persistentAgentDomainDirectory(uid string) (string, error) {
	return persistentAgentDomainDirectoryAt(imexDaemonConfigDirPath, uid)
}

func persistentAgentDomainDirectoryAt(root, uid string) (string, error) {
	if uid == "" || uid == "." || uid == ".." || filepath.Base(uid) != uid {
		return "", fmt.Errorf("invalid ComputeDomain UID %q for local state", uid)
	}
	return filepath.Join(root, uid), nil
}

func writePersistentAgentState(state string) error {
	temporary, err := os.CreateTemp(imexDaemonConfigDirPath, ".persistent-agent-state-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if _, err := temporary.WriteString(state + "\n"); err != nil {
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
	return os.Rename(name, persistentAgentStatePath)
}

func checkPersistentAgentState() error {
	return checkPersistentAgentStateAt(persistentAgentStatePath)
}

func checkPersistentAgentStateAt(path string) error {
	state, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("persistent agent supervisor state is unavailable: %w", err)
	}
	switch string(state) {
	case "idle\n", "ready\n":
		return nil
	default:
		return fmt.Errorf("persistent agent is not ready: %s", state)
	}
}

func invalidatePersistentAgentReceipts() error {
	receipts, err := filepath.Glob(filepath.Join(imexDaemonConfigDirPath, "*", "snapshot-receipt.json"))
	if err != nil {
		return err
	}
	for _, receipt := range receipts {
		if err := os.Remove(receipt); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("invalidate stale persistent-agent receipt %q: %w", receipt, err)
		}
	}
	return nil
}

func runPersistentAgent(ctx context.Context, cancel context.CancelFunc, daemonFlags *Flags, clientsets flags.ClientSets, bootID string) error {
	if err := invalidatePersistentAgentReceipts(); err != nil {
		return err
	}
	if daemonFlags.cliqueID == "" {
		if err := writePersistentAgentState("idle"); err != nil {
			return err
		}
		klog.Infof("persistent ComputeDomain agent has no fabric clique and remains idle")
		<-ctx.Done()
		return nil
	}
	config := &ControllerConfig{
		clientsets: clientsets, cliqueID: daemonFlags.cliqueID,
		nodeName: daemonFlags.nodeName, podIP: daemonFlags.podIP, podUID: daemonFlags.podUID,
		podName: daemonFlags.podName, podNamespace: daemonFlags.podNamespace, bootID: bootID,
		maxNodesPerIMEXDomain: daemonFlags.maxNodesPerIMEXDomain,
		protocol:              nvapi.ComputeDomainCliqueProtocolPersistentAgentV1,
	}
	controller, err := NewController(config)
	if err != nil {
		return err
	}
	// Never expose a receipt from a previous container incarnation as current
	// applied state. The matching snapshot will republish it only after the new
	// child has answered READY.
	if err := controller.ClearSnapshotAppliedState(ctx, &ControllerSnapshotDesiredState{Protocol: nvapi.ComputeDomainCliqueProtocolPersistentAgentV1}); err != nil {
		return fmt.Errorf("clear stale persistent-agent applied state: %w", err)
	}

	processManager := NewProcessManager(nil)
	runtime := &persistentAgentRuntime{
		processManager: processManager, podIP: daemonFlags.podIP,
		cliqueID: daemonFlags.cliqueID, capacity: daemonFlags.maxNodesPerIMEXDomain,
	}
	ops := controllerSnapshotApplyOperations{
		selectRuntime: runtime.selectSnapshot,
		updateHosts: func(members []*nvapi.ComputeDomainDaemonInfo) (bool, error) {
			return runtime.dnsNameManager.UpdateDNSNameMappings(members)
		},
		ensureIMEX:  processManager.EnsureStarted,
		restartIMEX: processManager.Restart,
		checkIMEX: func() error {
			checkCtx, checkCancel := context.WithTimeout(ctx, 10*time.Second)
			defer checkCancel()
			return checkIMEXReadyAt(checkCtx, runtime.configPath)
		},
		writeReceipt: func(receipt *nvapi.ComputeDomainCliqueSnapshotReceipt) error {
			return writeSnapshotReceiptAt(runtime.directory, receipt)
		},
		writeAppliedState: func(state *ControllerSnapshotDesiredState) error {
			if err := controller.WriteSnapshotAppliedState(ctx, state); err != nil {
				return err
			}
			return writePersistentAgentState("ready")
		},
		retireIMEX: processManager.Stop,
		writeRetirementEvidence: func(state *ControllerSnapshotDesiredState) error {
			return controller.PublishSnapshotRetirementEvidence(ctx, state)
		},
		clearAppliedState: func(state *ControllerSnapshotDesiredState) error {
			if err := controller.ClearSnapshotAppliedState(ctx, state); err != nil {
				return err
			}
			if err := os.Remove(filepath.Join(runtime.directory, "snapshot-receipt.json")); err != nil && !os.IsNotExist(err) {
				return err
			}
			return writePersistentAgentState("idle")
		},
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(3)
	go func() {
		defer waitGroup.Done()
		if err := controller.Run(ctx); err != nil {
			klog.Errorf("persistent-agent controller failed: %v", err)
			cancel()
		}
	}()
	go func() {
		defer waitGroup.Done()
		if err := runControllerSnapshotApplyLoop(ctx, controller, ops); err != nil {
			klog.Errorf("persistent-agent apply loop failed: %v", err)
			cancel()
		}
	}()
	go func() {
		defer waitGroup.Done()
		if err := processManager.WatchdogWithoutRestart(ctx); err != nil {
			klog.Errorf("persistent-agent process supervisor failed: %v", err)
			cancel()
		}
	}()
	select {
	case <-ctx.Done():
	case <-controller.Started():
		if !controller.HasActiveOrRetiringSnapshot() {
			if err := writePersistentAgentState("idle"); err != nil {
				klog.Errorf("persistent-agent idle state failed: %v", err)
				cancel()
			}
		}
	}
	waitGroup.Wait()
	return nil
}
