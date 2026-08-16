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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

func TestPersistentAgentRuntimeSelectsAndReusesOneChildConfiguration(t *testing.T) {
	root := t.TempDir()
	templatePath := filepath.Join(root, "imexd.cfg.tmpl")
	require.NoError(t, os.WriteFile(templatePath, []byte("bind={{.IMEXCmdBindInterfaceIP}}\nnodes={{.IMEXDaemonNodesConfigPath}}\n"), 0600))
	states := []string{}
	processManager := NewProcessManager(nil)
	runtime := &persistentAgentRuntime{
		processManager: processManager, podIP: "10.0.0.1", cliqueID: "clique-a", capacity: 18,
		root: root, templatePath: templatePath, writeState: func(state string) error {
			states = append(states, state)
			return nil
		},
	}
	first := &PersistentAgentDesiredState{
		ComputeDomainUID: types.UID("domain-a"), CliqueID: "clique-a",
	}
	require.NoError(t, runtime.selectSnapshot(first))
	require.FileExists(t, filepath.Join(root, "domain-a", "imexd.cfg"))
	require.Equal(t, []string{"assigning"}, states)
	require.Equal(t, []string{imexDaemonBinaryName, "-c", filepath.Join(root, "domain-a", "imexd.cfg")}, processManager.cmd)

	first.RetirementEvidence = &nvapi.ComputeDomainCliqueRetirementEvidenceSpec{}
	require.NoError(t, runtime.selectSnapshot(first))
	require.Equal(t, []string{"assigning", "retiring"}, states)

	second := &PersistentAgentDesiredState{
		ComputeDomainUID: types.UID("domain-b"), CliqueID: "clique-a",
	}
	require.NoError(t, runtime.selectSnapshot(second))
	require.NoDirExists(t, filepath.Join(root, "domain-a"))
	require.FileExists(t, filepath.Join(root, "domain-b", "imexd.cfg"))
	require.Equal(t, []string{imexDaemonBinaryName, "-c", filepath.Join(root, "domain-b", "imexd.cfg")}, processManager.cmd)

	second.ComputeDomainUID = types.UID("../escape")
	require.ErrorContains(t, runtime.selectSnapshot(second), "invalid ComputeDomain UID")
}

func TestPersistentAgentProbeState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	require.Error(t, checkPersistentAgentStateAt(path))
	for _, state := range []string{"starting", "assigning", "retiring"} {
		require.NoError(t, os.WriteFile(path, []byte(state+"\n"), 0600))
		require.Error(t, checkPersistentAgentStateAt(path))
	}
	for _, state := range []string{"idle", "ready"} {
		require.NoError(t, os.WriteFile(path, []byte(state+"\n"), 0600))
		require.NoError(t, checkPersistentAgentStateAt(path))
	}
}
