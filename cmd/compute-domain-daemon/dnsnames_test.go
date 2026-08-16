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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	nvapi "sigs.k8s.io/dra-driver-nvidia-gpu/api/nvidia.com/resource/v1beta1"
)

func TestDNSNameManagerDoesNotCacheFailedHostsWrite(t *testing.T) {
	temporaryDirectory := t.TempDir()
	manager := NewDNSNameManager("clique-a", 18, filepath.Join(temporaryDirectory, "nodes.cfg"))
	manager.hostsFilePath = filepath.Join(temporaryDirectory, "missing", "hosts")
	daemons := []*nvapi.ComputeDomainDaemonInfo{{
		NodeName: "node-a", IPAddress: "10.0.0.1", CliqueID: "clique-a", Index: 0,
	}}

	updated, err := manager.UpdateDNSNameMappings(daemons)
	require.Error(t, err)
	require.False(t, updated)
	require.Empty(t, manager.ipToDNSName, "failed persistence must remain retryable")

	manager.hostsFilePath = filepath.Join(temporaryDirectory, "hosts")
	require.NoError(t, os.WriteFile(manager.hostsFilePath, []byte("127.0.0.1 localhost\n"), 0o600))
	updated, err = manager.UpdateDNSNameMappings(daemons)
	require.NoError(t, err)
	require.True(t, updated)
	require.Equal(t, IPToDNSNameMap{"10.0.0.1": "compute-domain-daemon-0000"}, manager.ipToDNSName)
}
