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
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProcessManagerStopReapsAlreadyExitedChild(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	require.NoError(t, cmd.Start())
	waitResult := make(chan error, 1)
	waitResult <- cmd.Wait()
	manager := &ProcessManager{handle: cmd, waitResChan: waitResult}

	require.NoError(t, manager.stop())
	require.Nil(t, manager.handle)
}

func TestProcessManagerStopKillsAndReapsAfterGracePeriod(t *testing.T) {
	manager := NewProcessManager([]string{"/bin/sh", "-c", "trap '' TERM; while :; do :; done"})
	manager.stopTimeout = 20 * time.Millisecond
	started, err := manager.EnsureStarted()
	require.NoError(t, err)
	require.True(t, started)

	require.NoError(t, manager.stop())
	require.Nil(t, manager.handle)
}

func TestProcessManagerReusableChildLifecycle(t *testing.T) {
	manager := NewProcessManager([]string{"/bin/sh", "-c", "while :; do sleep 1; done"})
	manager.stopTimeout = time.Second

	started, err := manager.EnsureStarted()
	require.NoError(t, err)
	require.True(t, started)
	started, err = manager.EnsureStarted()
	require.NoError(t, err)
	require.False(t, started, "one manager must not start a second concurrent child")
	require.Error(t, manager.SetCommand([]string{"/bin/true"}))
	require.NoError(t, manager.Stop())

	require.NoError(t, manager.SetCommand([]string{"/bin/sh", "-c", "while :; do sleep 1; done"}))
	started, err = manager.EnsureStarted()
	require.NoError(t, err)
	require.True(t, started)
	require.NoError(t, manager.Stop())
}
