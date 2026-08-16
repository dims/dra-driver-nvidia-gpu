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
	"testing"

	"github.com/stretchr/testify/require"
	logsapi "k8s.io/component-base/logs/api/v1"
)

func TestPersistentAgentFlagAfterSubcommand(t *testing.T) {
	t.Setenv("PERSISTENT_AGENT", "")
	t.Setenv("PERSISTENT_AGENT_CDI", "")
	previousReapplyHandling := logsapi.ReapplyHandling
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged
	t.Cleanup(func() { logsapi.ReapplyHandling = previousReapplyHandling })

	tests := []struct {
		command   string
		wantError string
	}{
		{command: "run", wantError: "persistent agent CDI container edits did not apply -- is CDI enabled in your container runtime?"},
		{command: "check", wantError: "persistent agent CDI container edits did not apply"},
	}
	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			// Match the argument order rendered by the persistent-agent DaemonSet.
			err := newApp().Run([]string{"compute-domain-daemon", "-v", "0", test.command, "--persistent-agent"})
			require.EqualError(t, err, test.wantError)
		})
	}
}
