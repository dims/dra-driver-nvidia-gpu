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

package v1beta1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeDomainCliqueProtocolCompatibility(t *testing.T) {
	channel := DefaultComputeDomainChannelConfig()
	channel.DomainID = "domain"
	require.Empty(t, channel.Protocol, "legacy claim templates must remain marker-less during rollout")
	require.NoError(t, channel.Normalize())
	require.Equal(t, ComputeDomainCliqueProtocolLegacyV1, channel.Protocol)
	require.NoError(t, channel.Validate())

	channel.Protocol = "unknown-v9"
	require.Error(t, channel.Validate())

	daemon := DefaultComputeDomainDaemonConfig()
	daemon.DomainID = "domain"
	daemon.Protocol = ComputeDomainCliqueProtocolControllerV1
	require.NoError(t, daemon.Validate())
}

func TestComputeDomainDaemonModeCompatibility(t *testing.T) {
	t.Run("old config defaults to per-domain", func(t *testing.T) {
		config := DefaultComputeDomainDaemonConfig()
		config.DomainID = "domain-uid"
		require.NoError(t, config.Normalize())
		require.Equal(t, ComputeDomainDaemonModePerDomain, config.Mode)
		require.NoError(t, config.Validate())
	})

	t.Run("persistent agent has no synthetic domain", func(t *testing.T) {
		config := DefaultComputeDomainDaemonConfig()
		config.Mode = ComputeDomainDaemonModePersistentAgent
		require.NoError(t, config.Normalize())
		require.NoError(t, config.Validate())
	})

	for name, config := range map[string]*ComputeDomainDaemonConfig{
		"per-domain without domain": {
			Mode: ComputeDomainDaemonModePerDomain,
		},
		"persistent agent with domain": {
			Mode:     ComputeDomainDaemonModePersistentAgent,
			DomainID: "synthetic-domain",
		},
		"unknown mode": {
			Mode: "Other",
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, config.Validate())
		})
	}
}
