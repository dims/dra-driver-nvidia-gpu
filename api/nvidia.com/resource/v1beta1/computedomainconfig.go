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

package v1beta1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ComputeDomainCliqueProtocol identifies the component which owns clique
// allocation and membership for one ComputeDomain. The value is persisted in
// generated claim configuration, so it must not be inferred from feature-gate
// state or from whether a particular API object happens to exist.
type ComputeDomainCliqueProtocol string

// ComputeDomainDaemonMode identifies whether a daemon claim belongs to one
// ComputeDomain or bootstraps an installation-scoped persistent agent.
type ComputeDomainDaemonMode string

const (
	ComputeDomainCliqueProtocolLegacyV1          ComputeDomainCliqueProtocol = "legacy-v1"
	ComputeDomainCliqueProtocolPersistentAgentV1 ComputeDomainCliqueProtocol = "persistent-agent-v1"

	ComputeDomainDaemonModePerDomain       ComputeDomainDaemonMode = "PerDomain"
	ComputeDomainDaemonModePersistentAgent ComputeDomainDaemonMode = "PersistentAgent"

	// ComputeDomainCliqueProtocolAnnotation records which mutually exclusive
	// daemon provider owns a ComputeDomain. Existing marker-less domains use the
	// historical per-ComputeDomain daemon; new domains use the installation's
	// configured provider.
	ComputeDomainCliqueProtocolAnnotation = "resource.nvidia.com/computeDomainCliqueProtocol"
)

// ValidateComputeDomainCliqueProtocol rejects unknown protocol markers. Empty
// is accepted only as the backward-compatible spelling of legacy-v1.
func ValidateComputeDomainCliqueProtocol(protocol ComputeDomainCliqueProtocol) error {
	switch protocol {
	case "", ComputeDomainCliqueProtocolLegacyV1, ComputeDomainCliqueProtocolPersistentAgentV1:
		return nil
	default:
		return fmt.Errorf("unknown compute domain clique protocol %q", protocol)
	}
}

// EffectiveComputeDomainDaemonMode preserves the historical per-domain
// behavior for configurations written before mode was introduced.
func EffectiveComputeDomainDaemonMode(mode ComputeDomainDaemonMode) ComputeDomainDaemonMode {
	if mode == "" {
		return ComputeDomainDaemonModePerDomain
	}
	return mode
}

// EffectiveComputeDomainCliqueProtocol normalizes configuration created by an
// older controller to the legacy protocol.
func EffectiveComputeDomainCliqueProtocol(protocol ComputeDomainCliqueProtocol) ComputeDomainCliqueProtocol {
	if protocol == "" {
		return ComputeDomainCliqueProtocolLegacyV1
	}
	return protocol
}

func IsPersistentAgentComputeDomainCliqueProtocol(protocol ComputeDomainCliqueProtocol) bool {
	return EffectiveComputeDomainCliqueProtocol(protocol) == ComputeDomainCliqueProtocolPersistentAgentV1
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ComputeDomainChannelConfig holds the set of parameters for configuring an ComputeDomainChannel.
type ComputeDomainChannelConfig struct {
	metav1.TypeMeta `json:",inline"`
	DomainID        string                      `json:"domainID"`
	AllocationMode  string                      `json:"allocationMode,omitempty"`
	Protocol        ComputeDomainCliqueProtocol `json:"protocol,omitempty"`
}

// DefaultComputeDomainChannelConfig provides the default ComputeDomainChannel configuration.
func DefaultComputeDomainChannelConfig() *ComputeDomainChannelConfig {
	return &ComputeDomainChannelConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: GroupName + "/" + Version,
			Kind:       ComputeDomainChannelConfigKind,
		},
	}
}

// Normalize updates a ComputeDomainChannelConfig config with implied default values based on other settings.
func (c *ComputeDomainChannelConfig) Normalize() error {
	c.Protocol = EffectiveComputeDomainCliqueProtocol(c.Protocol)
	return nil
}

// Validate ensures that ComputeDomainDaemonConfig has a valid set of values.
func (c *ComputeDomainChannelConfig) Validate() error {
	if c.DomainID == "" {
		return fmt.Errorf("domainID cannot be empty")
	}
	return ValidateComputeDomainCliqueProtocol(c.Protocol)
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ComputeDomainDaemonConfig holds the set of parameters for configuring an ComputeDomainDaemon.
type ComputeDomainDaemonConfig struct {
	metav1.TypeMeta `json:",inline"`
	DomainID        string                      `json:"domainID,omitempty"`
	Protocol        ComputeDomainCliqueProtocol `json:"protocol,omitempty"`
	Mode            ComputeDomainDaemonMode     `json:"mode,omitempty"`
}

// DefaultComputeDomainDaemonConfig provides the default ComputeDomainDaemon configuration.
func DefaultComputeDomainDaemonConfig() *ComputeDomainDaemonConfig {
	return &ComputeDomainDaemonConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: GroupName + "/" + Version,
			Kind:       ComputeDomainDaemonConfigKind,
		},
	}
}

// Normalize updates a ComputeDomainDaemonConfig config with implied default values based on other settings.
func (c *ComputeDomainDaemonConfig) Normalize() error {
	c.Protocol = EffectiveComputeDomainCliqueProtocol(c.Protocol)
	c.Mode = EffectiveComputeDomainDaemonMode(c.Mode)
	return nil
}

// Validate ensures that ComputeDomainDaemonConfig has a valid set of values.
func (c *ComputeDomainDaemonConfig) Validate() error {
	if err := ValidateComputeDomainCliqueProtocol(c.Protocol); err != nil {
		return err
	}
	protocol := EffectiveComputeDomainCliqueProtocol(c.Protocol)
	switch EffectiveComputeDomainDaemonMode(c.Mode) {
	case ComputeDomainDaemonModePerDomain:
		if c.DomainID == "" {
			return fmt.Errorf("domainID cannot be empty in PerDomain mode")
		}
		if protocol != ComputeDomainCliqueProtocolLegacyV1 {
			return fmt.Errorf("PerDomain mode requires legacy-v1 protocol")
		}
	case ComputeDomainDaemonModePersistentAgent:
		if c.DomainID != "" {
			return fmt.Errorf("domainID must be empty in PersistentAgent mode")
		}
		if protocol != ComputeDomainCliqueProtocolPersistentAgentV1 {
			return fmt.Errorf("PersistentAgent mode requires persistent-agent-v1 protocol")
		}
	default:
		return fmt.Errorf("unknown compute domain daemon mode %q", c.Mode)
	}
	return nil
}
