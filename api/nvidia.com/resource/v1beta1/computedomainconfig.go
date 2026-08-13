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

const (
	ComputeDomainCliqueProtocolLegacyV1     ComputeDomainCliqueProtocol = "legacy-v1"
	ComputeDomainCliqueProtocolControllerV1 ComputeDomainCliqueProtocol = "controller-v1"

	// ComputeDomainCliqueProtocolAnnotation is set by the controller before it
	// creates any per-ComputeDomain objects. Existing marker-less domains are
	// normalized to legacy-v1.
	ComputeDomainCliqueProtocolAnnotation = "resource.nvidia.com/computeDomainCliqueProtocol"
	// ComputeDomainCliqueRequestedProtocolAnnotation is an explicit canary
	// request on a newly created ComputeDomain. The controller validates it and
	// persists the separate immutable protocol annotation before creating any
	// artifacts. A controller-v1 request is honored only while the alpha gate
	// and API preflight are enabled.
	ComputeDomainCliqueRequestedProtocolAnnotation = "resource.nvidia.com/requestedComputeDomainCliqueProtocol"
)

// ValidateComputeDomainCliqueProtocol rejects unknown protocol markers. Empty
// is accepted only as the backward-compatible spelling of legacy-v1.
func ValidateComputeDomainCliqueProtocol(protocol ComputeDomainCliqueProtocol) error {
	switch protocol {
	case "", ComputeDomainCliqueProtocolLegacyV1, ComputeDomainCliqueProtocolControllerV1:
		return nil
	default:
		return fmt.Errorf("unknown compute domain clique protocol %q", protocol)
	}
}

// EffectiveComputeDomainCliqueProtocol normalizes configuration created by an
// older controller to the legacy protocol.
func EffectiveComputeDomainCliqueProtocol(protocol ComputeDomainCliqueProtocol) ComputeDomainCliqueProtocol {
	if protocol == "" {
		return ComputeDomainCliqueProtocolLegacyV1
	}
	return protocol
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
	DomainID        string                      `json:"domainID"`
	Protocol        ComputeDomainCliqueProtocol `json:"protocol,omitempty"`
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
	return nil
}

// Validate ensures that ComputeDomainDaemonConfig has a valid set of values.
func (c *ComputeDomainDaemonConfig) Validate() error {
	if c.DomainID == "" {
		return fmt.Errorf("domainID cannot be empty")
	}
	return ValidateComputeDomainCliqueProtocol(c.Protocol)
}
