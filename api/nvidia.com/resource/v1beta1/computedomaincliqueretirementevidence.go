/*
Copyright The Kubernetes Authors.

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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	ComputeDomainCliqueRetirementEvidenceReasonProcessExit = "ProcessExit"
	ComputeDomainCliqueRetirementEvidenceReasonNodeReboot  = "NodeReboot"

	ComputeDomainCliqueRetirementEvidenceComputeDomainLabel = "resource.nvidia.com/computeDomain"
	ComputeDomainCliqueRetirementEvidenceSnapshotUIDLabel   = "resource.nvidia.com/computeDomainCliqueSnapshotUID"
)

func ComputeDomainCliqueRetirementEvidenceName(snapshotUID types.UID, index int) string {
	return fmt.Sprintf("retirement-%s-%d", snapshotUID, index)
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +kubebuilder:resource:scope=Namespaced,shortName=cdcretirementevidence
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.spec.reason`
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeName`
// +kubebuilder:printcolumn:name="Index",type=integer,JSONPath=`.spec.index`
// +kubebuilder:validation:XValidation:rule="self.spec == oldSelf.spec",message="retirement evidence is immutable"

// ComputeDomainCliqueRetirementEvidence is a durable, immutable witness that
// one published member's old IMEX process cannot still be running. It is not
// owned by the daemon Pod because Pod deletion must not erase fence evidence.
// The controller deletes it only after reservation release is durable.
type ComputeDomainCliqueRetirementEvidence struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ComputeDomainCliqueRetirementEvidenceSpec `json:"spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ComputeDomainCliqueRetirementEvidenceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ComputeDomainCliqueRetirementEvidence `json:"items"`
}

// ComputeDomainCliqueRetirementEvidenceSpec binds the original published
// member to the exact Pod-bound witness which created this object. ProcessExit
// requires the original daemon itself. NodeReboot permits a replacement
// daemon only when the published and current kernel boot IDs differ.
// +kubebuilder:validation:XValidation:rule="self.reason != 'ProcessExit' || (self.originalPodName == self.witnessPodName && self.originalPodUID == self.witnessPodUID)",message="process-exit evidence must come from the original published daemon"
// +kubebuilder:validation:XValidation:rule="self.reason != 'NodeReboot' || (self.activationBootID != '' && self.witnessBootID != '' && self.activationBootID != self.witnessBootID)",message="node-reboot evidence requires distinct nonempty boot IDs"
type ComputeDomainCliqueRetirementEvidenceSpec struct {
	// +kubebuilder:validation:Enum=controller-v1
	Protocol ComputeDomainCliqueProtocol `json:"protocol"`
	// +kubebuilder:validation:Enum=ProcessExit;NodeReboot
	Reason string `json:"reason"`

	// +kubebuilder:validation:XValidation:rule="self != ''",message="computeDomainUID must not be empty"
	ComputeDomainUID types.UID `json:"computeDomainUID"`
	// +kubebuilder:validation:MinLength=1
	SnapshotName string `json:"snapshotName"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="snapshotUID must not be empty"
	SnapshotUID types.UID `json:"snapshotUID"`
	// +kubebuilder:validation:Minimum=1
	SnapshotGeneration int64 `json:"snapshotGeneration"`
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	SnapshotHash string `json:"snapshotHash"`

	// +kubebuilder:validation:Minimum=0
	Index int `json:"index"`
	// +kubebuilder:validation:MinLength=1
	NodeName string `json:"nodeName"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="nodeUID must not be empty"
	NodeUID          types.UID `json:"nodeUID"`
	ActivationBootID string    `json:"activationBootID,omitempty"`
	WitnessBootID    string    `json:"witnessBootID,omitempty"`

	// +kubebuilder:validation:MinLength=1
	OriginalPodName string `json:"originalPodName"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="originalPodUID must not be empty"
	OriginalPodUID types.UID `json:"originalPodUID"`
	// +kubebuilder:validation:MinLength=1
	WitnessPodName string `json:"witnessPodName"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="witnessPodUID must not be empty"
	WitnessPodUID types.UID `json:"witnessPodUID"`

	// +kubebuilder:validation:MinLength=1
	DaemonSetName string `json:"daemonSetName"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="daemonSetUID must not be empty"
	DaemonSetUID types.UID `json:"daemonSetUID"`
}
