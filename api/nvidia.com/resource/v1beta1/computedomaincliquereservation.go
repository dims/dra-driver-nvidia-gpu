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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	ComputeDomainCliqueReservationPhaseActive   = "Active"
	ComputeDomainCliqueReservationPhaseReleased = "Released"

	ComputeDomainCliqueReservationReleaseReasonNeverPublished = "NeverPublished"
	ComputeDomainCliqueReservationReleaseReasonVerifiedFence  = "VerifiedFence"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +kubebuilder:resource:scope=Cluster,shortName=cdcreservation
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Clique",type=string,JSONPath=`.spec.cliqueID`
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.status) || !has(oldSelf.status.phase) || oldSelf.status.phase == self.status.phase || (oldSelf.status.phase == 'Active' && self.status.phase == 'Released')",message="reservation status may advance only from Active to Released"
// +kubebuilder:validation:XValidation:rule="!has(self.status) || !has(self.status.phase) || self.status.phase != 'Active' || (self.status.snapshotUID != '' && self.status.activationGeneration > 0 && self.status.activationHash.size() == 64)",message="Active reservation status requires a snapshot UID and exact first-activation identity"
// +kubebuilder:validation:XValidation:rule="!has(self.status) || !has(self.status.phase) || self.status.phase != 'Released' || (has(self.status.releasedAt) && self.status.releaseReason in ['NeverPublished', 'VerifiedFence'])",message="Released reservation status requires typed release evidence and timestamp"
// +kubebuilder:validation:XValidation:rule="!has(self.status) || !has(self.status.phase) || self.status.phase != 'Released' || self.status.releaseReason != 'VerifiedFence' || (self.status.snapshotUID != '' && self.status.activationGeneration > 0 && self.status.activationHash.size() == 64 && self.status.fencedGeneration >= self.status.activationGeneration && self.status.fencedHash.size() == 64)",message="verified fence release requires exact activation and fenced snapshot identities"
// +kubebuilder:validation:XValidation:rule="!has(self.status) || !has(self.status.phase) || self.status.phase != 'Released' || self.status.releaseReason != 'NeverPublished' || ((self.status.?snapshotUID.orValue('') == '' && self.status.?activationGeneration.orValue(0) == 0 && self.status.?activationHash.orValue('') == '') || (self.status.?snapshotUID.orValue('') != '' && self.status.?activationGeneration.orValue(0) > 0 && self.status.?activationHash.orValue('').size() == 64)) && self.status.?fencedGeneration.orValue(0) == 0 && self.status.?fencedHash.orValue('') == ''",message="never-published release may retain at most an exact pre-publication activation intent and no fence evidence"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.status) || !has(oldSelf.status.phase) || oldSelf.status.phase != 'Active' || (self.status.snapshotUID == oldSelf.status.snapshotUID && self.status.activationGeneration == oldSelf.status.activationGeneration && self.status.activationHash == oldSelf.status.activationHash)",message="activated snapshot identity is immutable"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.status) || !has(oldSelf.status.phase) || oldSelf.status.phase != 'Released' || self.status == oldSelf.status",message="released reservation evidence is immutable"

// ComputeDomainCliqueReservation is the API-server-atomic ownership record for
// one physical hardware clique. Its name is derived only from cliqueID, so two
// ComputeDomains, controller workers, namespaces, or controller instances must
// race on the same Kubernetes Create. A reservation may be deleted only after
// its status durably records either that no snapshot was ever published or
// that every exact published member supplied verified fence evidence.
type ComputeDomainCliqueReservation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ComputeDomainCliqueReservationSpec   `json:"spec"`
	Status ComputeDomainCliqueReservationStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ComputeDomainCliqueReservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ComputeDomainCliqueReservation `json:"items"`
}

// ComputeDomainCliqueReservationSpec is immutable.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="physical clique reservation is immutable"
type ComputeDomainCliqueReservationSpec struct {
	// +kubebuilder:validation:MinLength=1
	CliqueID string `json:"cliqueID"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="computeDomainUID must not be empty"
	ComputeDomainUID types.UID `json:"computeDomainUID"`
}

type ComputeDomainCliqueReservationStatus struct {
	// Empty means reserved. The controller persists Active before snapshot
	// generation one, and Released before DELETE.
	// +kubebuilder:validation:Enum=Active;Released
	Phase string `json:"phase,omitempty"`
	// +kubebuilder:validation:Enum=NeverPublished;VerifiedFence
	ReleaseReason string    `json:"releaseReason,omitempty"`
	SnapshotUID   types.UID `json:"snapshotUID,omitempty"`
	// ActivationGeneration and ActivationHash identify the proposed first
	// snapshot generation. The controller commits this intent before publishing
	// that generation, so it remains as audit evidence even if the subsequent
	// snapshot status write never commits. It is immutable if later semantic
	// membership changes advance the snapshot generation.
	// +kubebuilder:validation:Minimum=0
	ActivationGeneration int64 `json:"activationGeneration,omitempty"`
	// +kubebuilder:validation:Pattern=`^$|^[a-f0-9]{64}$`
	ActivationHash string `json:"activationHash,omitempty"`
	// FencedGeneration and FencedHash identify the final published generation
	// for which every exact daemon supplied verified fence evidence.
	// +kubebuilder:validation:Minimum=0
	FencedGeneration int64 `json:"fencedGeneration,omitempty"`
	// +kubebuilder:validation:Pattern=`^$|^[a-f0-9]{64}$`
	FencedHash string       `json:"fencedHash,omitempty"`
	ReleasedAt *metav1.Time `json:"releasedAt,omitempty"`
}
