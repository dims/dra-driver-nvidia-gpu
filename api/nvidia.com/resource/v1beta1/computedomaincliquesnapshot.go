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
	ComputeDomainCliqueSnapshotPhasePending  = "Pending"
	ComputeDomainCliqueSnapshotPhaseActive   = "Active"
	ComputeDomainCliqueSnapshotPhaseRetiring = "Retiring"
	ComputeDomainCliqueSnapshotPhaseFenced   = "Fenced"

	ComputeDomainCliqueAssignmentStateBound       = "Bound"
	ComputeDomainCliqueAssignmentStateQuarantined = "Quarantined"
	ComputeDomainCliqueAssignmentStateFenced      = "Fenced"

	// ComputeDomainCliqueSnapshotFinalizer deliberately keeps an allocation
	// tombstone after its DaemonSet and ComputeDomain are gone. The controller
	// may remove it only after the exact published daemons have supplied the
	// evidence required by the retirement protocol; API object disappearance is
	// never sufficient evidence by itself.
	ComputeDomainCliqueSnapshotFinalizer = "resource.nvidia.com/imex-index-fence"

	// ComputeDomainCliqueRetirementReceiptAnnotation is written once by an
	// exact, Pod-bound controller-v1 daemon after its supervised IMEX child has
	// exited and been reaped. The controller validates every published member's
	// receipt before it marks the whole snapshot Fenced.
	ComputeDomainCliqueRetirementReceiptAnnotation = "resource.nvidia.com/computeDomainCliqueRetirementReceipt"
	// ComputeDomainCliqueRetirementFencedAnnotation is a one-shot controller
	// authorization allowing the bound kubelet plugin to clear stale startup
	// topology after every published runtime for the ComputeDomain is fenced.
	ComputeDomainCliqueRetirementFencedAnnotation = "resource.nvidia.com/computeDomainCliqueRetirementFenced"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +kubebuilder:resource:scope=Namespaced,shortName=cdcsnapshot
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Clique",type=string,JSONPath=`.spec.cliqueID`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Members",type=integer,JSONPath=`.status.memberCount`
// +kubebuilder:validation:XValidation:rule="!has(self.status) || !has(self.status.assignments) || self.status.assignments.all(a, a.index < self.spec.capacity)",message="assignment indices must be below snapshot capacity"
// +kubebuilder:validation:XValidation:rule="!has(self.status) || !has(self.status.members) || self.status.members.all(m, m.index < self.spec.capacity)",message="member indices must be below snapshot capacity"
// +kubebuilder:validation:XValidation:rule="!has(self.status) || !has(self.status.assignments) || self.status.assignments.map(a, a.index).size() == self.status.assignments.size()",message="assignment indices must be unique"
// +kubebuilder:validation:XValidation:rule="!has(self.status) || (!has(self.status.members) ? self.status.memberCount == 0 : self.status.memberCount == self.status.members.size())",message="memberCount must equal the member list size"
// +kubebuilder:validation:XValidation:rule="!has(self.status) || !has(self.status.phase) || !(self.status.phase in ['Active', 'Retiring', 'Fenced']) || (self.status.memberCount > 0 && self.status.hash.size() == 64)",message="published and retiring snapshots require members and a SHA-256 hash"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.status) || !has(self.status) || self.status.generation >= oldSelf.status.generation",message="snapshot generation may not decrease"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.status) || !has(oldSelf.status.phase) || oldSelf.status.phase == self.status.phase || (oldSelf.status.phase == 'Pending' && self.status.phase == 'Active') || (oldSelf.status.phase == 'Active' && self.status.phase == 'Retiring') || (oldSelf.status.phase == 'Retiring' && self.status.phase == 'Fenced')",message="snapshot phase transition is invalid"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.status) || !has(oldSelf.status.phase) || !(oldSelf.status.phase in ['Active', 'Retiring']) || !(self.status.phase in ['Retiring', 'Fenced']) || (self.status.generation == oldSelf.status.generation && self.status.hash == oldSelf.status.hash && self.status.members == oldSelf.status.members && self.status.memberCount == oldSelf.status.memberCount)",message="retirement may not change the published generation, hash, or member set"
// +kubebuilder:validation:XValidation:rule="!has(self.status) || !has(self.status.phase) || self.status.phase != 'Fenced' || self.status.assignments.all(a, !a.everPublished || a.state == 'Fenced')",message="Fenced snapshots require every published assignment to be Fenced"

// ComputeDomainCliqueSnapshot is the controller-owned allocation and current
// membership snapshot for one hardware clique in one ComputeDomain. Daemons
// are read-only consumers of this object.
type ComputeDomainCliqueSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ComputeDomainCliqueSnapshotSpec   `json:"spec"`
	Status ComputeDomainCliqueSnapshotStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ComputeDomainCliqueSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ComputeDomainCliqueSnapshot `json:"items"`
}

// ComputeDomainCliqueSnapshotSpec is immutable scope. All frequently changing
// controller-owned data lives in the status subresource.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="snapshot spec is immutable"
type ComputeDomainCliqueSnapshotSpec struct {
	// +kubebuilder:validation:XValidation:rule="self != ''",message="computeDomainUID must not be empty"
	ComputeDomainUID types.UID `json:"computeDomainUID"`
	// +kubebuilder:validation:MinLength=1
	ComputeDomainName string `json:"computeDomainName"`
	// +kubebuilder:validation:MinLength=1
	ComputeDomainNamespace string `json:"computeDomainNamespace"`
	// +kubebuilder:validation:MinLength=1
	CliqueID string `json:"cliqueID"`
	// DaemonSetName and DaemonSetUID identify the only same-namespace workload
	// whose Pods may be admitted into this snapshot. The DaemonSet is also the
	// snapshot's Kubernetes owner; the cross-namespace ComputeDomain identity
	// above remains an explicit, immutable reference rather than an invalid
	// ownerReference.
	// +kubebuilder:validation:MinLength=1
	DaemonSetName string    `json:"daemonSetName"`
	DaemonSetUID  types.UID `json:"daemonSetUID"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1024
	Capacity int `json:"capacity"`
	// +kubebuilder:validation:Enum=controller-v1
	Protocol ComputeDomainCliqueProtocol `json:"protocol"`
}

type ComputeDomainCliqueSnapshotStatus struct {
	// +kubebuilder:validation:Enum=Pending;Active;Retiring;Fenced
	Phase string `json:"phase,omitempty"`
	// Generation increases only when the semantic active membership changes.
	// +kubebuilder:validation:Minimum=0
	Generation int64 `json:"generation,omitempty"`
	// Hash is a SHA-256 digest of the canonical, index-ordered membership.
	// +kubebuilder:validation:Pattern=`^$|^[a-f0-9]{64}$`
	Hash string `json:"hash,omitempty"`
	// +listType=map
	// +listMapKey=nodeUID
	// +kubebuilder:validation:MaxItems=1024
	Assignments []ComputeDomainCliqueAssignment `json:"assignments,omitempty"`
	// +listType=map
	// +listMapKey=index
	// +kubebuilder:validation:MaxItems=1024
	Members []ComputeDomainCliqueMember `json:"members,omitempty"`
	// +kubebuilder:validation:Minimum=0
	MemberCount        int                `json:"memberCount,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.index >= 0",message="assignment index must be non-negative"

// ComputeDomainCliqueAssignment is durable slot ownership. An ever-published
// assignment is retained and quarantined after ambiguous loss; absence and
// elapsed time alone never make it reusable.
// +kubebuilder:validation:XValidation:rule="!oldSelf.everPublished || self.everPublished",message="an existing assignment may not become unpublished"
type ComputeDomainCliqueAssignment struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	NodeName string `json:"nodeName"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="nodeUID must not be empty"
	NodeUID types.UID `json:"nodeUID"`
	// +kubebuilder:validation:Minimum=0
	Index int `json:"index"`
	// +kubebuilder:validation:Enum=Bound;Quarantined;Fenced
	State         string    `json:"state"`
	EverPublished bool      `json:"everPublished"`
	CurrentPodUID types.UID `json:"currentPodUID,omitempty"`
	// +kubebuilder:validation:Minimum=0
	LastAuthorizedGeneration int64 `json:"lastAuthorizedGeneration,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.index >= 0",message="member index must be non-negative"

// ComputeDomainCliqueMember binds one durable assignment to the exact current
// daemon Pod incarnation and address which may consume it.
type ComputeDomainCliqueMember struct {
	// +kubebuilder:validation:Minimum=0
	Index int `json:"index"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	NodeName string `json:"nodeName"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="nodeUID must not be empty"
	NodeUID types.UID `json:"nodeUID"`
	// NodeBootID is the kernel boot epoch observed when this member was
	// published. Empty is accepted only for snapshots created before reboot
	// evidence was introduced; those snapshots cannot use NodeReboot evidence.
	NodeBootID string `json:"nodeBootID,omitempty"`
	// +kubebuilder:validation:MinLength=1
	PodName string `json:"podName"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="podUID must not be empty"
	PodUID types.UID `json:"podUID"`
	// +kubebuilder:validation:Format=ip
	PodIP string `json:"podIP"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="daemonSetUID must not be empty"
	DaemonSetUID types.UID `json:"daemonSetUID"`
}

// ComputeDomainCliqueSnapshotReceipt is written atomically into the existing
// per-ComputeDomain node-local /imexd directory after a daemon installs a
// snapshot and starts (or restarts) its IMEX child, then observes that new
// process READY. The kubelet plugin checks the receipt
// together with PodReady before releasing a workload.
type ComputeDomainCliqueSnapshotReceipt struct {
	ComputeDomainUID   types.UID `json:"computeDomainUID"`
	SnapshotUID        types.UID `json:"snapshotUID"`
	SnapshotGeneration int64     `json:"snapshotGeneration"`
	SnapshotHash       string    `json:"snapshotHash"`
	NodeUID            types.UID `json:"nodeUID"`
	PodUID             types.UID `json:"podUID"`
	Index              int       `json:"index"`
}

// ComputeDomainCliqueRetirementReceipt is durable process-exit evidence from
// one exact daemon Pod. It is published only after the daemon has stopped and
// reaped the IMEX child authorized by the referenced snapshot. Receipt
// absence, Pod disappearance, timeout, and a new Pod UID are not equivalent
// evidence.
type ComputeDomainCliqueRetirementReceipt struct {
	ComputeDomainUID   types.UID `json:"computeDomainUID"`
	SnapshotUID        types.UID `json:"snapshotUID"`
	SnapshotGeneration int64     `json:"snapshotGeneration"`
	SnapshotHash       string    `json:"snapshotHash"`
	NodeUID            types.UID `json:"nodeUID"`
	PodUID             types.UID `json:"podUID"`
	Index              int       `json:"index"`
}
