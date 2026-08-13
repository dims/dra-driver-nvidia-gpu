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

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +kubebuilder:resource:scope=Cluster,shortName=cdcreservation
// +kubebuilder:printcolumn:name="Clique",type=string,JSONPath=`.spec.cliqueID`
// +kubebuilder:printcolumn:name="ComputeDomain",type=string,JSONPath=`.spec.computeDomainName`

// ComputeDomainCliqueReservation is the API-server-atomic ownership record for
// one physical hardware clique. Its name is derived only from cliqueID, so two
// ComputeDomains, controller workers, namespaces, or controller instances must
// race on the same Kubernetes Create. Strict v1 never deletes a reservation:
// Kubernetes object absence cannot prove that an old IMEX runtime is fenced.
type ComputeDomainCliqueReservation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ComputeDomainCliqueReservationSpec `json:"spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ComputeDomainCliqueReservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ComputeDomainCliqueReservation `json:"items"`
}

// ComputeDomainCliqueReservationSpec is immutable. A future verified fence
// protocol may introduce an explicit handoff object; it must not mutate or
// silently recycle this record.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="physical clique reservation is immutable"
type ComputeDomainCliqueReservationSpec struct {
	// +kubebuilder:validation:MinLength=1
	CliqueID string `json:"cliqueID"`
	// +kubebuilder:validation:XValidation:rule="self != ''",message="computeDomainUID must not be empty"
	ComputeDomainUID types.UID `json:"computeDomainUID"`
	// +kubebuilder:validation:MinLength=1
	ComputeDomainName string `json:"computeDomainName"`
	// +kubebuilder:validation:MinLength=1
	ComputeDomainNamespace string `json:"computeDomainNamespace"`
	// +kubebuilder:validation:Enum=controller-v1
	Protocol ComputeDomainCliqueProtocol `json:"protocol"`
}
