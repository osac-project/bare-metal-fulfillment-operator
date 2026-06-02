/*
Copyright 2026.

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

package v1alpha1

import (
	"strings"

	opv1alpha1 "github.com/osac-project/osac-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HostLeaseSpec defines the desired state of HostLease.
type HostLeaseSpec struct {
	// HostType is the resource class/type of the host.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="field is immutable"
	HostType string `json:"hostType"`
	// ExternalHostID is the host ID from external inventory (used by Host Management Operator as node identifier).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Type=string
	ExternalHostID string `json:"externalHostID"`
	// ExternalHostName is the host name from external inventory.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Type=string
	ExternalHostName string `json:"externalHostName,omitempty"`
	// HostClass is host management backend class (e.g. openstack).
	HostClass string `json:"hostClass,omitempty"`
	// NetworkClass is the network class for this host (e.g. openstack).
	NetworkClass string `json:"networkClass,omitempty"`
	// Selector defines additional host selection filters.
	// hostSelector accepts arbitrary key/value selectors such as managedBy or topology.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="field is immutable"
	Selector HostSelectorSpec `json:"selector,omitempty"`
	// InventoryLabels are labels to be applied to the host in the inventory system.
	// These labels are non-persistent and will be removed when the HostLease is deleted.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="field is immutable"
	InventoryLabels map[string]string `json:"inventorylabels,omitempty"`
	// InventoryPersistentLabels are labels to be applied to the host in the inventory system.
	// These labels are persistent and will remain on the host after the HostLease is deleted.
	// Persistent labels override InventoryLabels with the same key.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="field is immutable"
	InventoryPersistentLabels map[string]string `json:"inventorypersistentlabels,omitempty"`
	// TemplateID is the unique identifier of the host template to use.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern=^[a-zA-Z_][a-zA-Z0-9._]*$
	TemplateID string `json:"templateID"`
	// TemplateParameters is a JSON-encoded map of the parameter values for the
	// selected host template.
	// +kubebuilder:validation:Optional
	TemplateParameters string `json:"templateParameters,omitempty"`
	// PoweredOn is the desired power state (true = powered on, false = powered off, nil = unmanaged).
	// When nil, the operator will not attempt to change the host's power state (useful for adopting existing hosts).
	// This represents the user's intent; the actual observed state is in status.poweredOn.
	// +kubebuilder:validation:Optional
	PoweredOn *bool `json:"poweredOn,omitempty"`
}

// HostLeasePhaseType is a valid value for .status.phase
type HostLeasePhaseType string

const (
	// HostLeasePhaseAllocating means searching for a free host to allocate
	HostLeasePhaseAllocating HostLeasePhaseType = "Allocating"

	// HostLeasePhaseProgressing means the host is being worked on (allocating, provisioning, power changes, etc.)
	HostLeasePhaseProgressing HostLeasePhaseType = "Progressing"

	// HostLeasePhaseReady means the host is ready and stable
	HostLeasePhaseReady HostLeasePhaseType = "Ready"

	// HostLeasePhaseFailed means reconciliation has failed
	HostLeasePhaseFailed HostLeasePhaseType = "Failed"

	// HostLeasePhaseDeleting means the resource is being deleted
	HostLeasePhaseDeleting HostLeasePhaseType = "Deleting"
)

// HostLeaseConditionType is a valid value for .status.conditions.type
type HostLeaseConditionType string

const (
	// HostConditionAllocated means the host has been allocated.
	HostConditionAllocated HostLeaseConditionType = "Allocated"

	// HostConditionAvailable means the host is available for provisioning.
	HostConditionAvailable HostLeaseConditionType = "Available"

	// HostConditionPowerSynced tracks the host power synchronization state.
	// Set condition status to True and reason to PowerOn when power on is successful.
	// Set condition status to True and reason to PowerOff when power off is successful.
	// Set condition status to False and reason to IronicAPIFailure when the operation fails.
	HostConditionPowerSynced HostLeaseConditionType = "PowerSynced"

	// HostConditionProvisionTemplateComplete tracks provision template completion.
	// Set condition status True on success.
	// Set condition status False with reason Progressing or TemplateFailed while not complete.
	HostConditionProvisionTemplateComplete HostLeaseConditionType = "ProvisionTemplateComplete"

	// HostConditionDeprovisionTemplateComplete tracks deprovision template completion.
	// Set condition status True on success.
	// Set condition status False with reason Progressing or TemplateFailed while not complete.
	HostConditionDeprovisionTemplateComplete HostLeaseConditionType = "DeprovisionTemplateComplete"
)

// Host condition reason values
const (
	// HostConditionReasonProgressing indicates the template workflow is still running.
	HostConditionReasonProgressing = "Progressing"

	// HostConditionReasonTemplateFailed indicates the template workflow failed.
	HostConditionReasonTemplateFailed = "TemplateFailed"

	// HostConditionReasonPowerOn indicates the host is powered on successfully.
	HostConditionReasonPowerOn = "PowerOn"

	// HostConditionReasonPowerOff indicates the host is powered off successfully.
	HostConditionReasonPowerOff = "PowerOff"

	// HostConditionReasonIronicAPIFailure indicates a power operation failed due to Ironic API error.
	HostConditionReasonIronicAPIFailure = "IronicAPIFailure"
)

// HostSelectorSpec defines additional host selection constraints.
type HostSelectorSpec struct {
	// HostSelector is a map of arbitrary selector key/value pairs
	// (for example managedBy, topology, rack, zone).
	// +kubebuilder:validation:Optional
	HostSelector map[string]string `json:"hostSelector,omitempty"`
}

// HostLeaseStatus defines the observed state of HostLease.
type HostLeaseStatus struct {
	// Phase provides a single-value overview of the state of the HostLease
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Enum=Allocating;Progressing;Ready;Failed;Deleting
	Phase HostLeasePhaseType `json:"phase,omitempty"`
	// Jobs tracks the history of provision and deprovision operations
	// Ordered chronologically, with latest operations at the end
	// Limited to the last N jobs (configurable via OSAC_MAX_JOB_HISTORY, default 10)
	// +kubebuilder:validation:Optional
	Jobs []opv1alpha1.JobStatus `json:"jobs,omitempty"`
	// Conditions holds an array of metav1.Condition describing host state.
	// +kubebuilder:validation:Optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
	// DesiredConfigVersion is a hash of the spec, used to detect spec changes and control retry behavior.
	// +kubebuilder:validation:Optional
	DesiredConfigVersion string `json:"desiredConfigVersion,omitempty"`
	// PoweredOn is the current power state.
	PoweredOn *bool `json:"poweredOn,omitempty"`
}

// GetPoolID returns the owning BareMetalPool UID if the HostLease is owned by a BareMetalPool.
func (h *HostLease) GetPoolID() (string, bool) {
	for _, ownerReference := range h.OwnerReferences {
		if ownerReference.Controller == nil || !*ownerReference.Controller {
			continue
		}
		if strings.Contains(ownerReference.APIVersion, "osac.openshift.io") && ownerReference.Kind == "BareMetalPool" {
			return string(ownerReference.UID), true
		}
	}
	return "", false
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=hl
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="HostType",type=string,JSONPath=`.spec.hostType`
// +kubebuilder:printcolumn:name="Template",type=string,JSONPath=`.spec.templateID`
// +kubebuilder:printcolumn:name="HostClass",type=string,JSONPath=`.spec.hostClass`
// +kubebuilder:printcolumn:name="NetworkClass",type=string,JSONPath=`.spec.networkClass`
// +kubebuilder:printcolumn:name="ExternalHostID",type=string,JSONPath=`.spec.externalHostID`

// HostLease is the Schema for the hostleases API.
type HostLease struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HostLeaseSpec   `json:"spec,omitempty"`
	Status HostLeaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HostLeaseList contains a list of HostLease.
type HostLeaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HostLease `json:"items"`
}

func init() {
	SchemeBuilder.Register(&HostLease{}, &HostLeaseList{})
}
