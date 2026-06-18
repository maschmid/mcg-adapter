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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConditionOBCCredentialsAvailable = "OBCCredentialsAvailable"
	ConditionBucketNotificationSet   = "BucketNotificationSet"
	ConditionTestEventReceived       = "TestEventReceived"

	FinalizerName = "internal.functions.dev/mcgobctrigger"
)

// MCGOBCTriggerSpec defines the desired state of MCGOBCTrigger.
type MCGOBCTriggerSpec struct {
	// OBC is a reference to the ObjectBucketClaim in the same namespace.
	OBC OBCReference `json:"obc"`
	// Events is the list of S3 event types to subscribe to (e.g. "s3:ObjectCreated:*").
	Events []string `json:"events"`
	// Triggers is the list of endpoints to dispatch matching CloudEvents to.
	Triggers []TriggerTarget `json:"triggers"`
}

type OBCReference struct {
	// Name of the ObjectBucketClaim in the same namespace.
	Name string `json:"name"`
}

type TriggerTarget struct {
	// URI is the absolute URL to send CloudEvents to.
	URI string `json:"uri"`
}

// MCGOBCTriggerStatus defines the observed state of MCGOBCTrigger.
type MCGOBCTriggerStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// MCGOBCTrigger is the Schema for the mcgobctriggers API.
type MCGOBCTrigger struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MCGOBCTriggerSpec   `json:"spec,omitempty"`
	Status MCGOBCTriggerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MCGOBCTriggerList contains a list of MCGOBCTrigger.
type MCGOBCTriggerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MCGOBCTrigger `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MCGOBCTrigger{}, &MCGOBCTriggerList{})
}
