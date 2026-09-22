// +kubebuilder:object:generate=true
// +groupName=scheduling.soyup.top

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group/version of the ServiceSpreadPolicy CRD API.
	GroupVersion = schema.GroupVersion{Group: "scheduling.soyup.top", Version: "v1alpha1"}

	// SchemeBuilder registers this package's types into a scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds api/v1alpha1 types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&ServiceSpreadPolicy{}, &ServiceSpreadPolicyList{})
}
