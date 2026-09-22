package spread

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// EventsToRegister implements EnqueueExtensions (dev-design §5.7):
//
//   - Pod Add/Update/Delete: counts changed → retry only same-service pods
//     (same schedKey or quotaKey). Delete is mandatory: HPA scaledown and
//     evictions free quota that pending pods of the service depend on.
//   - Node Add/Update/Delete: domain membership may change for any pod;
//     conservatively Queue (cheap membership tests are not reliable).
//   - ServiceSpreadPolicy / ReplicaSet / Deployment events: policy and
//     owner-chain completion retry same-namespace pods.
//
// HPA/ScaledObject are deliberately not registered: replica targets never
// drive placement.
func (pl *ServiceSpread) EventsToRegister() []framework.ClusterEventWithHint {
	return []framework.ClusterEventWithHint{
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Add | framework.Update | framework.Delete},
			QueueingHintFn: pl.podHint},
		{Event: framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add | framework.Update | framework.Delete}},
		{Event: framework.ClusterEvent{Resource: policyEventGVK, ActionType: framework.Add | framework.Update | framework.Delete},
			QueueingHintFn: pl.policyHint},
		{Event: framework.ClusterEvent{Resource: replicaSetEventGVK, ActionType: framework.Add | framework.Update | framework.Delete},
			QueueingHintFn: sameNamespaceHint},
		{Event: framework.ClusterEvent{Resource: deploymentEventGVK, ActionType: framework.Add | framework.Update | framework.Delete},
			QueueingHintFn: sameNamespaceHint},
	}
}

// Custom GVKs use the scheduler's 3-folded dynamic-informer format
// `<plural>.<version>.<group>` (eventhandlers.go): the policy CRD plus the
// owner-chain resources.
var (
	policyEventGVK     = framework.GVK(policyGVR.Resource + "." + policyGVR.Version + "." + policyGVR.Group)
	replicaSetEventGVK = framework.GVK("replicasets.v1.apps")
	deploymentEventGVK = framework.GVK("deployments.v1.apps")
)

// objPod extracts a pod from a hint object.
func objPod(obj interface{}) *v1.Pod {
	switch o := obj.(type) {
	case *v1.Pod:
		return o
	default:
		return nil
	}
}

// podHint queues only when the event pod shares the schedKey (same domain)
// or quotaKey (same service) of the pending pod — i.e. when it can have
// changed the counts that rejected the pending pod.
func (pl *ServiceSpread) podHint(_ klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) framework.QueueingHint {
	evtPod := objPod(newObj)
	if evtPod == nil {
		evtPod = objPod(oldObj)
	}
	if evtPod == nil || pod == nil {
		return framework.QueueAfterBackoff
	}
	if evtPod.Spec.SchedulerName != pl.args.ManagedSchedulerName {
		return framework.QueueSkip
	}
	ref, err := PodRefFor(evtPod, pl.args.ManagedSchedulerName, pl.args.ServiceLabelKey)
	if err != nil {
		return framework.QueueSkip
	}
	podRef, err := PodRefFor(pod, pl.args.ManagedSchedulerName, pl.args.ServiceLabelKey)
	if err != nil {
		// Pending pod already rejected for the missing label; nothing here
		// can fix it (label changes trigger a Pod Update event anyway).
		return framework.QueueSkip
	}
	if ref.SchedKey == podRef.SchedKey || ref.QuotaKey == podRef.QuotaKey {
		return framework.QueueAfterBackoff
	}
	return framework.QueueSkip
}

// policyHint queues when the policy lives in the pending pod's namespace
// and selects its service label value. The object arrives as
// *unstructured.Unstructured (custom GVK); anything unexpected fails open
// to Queue (retry beats dead-wait).
func (pl *ServiceSpread) policyHint(_ klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) framework.QueueingHint {
	obj := newObj
	if obj == nil {
		obj = oldObj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok || pod == nil {
		return framework.QueueAfterBackoff
	}
	if u.GetNamespace() != pod.Namespace {
		return framework.QueueSkip
	}
	schedName, sel := policySpec(u)
	if schedName == "" || sel == nil {
		return framework.QueueAfterBackoff
	}
	if schedName != pl.args.ManagedSchedulerName {
		return framework.QueueSkip
	}
	labelValue, ok := pod.Labels[pl.args.ServiceLabelKey]
	if !ok {
		return framework.QueueSkip
	}
	if len(sel) == 1 && sel[pl.args.ServiceLabelKey] == labelValue {
		return framework.QueueAfterBackoff
	}
	return framework.QueueSkip
}

// policySpec extracts (schedulerName, serviceSelector.matchLabels) from an
// unstructured ServiceSpreadPolicy.
func policySpec(u *unstructured.Unstructured) (string, map[string]string) {
	spec, ok := u.Object["spec"].(map[string]interface{})
	if !ok {
		return "", nil
	}
	sched, _, _ := unstructured.NestedString(spec, "schedulerName")
	sel, _, _ := unstructured.NestedStringMap(spec, "serviceSelector", "matchLabels")
	return sched, sel
}

// sameNamespaceHint queues same-namespace pod retries for RS/Deployment
// events (owner-chain completion and cleanup).
func sameNamespaceHint(_ klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) framework.QueueingHint {
	obj := newObj
	if obj == nil {
		obj = oldObj
	}
	var ns string
	switch o := obj.(type) {
	case *unstructured.Unstructured:
		ns = o.GetNamespace()
	case metav1.Object:
		ns = o.GetNamespace()
	case runtime.Object:
		if mo, ok := o.(metav1.Object); ok {
			ns = mo.GetNamespace()
		}
	default:
		return framework.QueueAfterBackoff
	}
	if pod == nil || ns == "" {
		return framework.QueueAfterBackoff
	}
	if ns == pod.Namespace {
		return framework.QueueAfterBackoff
	}
	return framework.QueueSkip
}
