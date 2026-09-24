package spread

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	listersappsv1 "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	schedulingv1alpha1 "github.com/cnyup/service-spread-scheduler/api/v1alpha1"
	configv1alpha1 "github.com/cnyup/service-spread-scheduler/apis/config/v1alpha1"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Name is the plugin name used in scheduler profiles
// (config/scheduler/kubeconfig.yaml).
const Name = "ServiceSpread"

// Factory wiring (k8s 1.28 signatures). The framework types in this file
// and plugin.go are the only 1.28-specific surface; on a version bump start
// here (dev-design §5.1: plugins_api.go is the isolation layer).

// PolicyReader abstracts policy lookup for the plugin and tests.
type PolicyReader interface {
	// List returns every ServiceSpreadPolicy of one namespace.
	List(namespace string) ([]*schedulingv1alpha1.ServiceSpreadPolicy, error)
	HasSynced() bool
}

// OwnerReader resolves the Pod→ReplicaSet→Deployment ownership chain.
type OwnerReader interface {
	// OwnedByDeployment reports whether the pod descends from a Deployment
	// via a controlling ReplicaSet (both looked up in informer caches).
	OwnedByDeployment(podUID, namespace string, podOwnerRefs []metav1.OwnerReference) bool
	HasSynced() bool
}

// New is the plugin factory registered via app.WithPlugin(spread.Name,
// spread.New) in cmd/scheduler. configuration is ServiceSpreadArgs decoded
// strictly by apis/config.DecodeServiceSpreadArgs.
func New(ctx context.Context, configuration runtime.Object, h framework.Handle) (framework.Plugin, error) {
	args, err := configv1alpha1.DecodeServiceSpreadArgs(configuration)
	if err != nil {
		return nil, fmt.Errorf("decode %s args: %w", Name, err)
	}
	if args == nil {
		return nil, fmt.Errorf("%s: nil args", Name)
	}
	argsValue := *args
	return NewWithDeps(argsValue, depsFromHandle(ctx, argsValue, h))
}

// pluginDeps carries every external dependency of the plugin; the split
// keeps plugin.go free of framework.Handle so unit tests need no fake
// framework.
type pluginDeps struct {
	policies PolicyReader
	owners   OwnerReader
	nodes    NodeReader
	state    *spreadState
	events   *stateEvents
	// synced gates PreFilter on the placement-critical caches
	// (pods/nodes/policies — dev-design §5.2 step 0).
	synced func() bool
}

func depsFromHandle(ctx context.Context, args configv1alpha1.ServiceSpreadArgs, h framework.Handle) pluginDeps {
	sif := h.SharedInformerFactory()

	policies := newPolicyInformer(ctx, h.KubeConfig())
	nodes := NewNodeReader(sif.Core().V1().Nodes().Lister())
	owners := newRSOwnerReader(
		sif.Apps().V1().ReplicaSets().Lister(),
		sif.Apps().V1().Deployments().Lister(),
	)

	st := newSpreadState()
	events := newStateEvents(st)

	// Wire pod informer events onto the state machine. Only pods of the
	// managed scheduler (with the service label) enter the counts; every
	// other pod just runs the idempotent release path.
	podInformer := sif.Core().V1().Pods().Informer()
	_, _ = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*v1.Pod); ok {
				onPodEvent(events, args, pod)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if pod, ok := newObj.(*v1.Pod); ok {
				onPodEvent(events, args, pod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			switch p := obj.(type) {
			case *v1.Pod:
				events.OnPodDelete(p.UID)
			case cache.DeletedFinalStateUnknown:
				if pod, ok := p.Obj.(*v1.Pod); ok {
					events.OnPodDelete(pod.UID)
				}
			}
		},
	})

	synced := func() bool {
		return policies.HasSynced() && podInformer.HasSynced() &&
			sif.Core().V1().Nodes().Informer().HasSynced()
	}

	// Observer loop (M4): metrics-only goroutine sharing the same
	// informers (dev-design §9 deviation 4). HPA informer starts via the
	// shared factory; KEDA dynamic informer only when the group is in
	// discovery.
	hpaInform := sif.Autoscaling().V2().HorizontalPodAutoscalers()
	targets := newInformerTargetSource(
		hpaInform.Lister(),
		hpaInform.Informer(),
		func() (cache.Indexer, cache.InformerSynced, bool) {
			return newKEDAIndexer(ctx, h.KubeConfig())
		},
	)
	go RunObserverLoop(ctx, ObserverLoopDeps{
		Deployments:          sif.Apps().V1().Deployments().Lister(),
		Pods:                 sif.Core().V1().Pods().Lister(),
		TargetSource:         targets,
		ServiceLabelKey:      args.ServiceLabelKey,
		ExportInterval:       30 * time.Second,
		Nodes:                nodes,
		Policies:             policies,
		ManagedSchedulerName: args.ManagedSchedulerName,
	})

	// Reservation TTL janitor + snapshot reconciler (M4): both read the
	// shared pod informer cache as the verification source, same startup
	// convention as the observer loop above (dev-design §4.4/§8.3).
	StartLifecycleLoops(ctx, st,
		newListerPodSource(sif.Core().V1().Pods().Lister(), args.ManagedSchedulerName, args.ServiceLabelKey),
		args.ReservationTTL.Duration, args.ReconcilePeriod.Duration,
		func(uid PodUID) {
			klog.Background().Info(
				"ServiceSpread janitor kept a reservation after an inconclusive verification",
				"podUID", string(uid))
		})

	return pluginDeps{
		policies: policies,
		owners:   owners,
		nodes:    nodes,
		state:    st,
		events:   events,
		synced:   synced,
	}
}

// onPodEvent routes one pod object through the state machine.
func onPodEvent(e *stateEvents, args configv1alpha1.ServiceSpreadArgs, pod *v1.Pod) {
	if pod.Spec.SchedulerName == args.ManagedSchedulerName {
		if ref, err := PodRefFor(pod, args.ManagedSchedulerName, args.ServiceLabelKey); err == nil {
			e.OnPodAddOrUpdate(pod, ref)
			return
		}
	}
	// Unmanaged (or label-less) pods never count; still run the release
	// path in case a reservation lingers from a former configuration.
	e.OnPodAddOrUpdate(pod, podRef{})
}

// ---- policy informer (dynamic, converted to typed) ----

var policyGVR = schema.GroupVersionResource{
	Group:    schedulingv1alpha1.GroupVersion.Group,
	Version:  schedulingv1alpha1.GroupVersion.Version,
	Resource: "servicespreadpolicies",
}

type policyInformer struct {
	inf    cache.SharedIndexInformer
	synced cache.InformerSynced
}

func newPolicyInformer(ctx context.Context, cfg *rest.Config) *policyInformer {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		// A misconfigured kubeconfig keeps HasSynced()==false: PreFilter
		// then rejects with CacheNotSynced instead of crashing the
		// scheduler.
		return &policyInformer{}
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, 30*time.Minute, metav1.NamespaceAll, nil)
	inf := factory.ForResource(policyGVR).Informer()
	go inf.Run(ctx.Done())
	return &policyInformer{inf: inf, synced: inf.HasSynced}
}

func (p *policyInformer) HasSynced() bool {
	if p.synced == nil {
		return false
	}
	return p.synced()
}

func (p *policyInformer) List(namespace string) ([]*schedulingv1alpha1.ServiceSpreadPolicy, error) {
	if p.inf == nil {
		return nil, fmt.Errorf("policy informer unavailable")
	}
	objs, err := p.inf.GetIndexer().ByIndex(cache.NamespaceIndex, namespace)
	if err != nil {
		return nil, err
	}
	out := make([]*schedulingv1alpha1.ServiceSpreadPolicy, 0, len(objs))
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		pol := &schedulingv1alpha1.ServiceSpreadPolicy{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.UnstructuredContent(), pol); err != nil {
			klog.Background().Error(err, "converting ServiceSpreadPolicy", "name", u.GetName())
			continue
		}
		out = append(out, pol)
	}
	return out, nil
}

// newKEDAIndexer builds (indexer, synced, known) for ScaledObjects. known
// is false (and the rest nil) when the keda.sh group is absent from
// discovery — the observer then reports "not managed" without error. The
// dynamic informer runs for the process lifetime, mirroring the policy
// informer pattern above.
func newKEDAIndexer(ctx context.Context, cfg *rest.Config) (cache.Indexer, cache.InformerSynced, bool) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, false
	}
	groups, err := dyn.Resource(scaledObjectGVR).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		// Not registered (or no RBAC): treat as absent; degraded-vs-absent
		// refinement lands with the M4 alerting pass.
		return nil, nil, false
	}
	_ = groups
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, 30*time.Minute, metav1.NamespaceAll, nil)
	inf := factory.ForResource(scaledObjectGVR).Informer()
	go inf.Run(ctx.Done())
	return inf.GetIndexer(), inf.HasSynced, true
}

// ---- owner chain reader ----

type rsOwnerReader struct {
	rs  listersappsv1.ReplicaSetLister
	dep listersappsv1.DeploymentLister
}

func newRSOwnerReader(rs listersappsv1.ReplicaSetLister, dep listersappsv1.DeploymentLister) OwnerReader {
	return &rsOwnerReader{rs: rs, dep: dep}
}

func (r *rsOwnerReader) HasSynced() bool { return true } // cache-backed listers; misses surface as resolution failure

func (r *rsOwnerReader) OwnedByDeployment(_ string, namespace string, podOwnerRefs []metav1.OwnerReference) bool {
	rsRef := controllerRef(podOwnerRefs, "ReplicaSet")
	if rsRef == nil {
		return false
	}
	rs, err := r.rs.ReplicaSets(namespace).Get(rsRef.Name)
	if err != nil {
		return false
	}
	depRef := controllerRef(rs.OwnerReferences, "Deployment")
	if depRef == nil {
		return false
	}
	if _, err := r.dep.Deployments(namespace).Get(depRef.Name); err != nil {
		return false
	}
	return true
}

func controllerRef(refs []metav1.OwnerReference, kind string) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Kind == kind && refs[i].Controller != nil && *refs[i].Controller {
			return &refs[i]
		}
	}
	return nil
}
