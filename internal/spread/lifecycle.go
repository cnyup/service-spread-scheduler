package spread

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corev1listers "k8s.io/client-go/listers/core/v1"

	configv1alpha1 "github.com/cnyup/service-spread-scheduler/apis/config/v1alpha1"
)

// countablePod reports whether a pod meets the snapshot counting standard
// (design doc 7.1): phase Pending or Running AND spec.nodeName non-empty.
// Unbound Pending pods never affect node counts.
func countablePod(pod *v1.Pod) bool {
	return pod.Spec.NodeName != "" &&
		(pod.Status.Phase == v1.PodPending || pod.Status.Phase == v1.PodRunning)
}

// PodSource abstracts "list every countable pod" for the TTL janitor and the
// reconciler. The production implementation wraps the shared informer's pod
// lister; tests inject fakes.
type PodSource interface {
	// ListCountable returns UID -> ref for every pod meeting the counting
	// standard, plus the node each is bound to.
	ListCountable() (map[types.UID]podRef, error)
}

// stateEvents adapts informer callbacks onto the state machine. It is the
// single entry point for pod Add/Update/Delete events.
type stateEvents struct {
	state *spreadState
}

func newStateEvents(state *spreadState) *stateEvents {
	return &stateEvents{state: state}
}

// OnPodAddOrUpdate maintains the real counts and runs the idempotent release
// path: once the snapshot counts the UID on the reserved node, the
// reservation is dropped — regardless of whether PostBind already ran.
// A brief overcount (snapshot + reservation) before this release is the
// accepted safe direction (dev-design §4.1).
func (e *stateEvents) OnPodAddOrUpdate(pod *v1.Pod, ref podRef) {
	if countablePod(pod) {
		e.state.upsertCount(pod.UID, ref)
	} else {
		e.state.removeCount(pod.UID)
	}
	e.state.releaseIfObserved(pod.UID)
}

// OnPodDelete removes both the real count and any reservation of the UID:
// a deleted pod can no longer consume a seat in either dimension.
func (e *stateEvents) OnPodDelete(uid types.UID) {
	e.state.removeCount(uid)

	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	delete(e.state.resv, uid)
}

// RunJanitor is the ReservationTTL fallback loop. It verifies every expired
// reservation before acting and never deletes blind (design doc 8.3):
//
//   - pod gone or not bound to the reserved node        -> release
//   - pod bound to the reserved node but snapshot lacks
//     the UID (under-count window)                      -> force reconcile,
//     confirm the real count now covers the UID, release
//   - verification impossible (source error)            -> keep + alert
func (s *spreadState) RunJanitor(ctx context.Context, ttl time.Duration, src PodSource, alert func(uid types.UID)) {
	tick := time.NewTicker(janitorTick(ttl))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.janitorOnce(ttl, src, alert)
		}
	}
}

// janitorTick keeps the loop responsive without hot-spinning on tiny TTLs.
func janitorTick(ttl time.Duration) time.Duration {
	if ttl < 5*time.Second {
		return ttl
	}
	if ttl/4 < 5*time.Second {
		return 5 * time.Second
	}
	return ttl / 4
}

// janitorOnce runs one verify-then-act pass; exported state is kept on
// spreadState so tests can drive single passes deterministically.
func (s *spreadState) janitorOnce(ttl time.Duration, src PodSource, alert func(uid types.UID)) {
	now := time.Now()

	s.mu.Lock()
	var expired []types.UID
	for uid, r := range s.resv {
		if now.Sub(r.Since) >= ttl {
			expired = append(expired, uid)
		}
	}
	s.mu.Unlock()
	if len(expired) == 0 {
		return
	}

	// Verification source read happens outside mu: it may list the cluster.
	counts, err := src.ListCountable()
	if err != nil {
		// Verification impossible: keep every reservation and alert.
		for _, uid := range expired {
			alert(uid)
		}
		return
	}

	for _, uid := range expired {
		s.mu.Lock()
		r := s.resv[uid]
		if r == nil { // released concurrently (event path won the race)
			s.mu.Unlock()
			continue
		}
		expectedNode := r.NodeName
		s.mu.Unlock()

		ref, bound := counts[uid]
		if !bound || ref.NodeName != expectedNode {
			// Pod gone or bound elsewhere: the seat is definitely free.
			s.releaseUnverified(uid, expectedNode)
			continue
		}
		// Bound to the expected node but (maybe) missing from the snapshot:
		// force-rebuild from the verified counts, then release only once the
		// rebuilt snapshot really covers the UID.
		s.rebuild(counts)
		s.releaseIfObserved(uid)
		if _, still := s.reservationPhase(uid); still {
			// Should not happen after rebuild; keep + alert rather than
			// risk releasing an unaccounted seat.
			alert(uid)
		}
	}
}

// releaseUnverified drops the reservation when verification proved the pod
// is NOT consuming the seat (missing or on another node).
func (s *spreadState) releaseUnverified(uid types.UID, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.resv[uid]; r != nil {
		delete(s.resv, uid)
	}
}

// RunReconciler periodically rebuilds the count snapshot from the source.
// It only repairs drift and never inspects, ages out or drops reservations:
// reservations always leave through the verified release paths
// (dev-design §4.4).
func (s *spreadState) RunReconciler(ctx context.Context, period time.Duration, src PodSource) {
	tick := time.NewTicker(period)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if counts, err := src.ListCountable(); err == nil {
				s.rebuild(counts)
			}
		}
	}
}

// ---- production wiring (M4) ----

// listerPodSource derives countable pod refs from the shared pod informer
// cache. It is the production PodSource for the janitor and reconciler: it
// applies the same counting standard as the pod event path (countablePod on
// pods of the managed scheduler carrying the service label), so a snapshot
// rebuilt from it covers exactly the UIDs the event path would count.
type listerPodSource struct {
	pods            corev1listers.PodLister
	managedSched    string
	serviceLabelKey string
}

// newListerPodSource adapts a pod lister to PodSource; it is the production
// wiring used by depsFromHandle.
func newListerPodSource(pods corev1listers.PodLister, managedScheduler, serviceLabelKey string) PodSource {
	return &listerPodSource{pods: pods, managedSched: managedScheduler, serviceLabelKey: serviceLabelKey}
}

func (s *listerPodSource) ListCountable() (map[types.UID]podRef, error) {
	all, err := s.pods.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	out := make(map[types.UID]podRef, len(all))
	for _, pod := range all {
		if pod.Spec.SchedulerName != s.managedSched || !countablePod(pod) {
			continue
		}
		ref, err := PodRefFor(pod, s.managedSched, s.serviceLabelKey)
		if err != nil {
			// Managed but unlabeled: never counted, mirroring onPodEvent.
			continue
		}
		out[pod.UID] = ref
	}
	return out, nil
}

// StartLifecycleLoops launches the TTL janitor and the periodic snapshot
// reconciler against one verification source, mirroring RunObserverLoop's
// startup convention (one goroutine per loop, owned by ctx, non-blocking).
// Zero or negative durations fall back to the documented defaults so a
// mis-wired call can never panic time.NewTicker inside a goroutine and take
// the scheduler process down.
func StartLifecycleLoops(
	ctx context.Context,
	state *spreadState,
	src PodSource,
	ttl, period time.Duration,
	alert func(uid types.UID),
) {
	if ttl <= 0 {
		ttl = configv1alpha1.DefaultReservationTTL
	}
	if period <= 0 {
		period = configv1alpha1.DefaultReconcilePeriod
	}
	if alert == nil {
		alert = func(types.UID) {}
	}
	go state.RunJanitor(ctx, ttl, src, alert)
	go state.RunReconciler(ctx, period, src)
}
