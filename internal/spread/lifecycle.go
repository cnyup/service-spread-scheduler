package spread

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
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
