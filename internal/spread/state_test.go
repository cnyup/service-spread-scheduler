package spread

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type fakeSource struct {
	mu     sync.Mutex
	pods   map[types.UID]podRef
	hasErr error
}

func (f *fakeSource) ListCountable() (map[types.UID]podRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hasErr != nil {
		return nil, f.hasErr
	}
	out := make(map[types.UID]podRef, len(f.pods))
	for k, v := range f.pods {
		out[k] = v
	}
	return out, nil
}

func refFor(node, quota, sched string) podRef {
	return podRef{QuotaKey: quota, SchedKey: sched, NodeName: node}
}

func boundPod(uid, node string, phase v1.PodPhase) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: uid, Namespace: "ns"},
		Spec:       v1.PodSpec{NodeName: node},
		Status:     v1.PodStatus{Phase: phase},
	}
}

// ---- COW snapshot consistency ----

func TestSnapshotCOWConsistency(t *testing.T) {
	s := newSpreadState()
	ev := newStateEvents(s)

	ev.OnPodAddOrUpdate(boundPod("u1", "n1", v1.PodRunning), refFor("n1", "q1", "d1"))
	ev.OnPodAddOrUpdate(boundPod("u2", "n1", v1.PodRunning), refFor("n1", "q1", "d1"))
	ev.OnPodAddOrUpdate(boundPod("u3", "n2", v1.PodPending), refFor("n2", "q1", "d1"))

	domainN, quotaN := s.effCount("n1", "d1", "q1")
	if domainN != 2 || quotaN != 2 {
		t.Fatalf("n1 eff = (%d,%d), want (2,2)", domainN, quotaN)
	}
	domainN, quotaN = s.effCount("n2", "d1", "q1")
	if domainN != 1 || quotaN != 1 {
		t.Fatalf("n2 eff = (%d,%d), want (1,1)", domainN, quotaN)
	}
	// Different schedKey shares quota but not domain count.
	domainN, quotaN = s.effCount("n2", "d2", "q1")
	if domainN != 0 || quotaN != 1 {
		t.Fatalf("n2 eff(d2) = (%d,%d), want (0,1)", domainN, quotaN)
	}

	// Update moves the pod: old node counts must drop.
	ev.OnPodAddOrUpdate(boundPod("u1", "n2", v1.PodRunning), refFor("n2", "q1", "d1"))
	if domainN, _ := s.effCount("n1", "d1", "q1"); domainN != 1 {
		t.Fatalf("n1 domain count after move = %d, want 1", domainN)
	}
	// Delete removes it entirely.
	ev.OnPodDelete("u1")
	if _, quotaN := s.effCount("n2", "d1", "q1"); quotaN != 1 {
		t.Fatalf("n2 quota count after delete = %d, want 1 (u3 only)", quotaN)
	}
}

func TestUnboundPodNotCounted(t *testing.T) {
	s := newSpreadState()
	ev := newStateEvents(s)
	unbound := boundPod("u1", "", v1.PodPending)
	ev.OnPodAddOrUpdate(unbound, refFor("", "q", "d"))
	if _, quotaN := s.effCount("", "d", "q"); quotaN != 0 {
		t.Fatalf("unbound pod must not count, got %d", quotaN)
	}
	// Phase outside {Pending, Running} never counts either.
	succeeded := boundPod("u1", "n1", v1.PodSucceeded)
	ev.OnPodAddOrUpdate(succeeded, refFor("n1", "q", "d"))
	if _, quotaN := s.effCount("n1", "d", "q"); quotaN != 0 {
		t.Fatalf("Succeeded pod must not count, got %d", quotaN)
	}
}

// ---- reservation lifecycle ----

func TestReserveBindObserveReleaseChain(t *testing.T) {
	s := newSpreadState()
	ev := newStateEvents(s)
	r := refFor("n1", "q1", "d1")

	if !s.TryReserve("u1", r, func(d, q int32) bool { return true }) {
		t.Fatal("reserve failed")
	}
	domainN, quotaN := s.effCount("n1", "d1", "q1")
	if domainN != 1 || quotaN != 1 {
		t.Fatalf("eff after reserve = (%d,%d), want (1,1)", domainN, quotaN)
	}
	if _, ok := s.reservationPhase("u1"); !ok {
		t.Fatal("reservation missing")
	}

	// Duplicate reserve for same UID must be refused.
	if s.TryReserve("u1", r, func(d, q int32) bool { return true }) {
		t.Fatal("duplicate reserve accepted")
	}

	// Admission-failing check must not write state.
	if s.TryReserve("u2", r, func(d, q int32) bool { return false }) {
		t.Fatal("reserve accepted with failing check")
	}
	if s.reservationsLen() != 1 {
		t.Fatalf("reservations = %d, want 1", s.reservationsLen())
	}

	s.MarkBound("u1")
	if ph, _ := s.reservationPhase("u1"); ph != resvBoundObs {
		t.Fatalf("phase = %v, want BoundObs", ph)
	}
	// MarkBound is not a release.
	if s.reservationsLen() != 1 {
		t.Fatal("MarkBound must not release")
	}

	// Informer observes the binding on the expected node -> release.
	ev.OnPodAddOrUpdate(boundPod("u1", "n1", v1.PodRunning), r)
	if s.reservationsLen() != 0 {
		t.Fatalf("reservation not released after observed bind, len=%d", s.reservationsLen())
	}
	// Real count remains.
	if _, quotaN := s.effCount("n1", "d1", "q1"); quotaN != 1 {
		t.Fatalf("real count lost after release: %d", quotaN)
	}
}

func TestUnreserveReleases(t *testing.T) {
	s := newSpreadState()
	r := refFor("n1", "q", "d")
	s.TryReserve("u1", r, func(int32, int32) bool { return true })
	s.Unreserve("u1")
	if s.reservationsLen() != 0 {
		t.Fatal("Unreserve must release a Reserved reservation")
	}
	// Unreserve on BoundObs is a no-op (only verified paths release).
	s.TryReserve("u1", r, func(int32, int32) bool { return true })
	s.MarkBound("u1")
	s.Unreserve("u1")
	if s.reservationsLen() != 1 {
		t.Fatal("Unreserve must not release a BoundObs reservation")
	}
}

func TestBindEventBeforePostBindReleasesDirectly(t *testing.T) {
	s := newSpreadState()
	ev := newStateEvents(s)
	r := refFor("n1", "q", "d")
	s.TryReserve("u1", r, func(int32, int32) bool { return true })
	// Bind event arrives while phase is still Reserved: release immediately.
	ev.OnPodAddOrUpdate(boundPod("u1", "n1", v1.PodRunning), r)
	if s.reservationsLen() != 0 {
		t.Fatal("reserved-phase bind observation must release")
	}
	// A late MarkBound for a gone reservation is a harmless no-op.
	s.MarkBound("u1")
}

func TestDoubleCountDirectionOnBindObservation(t *testing.T) {
	s := newSpreadState()
	ev := newStateEvents(s)
	r := refFor("n1", "q", "d")
	s.TryReserve("u1", r, func(int32, int32) bool { return true })
	// Snapshot counts the pod but the reservation has not been released yet
	// (narrow window inside OnPodAddOrUpdate, simulated by manual upsert).
	s.upsertCount("u1", r)
	domainN, quotaN := s.effCount("n1", "d", "q")
	if domainN != 2 || quotaN != 2 {
		t.Fatalf("window must OVERestimate: (%d,%d), want (2,2)", domainN, quotaN)
	}
	ev.OnPodAddOrUpdate(boundPod("u1", "n1", v1.PodRunning), r) // idempotent release path
	if domainN, _ := s.effCount("n1", "d", "q"); domainN != 1 {
		t.Fatalf("after release = %d, want 1", domainN)
	}
}

// ---- TTL janitor branches ----

func ageReservation(s *spreadState, uid types.UID, age time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.resv[uid]; r != nil {
		r.Since = time.Now().Add(-age)
	}
}

func TestJanitorReleasesWhenPodGoneOrMoved(t *testing.T) {
	s := newSpreadState()
	src := &fakeSource{pods: map[types.UID]podRef{}}
	alerts := 0
	alert := func(types.UID) { alerts++ }

	// Pod gone entirely.
	s.TryReserve("u1", refFor("n1", "q", "d"), func(int32, int32) bool { return true })
	ageReservation(s, "u1", 10*time.Minute)
	s.janitorOnce(time.Minute, src, alert)
	if s.reservationsLen() != 0 {
		t.Fatal("vanished pod reservation must be released")
	}

	// Pod exists but bound to another node.
	s.TryReserve("u2", refFor("n1", "q", "d"), func(int32, int32) bool { return true })
	ageReservation(s, "u2", 10*time.Minute)
	src.pods["u2"] = refFor("n9", "q", "d")
	s.janitorOnce(time.Minute, src, alert)
	if s.reservationsLen() != 0 {
		t.Fatal("moved pod reservation must be released")
	}
	if alerts != 0 {
		t.Fatalf("alerts = %d, want 0", alerts)
	}
}

func TestJanitorBoundButSnapshotMissingReconcilesThenReleases(t *testing.T) {
	s := newSpreadState()
	r := refFor("n1", "q", "d")
	s.TryReserve("u1", r, func(int32, int32) bool { return true })
	s.MarkBound("u1")
	ageReservation(s, "u1", 10*time.Minute)

	// Source (lister) sees the binding, but the snapshot does not.
	src := &fakeSource{pods: map[types.UID]podRef{"u1": r}}
	s.janitorOnce(time.Minute, src, func(types.UID) {})

	if s.reservationsLen() != 0 {
		t.Fatal("verified-but-unobserved reservation must be released after reconcile")
	}
	if _, quotaN := s.effCount("n1", "d", "q"); quotaN != 1 {
		t.Fatalf("forced reconcile lost the real count: %d", quotaN)
	}
}

func TestJanitorVerificationImpossibleKeepsAndAlerts(t *testing.T) {
	s := newSpreadState()
	s.TryReserve("u1", refFor("n1", "q", "d"), func(int32, int32) bool { return true })
	ageReservation(s, "u1", 10*time.Minute)

	src := &fakeSource{hasErr: errors.New("informer down")}
	var alerted []types.UID
	s.janitorOnce(time.Minute, src, func(uid types.UID) { alerted = append(alerted, uid) })

	if s.reservationsLen() != 1 {
		t.Fatal("unverifiable reservation must be KEPT (never blind-deleted)")
	}
	if len(alerted) != 1 || alerted[0] != "u1" {
		t.Fatalf("alerts = %v, want [u1]", alerted)
	}
}

func TestJanitorIgnoresFreshReservations(t *testing.T) {
	s := newSpreadState()
	src := &fakeSource{pods: map[types.UID]podRef{}}
	s.TryReserve("u1", refFor("n1", "q", "d"), func(int32, int32) bool { return true })
	s.janitorOnce(time.Hour, src, func(types.UID) { t.Fatal("fresh reservation alerted") })
	if s.reservationsLen() != 1 {
		t.Fatal("fresh reservation must survive the janitor")
	}
}

// ---- reconciler ----

func TestReconcileRepairsDriftWithoutTouchingReservations(t *testing.T) {
	s := newSpreadState()
	ev := newStateEvents(s)

	// Simulate drift: real pods exist in the source but the snapshot missed
	// events (e.g. scheduler restart).
	src := &fakeSource{pods: map[types.UID]podRef{
		"u1": refFor("n1", "q", "d"),
		"u2": refFor("n1", "q", "d"),
		"u3": refFor("n2", "q", "d"),
	}}
	s.rebuild(src.pods)
	domainN, quotaN := s.effCount("n1", "d", "q")
	if domainN != 2 || quotaN != 2 {
		t.Fatalf("after rebuild = (%d,%d), want (2,2)", domainN, quotaN)
	}

	// A stale snapshot entry not in the source disappears.
	ev.OnPodAddOrUpdate(boundPod("u9", "n3", v1.PodRunning), refFor("n3", "q", "d"))
	if _, quotaN := s.effCount("n3", "d", "q"); quotaN != 1 {
		t.Fatal("setup: u9 counted")
	}
	s.rebuild(src.pods)
	if _, quotaN := s.effCount("n3", "d", "q"); quotaN != 0 {
		t.Fatalf("stale entry survived rebuild: %d", quotaN)
	}

	// Reservations are never aged out or dropped by rebuild.
	s.TryReserve("uR", refFor("n1", "q", "d"), func(int32, int32) bool { return true })
	ageReservation(s, "uR", time.Hour)
	s.rebuild(src.pods)
	if s.reservationsLen() != 1 {
		t.Fatal("reconciler must not touch reservations")
	}
}

func TestRunReconcilerLoop(t *testing.T) {
	s := newSpreadState()
	src := &fakeSource{pods: map[types.UID]podRef{"u1": refFor("n1", "q", "d")}}
	ctx, cancel := context.WithCancel(context.Background())
	go s.RunReconciler(ctx, 10*time.Millisecond, src)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, q := s.effCount("n1", "d", "q"); q == 1 {
			cancel()
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	t.Fatal("reconciler loop never applied the rebuild")
}

// ---- Reserve recheck atomicity under concurrency ----

func TestConcurrentReserveLastSeat(t *testing.T) {
	s := newSpreadState()
	r := refFor("n1", "q", "d")
	const maxPerNode = int32(1)
	const racers = 16

	var mu sync.Mutex
	granted := 0
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uid := types.UID(fmt.Sprintf("u%d", i))
			ok := s.TryReserve(uid, r, func(domainN, quotaN int32) bool {
				return quotaN < maxPerNode && domainN < maxPerNode
			})
			if ok {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if granted != int(maxPerNode) {
		t.Fatalf("granted = %d, want %d (quota must be enforced atomically)", granted, maxPerNode)
	}
	domainN, quotaN := s.effCount("n1", "d", "q")
	if domainN != 1 || quotaN != 1 {
		t.Fatalf("eff after race = (%d,%d), want (1,1)", domainN, quotaN)
	}
}
