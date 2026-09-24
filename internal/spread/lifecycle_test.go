package spread

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// countingSource counts ListCountable calls so the smoke test can prove the
// loops actually ticked on their periods, not merely that startup returned.
type countingSource struct {
	src   *fakeSource
	calls atomic.Int32
}

func (c *countingSource) ListCountable() (map[types.UID]podRef, error) {
	c.calls.Add(1)
	return c.src.ListCountable()
}

func newCountingSource() *countingSource {
	return &countingSource{src: &fakeSource{pods: map[types.UID]podRef{}}}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// TestStartLifecycleLoopsSmoke is the assembly smoke test for the M4 wiring:
// starting both loops must not panic and must actually fire on their
// periods. The reconciler's source read proves its ticker fired; an expired
// reservation absent from the source drives the janitor's verified release
// path, proving the janitor fired too.
func TestStartLifecycleLoopsSmoke(t *testing.T) {
	st := newSpreadState()
	if !st.TryReserve("uJan", refFor("n1", "q", "d"), func(int32, int32) bool { return true }) {
		t.Fatal("setup: reservation not created")
	}
	ageReservation(st, "uJan", time.Hour)

	src := newCountingSource()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Non-blocking and panic-free: the loops run in their own goroutines.
	StartLifecycleLoops(ctx, st, src, 10*time.Millisecond, 10*time.Millisecond, func(types.UID) {})

	if !waitFor(t, 2*time.Second, func() bool { return src.calls.Load() > 0 }) {
		t.Fatal("lifecycle loops never queried the pod source (no tick fired)")
	}
	if !waitFor(t, 2*time.Second, func() bool { return st.reservationsLen() == 0 }) {
		t.Fatalf("janitor never released the expired reservation (%d left)", st.reservationsLen())
	}

	cancel() // both goroutines must observe ctx.Done and exit
}

// TestStartLifecycleLoopsGuardsZeroDurations pins the guard against the
// ticker panic that zero durations would otherwise cause inside the loops
// (time.NewTicker(0) panics; in a goroutine that crashes the whole process,
// so this test failing means the scheduler would die at startup).
func TestStartLifecycleLoopsGuardsZeroDurations(t *testing.T) {
	st := newSpreadState()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Zero ttl/period fall back to defaults; a nil alert must be tolerated.
	StartLifecycleLoops(ctx, st, newCountingSource(), 0, 0, nil)

	// Give any unguarded goroutine a window to panic before cancelling.
	time.Sleep(20 * time.Millisecond)
	cancel()
}
