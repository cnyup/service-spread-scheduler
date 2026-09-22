package spread

import (
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// podRef is the per-pod footprint inside a snapshot (dev-design §4.2).
type podRef struct {
	QuotaKey string // service quota key (maxPodsPerNode dimension)
	SchedKey string // service scheduling key (maxSkew dimension)
	NodeName string
}

// snapshot is the immutable per-node count state maintained by COW from pod
// informer events. Readers only ever see a fully built snapshot.
type snapshot struct {
	pods       map[types.UID]podRef
	nodeQuota  map[string]map[string]int32 // node -> quotaKey -> n
	nodeDomain map[string]map[string]int32 // node -> schedKey -> n
}

func (s *snapshot) clone() *snapshot {
	c := &snapshot{
		pods:       make(map[types.UID]podRef, len(s.pods)),
		nodeQuota:  make(map[string]map[string]int32, len(s.nodeQuota)),
		nodeDomain: make(map[string]map[string]int32, len(s.nodeDomain)),
	}
	for k, v := range s.pods {
		c.pods[k] = v
	}
	for n, m := range s.nodeQuota {
		cm := make(map[string]int32, len(m))
		for k, v := range m {
			cm[k] = v
		}
		c.nodeQuota[n] = cm
	}
	for n, m := range s.nodeDomain {
		cm := make(map[string]int32, len(m))
		for k, v := range m {
			cm[k] = v
		}
		c.nodeDomain[n] = cm
	}
	return c
}

func (s *snapshot) count(node, quotaKey, schedKey string) (quotaN, domainN int32) {
	if m := s.nodeQuota[node]; m != nil {
		quotaN = m[quotaKey]
	}
	if m := s.nodeDomain[node]; m != nil {
		domainN = m[schedKey]
	}
	return
}

type resvPhase int

const (
	// resvReserved: Reserve succeeded, Bind not yet reported.
	resvReserved resvPhase = iota
	// resvBoundObs: PostBind ran; waiting for the informer to observe the
	// binding before the reservation is released.
	resvBoundObs
)

type reservation struct {
	UID      types.UID
	QuotaKey string
	SchedKey string
	NodeName string
	Phase    resvPhase
	Since    time.Time // TTL janitor baseline
}

// spreadState is the M2 state machine: an atomically swapped immutable count
// snapshot plus mutex-guarded reservations. The only safety boundary is the
// Reserve critical section, which reads the latest snapshot, rechecks and
// writes the reservation under mu (dev-design §4.1).
type spreadState struct {
	mu   sync.Mutex
	snap atomic.Pointer[snapshot]
	resv map[types.UID]*reservation
}

func newSpreadState() *spreadState {
	s := &spreadState{resv: map[types.UID]*reservation{}}
	s.snap.Store(&snapshot{
		pods:       map[types.UID]podRef{},
		nodeQuota:  map[string]map[string]int32{},
		nodeDomain: map[string]map[string]int32{},
	})
	return s
}

// effCount returns the effective per-node counts for the given keys: the
// snapshot counts plus the reservation overlay. It is the read path used by
// Filter/Reserve rechecks. Reservations may briefly double count a pod whose
// binding the informer already observed — overestimating by one seat is the
// accepted direction (dev-design §4.1).
func (s *spreadState) effCount(node, schedKey, quotaKey string) (domainN, quotaN int32) {
	snap := s.snap.Load()
	quotaN, domainN = snap.count(node, quotaKey, schedKey)

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.resv {
		if r.NodeName != node {
			continue
		}
		if r.QuotaKey == quotaKey {
			quotaN++
		}
		if r.SchedKey == schedKey {
			domainN++
		}
	}
	return domainN, quotaN
}

// TryReserve performs the Reserve critical section atomically: read the
// latest snapshot, overlay reservations, run the caller's admission check
// against the effective counts, and only then persist the reservation. The
// check callback receives (domainN, quotaN) for the candidate node.
// A reservation for an existing UID is refused.
func (s *spreadState) TryReserve(uid types.UID, ref podRef, admit func(domainN, quotaN int32) bool) bool {
	return s.TryReserveChecked(uid, ref, func(g *reserveGuard) bool {
		domainN, quotaN := g.EffCount(ref.NodeName, ref.SchedKey, ref.QuotaKey)
		return admit(domainN, quotaN)
	})
}

// reserveGuard is a lock-held view over the latest snapshot plus all
// reservations. It exists so the plugin's Reserve revalidation (dev-design
// §5.6: domain membership + quota + skew against the freshest state) runs
// inside the same critical section that writes the reservation.
type reserveGuard struct {
	snap *snapshot
	resv map[types.UID]*reservation
}

// EffCount returns (domainN, quotaN) effective counts for one node from the
// guarded view.
func (g *reserveGuard) EffCount(node, schedKey, quotaKey string) (int32, int32) {
	quotaN, domainN := g.snap.count(node, quotaKey, schedKey)
	for _, r := range g.resv {
		if r.NodeName != node {
			continue
		}
		if r.QuotaKey == quotaKey {
			quotaN++
		}
		if r.SchedKey == schedKey {
			domainN++
		}
	}
	return domainN, quotaN
}

// TryReserveChecked is the general Reserve entry point: admit runs under mu
// with a consistent view of snapshot + reservations. Persistence happens
// only when admit returns true. A reservation for an existing UID is
// refused.
func (s *spreadState) TryReserveChecked(uid types.UID, ref podRef, admit func(g *reserveGuard) bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.resv[uid]; exists {
		return false
	}
	g := &reserveGuard{snap: s.snap.Load(), resv: s.resv}
	if !admit(g) {
		return false
	}
	s.resv[uid] = &reservation{
		UID: uid, QuotaKey: ref.QuotaKey, SchedKey: ref.SchedKey,
		NodeName: ref.NodeName, Phase: resvReserved, Since: time.Now(),
	}
	return true
}

// Unreserve releases a Reserved-phase reservation (Bind failed). It is a
// no-op for BoundObs entries, which only leave via observation or the
// verifying janitor.
func (s *spreadState) Unreserve(uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.resv[uid]; r != nil && r.Phase == resvReserved {
		delete(s.resv, uid)
	}
}

// MarkBound transitions Reserved -> BoundObs (PostBind). It never deletes
// the reservation: release happens only when the informer observes the
// binding (or the janitor verifies it).
func (s *spreadState) MarkBound(uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.resv[uid]; r != nil && r.Phase == resvReserved {
		r.Phase = resvBoundObs
	}
}

// releaseIfObserved releases the reservation when the real snapshot already
// counts the pod on the reserved node — the idempotent, order-independent
// release path (bind event before or after PostBind both converge here).
func (s *spreadState) releaseIfObserved(uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.resv[uid]
	if r == nil {
		return
	}
	if ref, counted := s.snap.Load().pods[uid]; counted && ref.NodeName == r.NodeName {
		delete(s.resv, uid)
	}
}

// reservationsLen is a test/observability helper.
func (s *spreadState) reservationsLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.resv)
}

// reservationPhase is a test/observability helper; returns (-1, false) when
// no reservation exists.
func (s *spreadState) reservationPhase(uid types.UID) (resvPhase, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.resv[uid]
	if !ok {
		return -1, false
	}
	return r.Phase, true
}

// ---- snapshot maintenance (COW, under mu so swaps serialize) ----

func (s *spreadState) upsertCount(uid types.UID, ref podRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.snap.Load().clone()
	applyCount(next, uid, ref)
	s.snap.Store(next)
}

func (s *spreadState) removeCount(uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.snap.Load().clone()
	unapplyCount(next, uid)
	s.snap.Store(next)
}

// rebuild replaces the snapshot with one derived from the given counts. It
// only repairs count drift and never touches reservations (dev-design §4.4).
func (s *spreadState) rebuild(pods map[types.UID]podRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := &snapshot{
		pods:       make(map[types.UID]podRef, len(pods)),
		nodeQuota:  map[string]map[string]int32{},
		nodeDomain: map[string]map[string]int32{},
	}
	for uid, ref := range pods {
		applyCount(next, uid, ref)
	}
	s.snap.Store(next)
}

func applyCount(snap *snapshot, uid types.UID, ref podRef) {
	if _, ok := snap.pods[uid]; ok {
		unapplyCount(snap, uid)
	}
	snap.pods[uid] = ref
	incMap(snap.nodeQuota, ref.NodeName, ref.QuotaKey)
	incMap(snap.nodeDomain, ref.NodeName, ref.SchedKey)
}

func unapplyCount(snap *snapshot, uid types.UID) {
	old, ok := snap.pods[uid]
	if !ok {
		return
	}
	delete(snap.pods, uid)
	decMap(snap.nodeQuota, old.NodeName, old.QuotaKey)
	decMap(snap.nodeDomain, old.NodeName, old.SchedKey)
}

func incMap(m map[string]map[string]int32, outer, inner string) {
	if m[outer] == nil {
		m[outer] = map[string]int32{}
	}
	m[outer][inner]++
}

func decMap(m map[string]map[string]int32, outer, inner string) {
	if m[outer] == nil {
		return
	}
	if m[outer][inner] <= 1 {
		delete(m[outer], inner)
		if len(m[outer]) == 0 {
			delete(m, outer)
		}
		return
	}
	m[outer][inner]--
}
