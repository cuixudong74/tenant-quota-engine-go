package store

import (
	"context"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// MemoryStore is an in-memory Store implementation with shard-level locking.
// It is safe for concurrent use.
type MemoryStore struct {
	// mu guards all maps. Shard locks (locks) are used by the engine to
	// perform multi-key check-then-act atomically; individual mutations also
	// take mu so direct store access is race-free.
	mu sync.RWMutex

	counters     map[string]*model.Counter
	reservations map[string]*model.Reservation
	ruleVersions map[int64]*model.RuleVersion
	borrows      map[string]*model.BorrowRecord
	plans        map[string]*model.Plan
	planSwitches map[string][]*model.PlanSwitch

	locks    []sync.RWMutex
	lockMask uint32

	events []model.Event
}

// NewMemoryStore creates a new in-memory store with the given shard count.
// The shard count is rounded up to a power of two.
func NewMemoryStore(shards int) *MemoryStore {
	if shards <= 0 {
		shards = 64
	}
	n := 1
	for n < shards {
		n <<= 1
	}
	return &MemoryStore{
		counters:     make(map[string]*model.Counter),
		reservations: make(map[string]*model.Reservation),
		ruleVersions: make(map[int64]*model.RuleVersion),
		borrows:      make(map[string]*model.BorrowRecord),
		plans:        make(map[string]*model.Plan),
		planSwitches: make(map[string][]*model.PlanSwitch),
		locks:        make([]sync.RWMutex, n),
		lockMask:     uint32(n - 1),
	}
}

func (s *MemoryStore) lockIndex(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() & s.lockMask
}

func (s *MemoryStore) WithLocks(ctx context.Context, keys []string, fn func() error) error {
	if len(keys) == 0 {
		return fn()
	}
	idx := make([]uint32, 0, len(keys))
	seen := make(map[uint32]struct{}, len(keys))
	for _, k := range keys {
		i := s.lockIndex(k)
		if _, ok := seen[i]; ok {
			continue
		}
		seen[i] = struct{}{}
		idx = append(idx, i)
	}
	sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })

	for _, i := range idx {
		s.locks[i].Lock()
	}
	defer func() {
		for i := len(idx) - 1; i >= 0; i-- {
			s.locks[idx[i]].Unlock()
		}
	}()
	return fn()
}

func cloneCounter(c *model.Counter) *model.Counter {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

func cloneReservation(r *model.Reservation) *model.Reservation {
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}

func cloneBorrow(b *model.BorrowRecord) *model.BorrowRecord {
	if b == nil {
		return nil
	}
	cp := *b
	return &cp
}

func cloneRuleVersion(v *model.RuleVersion) *model.RuleVersion {
	if v == nil {
		return nil
	}
	cp := *v
	cp.Rules = append([]model.QuotaRule(nil), v.Rules...)
	return &cp
}

func clonePlan(p *model.Plan) *model.Plan {
	if p == nil {
		return nil
	}
	cp := *p
	cp.Quotas = append([]model.QuotaRule(nil), p.Quotas...)
	return &cp
}

func (s *MemoryStore) GetCounter(ctx context.Context, key string) (*model.Counter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneCounter(s.counters[key]), nil
}

func (s *MemoryStore) GetCounters(ctx context.Context, keys []string) (map[string]*model.Counter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*model.Counter, len(keys))
	for _, k := range keys {
		if c, ok := s.counters[k]; ok {
			out[k] = cloneCounter(c)
		}
	}
	return out, nil
}

func (s *MemoryStore) ApplyCounterDelta(ctx context.Context, key string, delta CounterDelta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.counters[key]
	if !ok {
		c = &model.Counter{Key: key}
		s.counters[key] = c
	}
	if delta.Guard != nil && !delta.Guard(c) {
		return ErrConflict
	}
	c.Used += delta.UsedDelta
	c.Reserved += delta.ReservedDelta
	c.Borrowed += delta.BorrowedDelta
	c.Lent += delta.LentDelta
	c.RepaymentDue += delta.RepaymentDelta
	c.Requests += delta.RequestsDelta
	c.Denies += delta.DeniesDelta
	if c.Used < 0 {
		c.Used = 0
	}
	if c.Reserved < 0 {
		c.Reserved = 0
	}
	if c.Borrowed < 0 {
		c.Borrowed = 0
	}
	if c.Lent < 0 {
		c.Lent = 0
	}
	if c.RepaymentDue < 0 {
		c.RepaymentDue = 0
	}
	return nil
}

func (s *MemoryStore) PutReservation(ctx context.Context, r *model.Reservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reservations[r.ID] = cloneReservation(r)
	return nil
}

func (s *MemoryStore) GetReservation(ctx context.Context, id string) (*model.Reservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.reservations[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneReservation(r), nil
}

func (s *MemoryStore) UpdateReservation(ctx context.Context, id string, fn func(r *model.Reservation) (*model.Reservation, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.reservations[id]
	if !ok {
		return ErrNotFound
	}
	updated, err := fn(r)
	if err != nil {
		return err
	}
	if updated != nil {
		s.reservations[id] = cloneReservation(updated)
	}
	return nil
}

func (s *MemoryStore) ListReservations(ctx context.Context) ([]*model.Reservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Reservation, 0, len(s.reservations))
	for _, r := range s.reservations {
		out = append(out, cloneReservation(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out, nil
}

func (s *MemoryStore) PutRuleVersion(ctx context.Context, v *model.RuleVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ruleVersions[v.Version] = cloneRuleVersion(v)
	return nil
}

func (s *MemoryStore) GetRuleVersion(ctx context.Context, version int64) (*model.RuleVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.ruleVersions[version]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneRuleVersion(v), nil
}

func (s *MemoryStore) ListRuleVersions(ctx context.Context) ([]*model.RuleVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.RuleVersion, 0, len(s.ruleVersions))
	for _, v := range s.ruleVersions {
		out = append(out, cloneRuleVersion(v))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

func (s *MemoryStore) PutBorrow(ctx context.Context, b *model.BorrowRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.borrows[b.ID] = cloneBorrow(b)
	return nil
}

func (s *MemoryStore) GetBorrow(ctx context.Context, id string) (*model.BorrowRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.borrows[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneBorrow(b), nil
}

func (s *MemoryStore) UpdateBorrow(ctx context.Context, id string, fn func(b *model.BorrowRecord) (*model.BorrowRecord, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.borrows[id]
	if !ok {
		return ErrNotFound
	}
	updated, err := fn(b)
	if err != nil {
		return err
	}
	if updated != nil {
		s.borrows[id] = cloneBorrow(updated)
	}
	return nil
}

func (s *MemoryStore) ListBorrows(ctx context.Context) ([]*model.BorrowRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.BorrowRecord, 0, len(s.borrows))
	for _, b := range s.borrows {
		out = append(out, cloneBorrow(b))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt < out[j].CreatedAt })
	return out, nil
}

func (s *MemoryStore) PutPlan(ctx context.Context, p *model.Plan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plans[p.ID] = clonePlan(p)
	return nil
}

func (s *MemoryStore) GetPlan(ctx context.Context, id string) (*model.Plan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.plans[id]
	if !ok {
		return nil, ErrNotFound
	}
	return clonePlan(p), nil
}

func (s *MemoryStore) ListPlans(ctx context.Context) ([]*model.Plan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.Plan, 0, len(s.plans))
	for _, p := range s.plans {
		out = append(out, clonePlan(p))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *MemoryStore) PutPlanSwitch(ctx context.Context, sw *model.PlanSwitch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.planSwitches[sw.TenantID] = append(s.planSwitches[sw.TenantID], sw)
	return nil
}

func (s *MemoryStore) ListPlanSwitches(ctx context.Context, tenantID string) ([]*model.PlanSwitch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*model.PlanSwitch, 0, len(s.planSwitches[tenantID]))
	for _, sw := range s.planSwitches[tenantID] {
		cp := *sw
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At < out[j].At })
	return out, nil
}

func (s *MemoryStore) AppendEvent(ctx context.Context, ev model.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
	return nil
}

func (s *MemoryStore) Replay(ctx context.Context, fn func(ev model.Event) error) error {
	s.mu.RLock()
	events := append([]model.Event(nil), s.events...)
	s.mu.RUnlock()
	for _, ev := range events {
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}

func (s *MemoryStore) Snapshot(ctx context.Context) error {
	// In-memory store needs no compaction; events are retained for replay.
	return nil
}

func (s *MemoryStore) Close() error {
	return nil
}
