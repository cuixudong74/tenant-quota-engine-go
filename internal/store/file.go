package store

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// FileStore is a Store backed by an append-only write-ahead log on disk.
//
// Every mutation is durably appended to the log (fsynced) before the in-memory
// state is updated, so a process restart replays the log and recovers exact
// state — usage is never lost or doubled. Periodic snapshots compact the log.
type FileStore struct {
	mem *MemoryStore
	mu  sync.Mutex // protects the WAL file
	dir string

	walPath      string
	snapshotPath string
	wal          *os.File
}

// walEntry is the serialized payload of a WAL event.
type walEntry struct {
	Key         string              `json:"key,omitempty"`
	Delta       *CounterDelta       `json:"delta,omitempty"`
	Reservation *model.Reservation  `json:"reservation,omitempty"`
	RuleVersion *model.RuleVersion  `json:"rule_version,omitempty"`
	Borrow      *model.BorrowRecord `json:"borrow,omitempty"`
	Plan        *model.Plan         `json:"plan,omitempty"`
	PlanSwitch  *model.PlanSwitch   `json:"plan_switch,omitempty"`
}

// snapshotData is the serialized full state used for log compaction.
type snapshotData struct {
	Counters     map[string]*model.Counter     `json:"counters"`
	Reservations map[string]*model.Reservation `json:"reservations"`
	RuleVersions map[int64]*model.RuleVersion  `json:"rule_versions"`
	Borrows      map[string]*model.BorrowRecord `json:"borrows"`
	Plans        map[string]*model.Plan        `json:"plans"`
	PlanSwitches map[string][]*model.PlanSwitch `json:"plan_switches"`
}

// NewFileStore opens (or creates) a file-backed store under dir and replays
// any existing log to recover state.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStore{
		mem:          NewMemoryStore(64),
		dir:          dir,
		walPath:      filepath.Join(dir, "wal.log"),
		snapshotPath: filepath.Join(dir, "snapshot.json"),
	}

	// Load snapshot if present.
	if data, err := os.ReadFile(s.snapshotPath); err == nil {
		var snap snapshotData
		if err := json.Unmarshal(data, &snap); err != nil {
			return nil, fmt.Errorf("store: load snapshot: %w", err)
		}
		s.loadSnapshot(&snap)
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	// Open WAL in append mode.
	f, err := os.OpenFile(s.walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.wal = f

	// Replay WAL to recover state after the snapshot.
	if err := s.replayWAL(); err != nil {
		return nil, fmt.Errorf("store: replay wal: %w", err)
	}
	return s, nil
}

func (s *FileStore) loadSnapshot(snap *snapshotData) {
	s.mem.mu.Lock()
	defer s.mem.mu.Unlock()
	if snap.Counters != nil {
		s.mem.counters = snap.Counters
	}
	if snap.Reservations != nil {
		s.mem.reservations = snap.Reservations
	}
	if snap.RuleVersions != nil {
		s.mem.ruleVersions = snap.RuleVersions
	}
	if snap.Borrows != nil {
		s.mem.borrows = snap.Borrows
	}
	if snap.Plans != nil {
		s.mem.plans = snap.Plans
	}
	if snap.PlanSwitches != nil {
		s.mem.planSwitches = snap.PlanSwitches
	}
}

// appendWAL durably appends an event to the WAL. Caller must hold s.mu.
func (s *FileStore) appendWAL(ev model.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := s.wal.Write(append(data, '\n')); err != nil {
		return err
	}
	return s.wal.Sync()
}

func (s *FileStore) replayWAL() error {
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(s.wal)
	scanner.Buffer(make([]byte, 1024*1024), 32*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev model.Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return err
		}
		if err := s.applyEvent(ev); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	// Seek back to end for appends.
	_, err := s.wal.Seek(0, io.SeekEnd)
	return err
}

// applyEvent applies a persisted event to the in-memory state. During replay
// guards are intentionally skipped: committed events must always land.
func (s *FileStore) applyEvent(ev model.Event) error {
	var e walEntry
	if len(ev.Payload) > 0 {
		if err := json.Unmarshal(ev.Payload, &e); err != nil {
			return err
		}
	}
	s.mem.mu.Lock()
	defer s.mem.mu.Unlock()

	switch ev.Type {
	case model.EventCounterDelta:
		c, ok := s.mem.counters[e.Key]
		if !ok {
			c = &model.Counter{Key: e.Key}
			s.mem.counters[e.Key] = c
		}
		applyDelta(c, e.Delta)
	case model.EventReservationPut, model.EventReservationUpdate:
		s.mem.reservations[e.Reservation.ID] = cloneReservation(e.Reservation)
	case model.EventRuleVersionPut:
		s.mem.ruleVersions[e.RuleVersion.Version] = cloneRuleVersion(e.RuleVersion)
	case model.EventBorrowPut, model.EventBorrowUpdate:
		s.mem.borrows[e.Borrow.ID] = cloneBorrow(e.Borrow)
	case model.EventPlanPut:
		s.mem.plans[e.Plan.ID] = clonePlan(e.Plan)
	case model.EventPlanSwitch:
		s.mem.planSwitches[e.PlanSwitch.TenantID] = append(s.mem.planSwitches[e.PlanSwitch.TenantID], e.PlanSwitch)
	default:
		// Unknown event types are ignored for forward compatibility.
	}
	return nil
}

func applyDelta(c *model.Counter, d *CounterDelta) {
	if d == nil {
		return
	}
	c.Used += d.UsedDelta
	c.Reserved += d.ReservedDelta
	c.Borrowed += d.BorrowedDelta
	c.Lent += d.LentDelta
	c.RepaymentDue += d.RepaymentDelta
	c.Requests += d.RequestsDelta
	c.Denies += d.DeniesDelta
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
}

// persist appends an event to the WAL and then applies it to memory.
func (s *FileStore) persist(ev model.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.appendWAL(ev); err != nil {
		return err
	}
	return s.applyEvent(ev)
}

func eventFor(typ string, e walEntry) model.Event {
	payload, _ := json.Marshal(e)
	return model.Event{Type: typ, Payload: payload}
}

func (s *FileStore) WithLocks(ctx context.Context, keys []string, fn func() error) error {
	return s.mem.WithLocks(ctx, keys, fn)
}

func (s *FileStore) GetCounter(ctx context.Context, key string) (*model.Counter, error) {
	return s.mem.GetCounter(ctx, key)
}

func (s *FileStore) GetCounters(ctx context.Context, keys []string) (map[string]*model.Counter, error) {
	return s.mem.GetCounters(ctx, keys)
}

func (s *FileStore) ApplyCounterDelta(ctx context.Context, key string, delta CounterDelta) error {
	ev := eventFor(model.EventCounterDelta, walEntry{Key: key, Delta: &delta})
	return s.persist(ev)
}

func (s *FileStore) PutReservation(ctx context.Context, r *model.Reservation) error {
	ev := eventFor(model.EventReservationPut, walEntry{Reservation: r})
	return s.persist(ev)
}

func (s *FileStore) GetReservation(ctx context.Context, id string) (*model.Reservation, error) {
	return s.mem.GetReservation(ctx, id)
}

func (s *FileStore) UpdateReservation(ctx context.Context, id string, fn func(r *model.Reservation) (*model.Reservation, error)) error {
	// Run the update under the shard lock, then persist the resulting state.
	return s.mem.UpdateReservation(ctx, id, func(r *model.Reservation) (*model.Reservation, error) {
		updated, err := fn(r)
		if err != nil {
			return nil, err
		}
		if updated == nil {
			return nil, nil
		}
		ev := eventFor(model.EventReservationUpdate, walEntry{Reservation: updated})
		if err := s.persist(ev); err != nil {
			return nil, err
		}
		return updated, nil
	})
}

func (s *FileStore) ListReservations(ctx context.Context) ([]*model.Reservation, error) {
	return s.mem.ListReservations(ctx)
}

func (s *FileStore) PutRuleVersion(ctx context.Context, v *model.RuleVersion) error {
	ev := eventFor(model.EventRuleVersionPut, walEntry{RuleVersion: v})
	return s.persist(ev)
}

func (s *FileStore) GetRuleVersion(ctx context.Context, version int64) (*model.RuleVersion, error) {
	return s.mem.GetRuleVersion(ctx, version)
}

func (s *FileStore) ListRuleVersions(ctx context.Context) ([]*model.RuleVersion, error) {
	return s.mem.ListRuleVersions(ctx)
}

func (s *FileStore) PutBorrow(ctx context.Context, b *model.BorrowRecord) error {
	ev := eventFor(model.EventBorrowPut, walEntry{Borrow: b})
	return s.persist(ev)
}

func (s *FileStore) GetBorrow(ctx context.Context, id string) (*model.BorrowRecord, error) {
	return s.mem.GetBorrow(ctx, id)
}

func (s *FileStore) UpdateBorrow(ctx context.Context, id string, fn func(b *model.BorrowRecord) (*model.BorrowRecord, error)) error {
	return s.mem.UpdateBorrow(ctx, id, func(b *model.BorrowRecord) (*model.BorrowRecord, error) {
		updated, err := fn(b)
		if err != nil {
			return nil, err
		}
		if updated == nil {
			return nil, nil
		}
		ev := eventFor(model.EventBorrowUpdate, walEntry{Borrow: updated})
		if err := s.persist(ev); err != nil {
			return nil, err
		}
		return updated, nil
	})
}

func (s *FileStore) ListBorrows(ctx context.Context) ([]*model.BorrowRecord, error) {
	return s.mem.ListBorrows(ctx)
}

func (s *FileStore) PutPlan(ctx context.Context, p *model.Plan) error {
	ev := eventFor(model.EventPlanPut, walEntry{Plan: p})
	return s.persist(ev)
}

func (s *FileStore) GetPlan(ctx context.Context, id string) (*model.Plan, error) {
	return s.mem.GetPlan(ctx, id)
}

func (s *FileStore) ListPlans(ctx context.Context) ([]*model.Plan, error) {
	return s.mem.ListPlans(ctx)
}

func (s *FileStore) PutPlanSwitch(ctx context.Context, sw *model.PlanSwitch) error {
	ev := eventFor(model.EventPlanSwitch, walEntry{PlanSwitch: sw})
	return s.persist(ev)
}

func (s *FileStore) ListPlanSwitches(ctx context.Context, tenantID string) ([]*model.PlanSwitch, error) {
	return s.mem.ListPlanSwitches(ctx, tenantID)
}

func (s *FileStore) AppendEvent(ctx context.Context, ev model.Event) error {
	return s.persist(ev)
}

func (s *FileStore) Replay(ctx context.Context, fn func(ev model.Event) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(s.wal)
	scanner.Buffer(make([]byte, 1024*1024), 32*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev model.Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return err
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	_, err := s.wal.Seek(0, io.SeekEnd)
	return err
}

func (s *FileStore) Snapshot(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mem.mu.RLock()
	snap := snapshotData{
		Counters:     make(map[string]*model.Counter, len(s.mem.counters)),
		Reservations: make(map[string]*model.Reservation, len(s.mem.reservations)),
		RuleVersions: make(map[int64]*model.RuleVersion, len(s.mem.ruleVersions)),
		Borrows:      make(map[string]*model.BorrowRecord, len(s.mem.borrows)),
		Plans:        make(map[string]*model.Plan, len(s.mem.plans)),
		PlanSwitches: make(map[string][]*model.PlanSwitch, len(s.mem.planSwitches)),
	}
	for k, v := range s.mem.counters {
		snap.Counters[k] = cloneCounter(v)
	}
	for k, v := range s.mem.reservations {
		snap.Reservations[k] = cloneReservation(v)
	}
	for k, v := range s.mem.ruleVersions {
		snap.RuleVersions[k] = cloneRuleVersion(v)
	}
	for k, v := range s.mem.borrows {
		snap.Borrows[k] = cloneBorrow(v)
	}
	for k, v := range s.mem.plans {
		snap.Plans[k] = clonePlan(v)
	}
	for k, v := range s.mem.planSwitches {
		cp := make([]*model.PlanSwitch, len(v))
		for i, sw := range v {
			c := *sw
			cp[i] = &c
		}
		snap.PlanSwitches[k] = cp
	}
	s.mem.mu.RUnlock()

	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := s.snapshotPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.snapshotPath); err != nil {
		return err
	}
	// Compact the WAL: truncate to empty.
	if err := s.wal.Truncate(0); err != nil {
		return err
	}
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return s.wal.Sync()
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal != nil {
		err := s.wal.Sync()
		if cerr := s.wal.Close(); err == nil {
			err = cerr
		}
		s.wal = nil
		return err
	}
	return nil
}
