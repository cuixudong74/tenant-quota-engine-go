// Package store provides the persistence layer for the quota engine.
//
// The store is event-sourced: every mutation is appended to a write-ahead log
// (see FileStore) and the in-memory state is derived from it. This guarantees
// that restarts neither lose nor double usage statistics.
package store

import (
	"context"
	"errors"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// Common store errors.
var (
	ErrNotFound      = errors.New("store: not found")
	ErrConflict      = errors.New("store: conflict")
	ErrInvalidState  = errors.New("store: invalid state")
	ErrLockNotHeld   = errors.New("store: lock not held")
)

// CounterDelta is an atomic, conditional update to a counter.
//
// The update is applied as:
//
//	used      += UsedDelta
//	reserved  += ReservedDelta
//	borrowed  += BorrowedDelta
//	lent      += LentDelta
//	repayment += RepaymentDelta
//	requests  += RequestsDelta
//	denies    += DeniesDelta
//
// If Guard is non-nil, the delta is applied only when the guard predicate
// holds; otherwise ErrConflict is returned and no mutation happens.
type CounterDelta struct {
	UsedDelta      int64
	ReservedDelta  int64
	BorrowedDelta  int64
	LentDelta      int64
	RepaymentDelta int64
	RequestsDelta  int64
	DeniesDelta    int64

	// Guard, when set, must return true for the delta to be applied.
	Guard func(c *model.Counter) bool `json:"-"`
}

// Store is the persistence interface used by the engine.
//
// All mutating methods are safe for concurrent use. Multi-key atomicity is
// achieved by acquiring shard locks through WithLocks and performing
// check-then-act while the locks are held.
type Store interface {
	// WithLocks acquires the shard locks for the given keys (in a stable
	// order to avoid deadlock) and runs fn while holding them.
	WithLocks(ctx context.Context, keys []string, fn func() error) error

	// GetCounter returns the counter for key, or nil if it does not exist.
	GetCounter(ctx context.Context, key string) (*model.Counter, error)

	// GetCounters returns a map of key -> counter for all existing keys.
	GetCounters(ctx context.Context, keys []string) (map[string]*model.Counter, error)

	// ApplyCounterDelta atomically applies delta to the counter for key,
	// creating it if necessary.
	ApplyCounterDelta(ctx context.Context, key string, delta CounterDelta) error

	// PutReservation stores a reservation (insert or replace).
	PutReservation(ctx context.Context, r *model.Reservation) error

	// GetReservation returns the reservation with the given id.
	GetReservation(ctx context.Context, id string) (*model.Reservation, error)

	// UpdateReservation atomically updates a reservation under the key lock.
	UpdateReservation(ctx context.Context, id string, fn func(r *model.Reservation) (*model.Reservation, error)) error

	// ListReservations returns all reservations (used by the reaper).
	ListReservations(ctx context.Context) ([]*model.Reservation, error)

	// PutRuleVersion stores a rule version.
	PutRuleVersion(ctx context.Context, v *model.RuleVersion) error

	// GetRuleVersion returns the rule version with the given version number.
	GetRuleVersion(ctx context.Context, version int64) (*model.RuleVersion, error)

	// ListRuleVersions returns all rule versions ordered by version.
	ListRuleVersions(ctx context.Context) ([]*model.RuleVersion, error)

	// PutBorrow stores a borrow record.
	PutBorrow(ctx context.Context, b *model.BorrowRecord) error

	// GetBorrow returns the borrow record with the given id.
	GetBorrow(ctx context.Context, id string) (*model.BorrowRecord, error)

	// UpdateBorrow atomically updates a borrow record under the key lock.
	UpdateBorrow(ctx context.Context, id string, fn func(b *model.BorrowRecord) (*model.BorrowRecord, error)) error

	// ListBorrows returns all borrow records.
	ListBorrows(ctx context.Context) ([]*model.BorrowRecord, error)

	// PutPlan stores a plan.
	PutPlan(ctx context.Context, p *model.Plan) error

	// GetPlan returns the plan with the given id.
	GetPlan(ctx context.Context, id string) (*model.Plan, error)

	// ListPlans returns all plans.
	ListPlans(ctx context.Context) ([]*model.Plan, error)

	// PutPlanSwitch records a plan switch.
	PutPlanSwitch(ctx context.Context, s *model.PlanSwitch) error

	// ListPlanSwitches returns all plan switches for a tenant, ordered by time.
	ListPlanSwitches(ctx context.Context, tenantID string) ([]*model.PlanSwitch, error)

	// AppendEvent appends a raw event to the write-ahead log.
	AppendEvent(ctx context.Context, ev model.Event) error

	// Replay replays all persisted events through fn in order.
	Replay(ctx context.Context, fn func(ev model.Event) error) error

	// Snapshot writes a full state snapshot and compacts the log.
	Snapshot(ctx context.Context) error

	// Close flushes and closes the store.
	Close() error
}
