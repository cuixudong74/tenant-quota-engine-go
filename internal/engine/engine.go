// Package engine implements the multi-tenant quota control engine.
//
// The engine provides reserve/confirm/cancel semantics with hierarchical
// (tenant/team/caller) quota enforcement, burst borrowing from parent scopes,
// plan upgrade/downgrade with in-window proration, and durable usage state.
package engine

import (
	"context"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/rules"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

// ReserveRequest is a request to pre-occupy quota.
type ReserveRequest struct {
	TenantID string
	TeamID   string
	CallerID string
	Resource model.Resource
	Amount   int64
	TTL      time.Duration
}

// ReserveResult is the outcome of a reserve call.
type ReserveResult struct {
	Allowed     bool
	Reason      model.DenyReason
	Reservation *model.Reservation
	RuleVersion int64
	DenyScope   model.Scope
	Remaining   int64
}

// UsageInfo is an observable usage snapshot for a scope/resource.
type UsageInfo struct {
	Scope        model.Scope
	Resource     model.Resource
	WindowStart  int64
	WindowEnd    int64
	Limit        int64
	Used         int64
	Reserved     int64
	Borrowed     int64
	Lent         int64
	RepaymentDue int64
	Remaining    int64
	Requests     int64
	Denies       int64
}

// Engine is the quota engine interface.
type Engine interface {
	// Reserve pre-occupies quota. The decision is atomic across the whole
	// tenant/team/caller hierarchy.
	Reserve(ctx context.Context, req *ReserveRequest) (*ReserveResult, error)

	// Confirm settles a pending reservation. Settled may be less than or
	// equal to the reserved amount; the difference is released. Idempotent.
	Confirm(ctx context.Context, reservationID string, settled int64) (*model.Reservation, error)

	// Cancel releases a pending reservation's quota. Idempotent.
	Cancel(ctx context.Context, reservationID string) (*model.Reservation, error)

	// SwitchPlan upgrades or downgrades a tenant's plan.
	SwitchPlan(ctx context.Context, tenantID, planID, mode string) (*model.PlanSwitch, error)

	// GetUsage returns the current usage snapshot for a scope/resource.
	GetUsage(ctx context.Context, scope model.Scope, resource model.Resource) (*UsageInfo, error)

	// ReapExpired expires reservations past their TTL and processes borrow
	// repayments/defaults. It is safe to call periodically.
	ReapExpired(ctx context.Context) (expired int, err error)

	// Rules returns the rule version manager.
	Rules() *rules.Manager
}

// Config holds engine tuning parameters.
type Config struct {
	// DefaultTTL is the reservation TTL used when ReserveRequest.TTL is 0.
	DefaultTTL time.Duration
	// ReapInterval is how often the background reaper runs (0 disables it).
	ReapInterval time.Duration
	// ShardCount is the number of store lock shards.
	ShardCount int
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		DefaultTTL:   5 * time.Minute,
		ReapInterval: 30 * time.Second,
		ShardCount:   64,
	}
}

// engine is the default Engine implementation.
type engine struct {
	store  store.Store
	rules  *rules.Manager
	config Config
}

// New creates a quota engine backed by the given store and rule manager.
func New(s store.Store, rm *rules.Manager, cfg Config) Engine {
	if cfg.DefaultTTL == 0 {
		cfg.DefaultTTL = 5 * time.Minute
	}
	if cfg.ShardCount <= 0 {
		cfg.ShardCount = 64
	}
	return &engine{store: s, rules: rm, config: cfg}
}

func (e *engine) Rules() *rules.Manager {
	return e.rules
}
