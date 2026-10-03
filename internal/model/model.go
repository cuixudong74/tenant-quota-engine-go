// Package model defines the core domain types for the multi-tenant quota engine.
package model

import "time"

// ScopeType identifies the kind of scope a quota applies to.
type ScopeType string

const (
	// ScopeTenant is the top-level tenant scope.
	ScopeTenant ScopeType = "tenant"
	// ScopeTeam is a team within a tenant.
	ScopeTeam ScopeType = "team"
	// ScopeCaller is the direct caller (leaf) scope.
	ScopeCaller ScopeType = "caller"
)

// Scope is a (type, id) pair identifying a quota scope.
type Scope struct {
	Type ScopeType `json:"type"`
	ID   string    `json:"id"`
}

// Resource is the dimension being quota-controlled (e.g. "api_calls", "tokens").
type Resource string

// DenyReason classifies why a request was rejected.
type DenyReason string

const (
	DenyNone                DenyReason = ""
	DenyQuotaExhausted      DenyReason = "quota_exhausted"
	DenyNoRule              DenyReason = "no_rule_configured"
	DenyReservationNotFound DenyReason = "reservation_not_found"
	DenyReservationState    DenyReason = "reservation_invalid_state"
	DenyAmountInvalid       DenyReason = "amount_invalid"
	DenyBorrowDisabled      DenyReason = "borrow_disabled"
	DenyTenantDefaulted     DenyReason = "tenant_defaulted"
)

// QuotaRule defines a quota limit for a scope/resource/window combination.
type QuotaRule struct {
	Scope         Scope         `json:"scope"`
	Resource      Resource      `json:"resource"`
	Limit         int64         `json:"limit"`
	Window        time.Duration `json:"window"`
	Priority      int           `json:"priority"`
	BorrowEnabled bool          `json:"borrow_enabled"`
	BorrowLimit   int64         `json:"borrow_limit"` // max outstanding borrowed amount; 0 disables
	Version       int64         `json:"version"`
}

// Counter is the per-scope/resource/window usage state.
type Counter struct {
	Key           string   `json:"key"`
	Scope         Scope    `json:"scope"`
	Resource      Resource `json:"resource"`
	WindowStart   int64    `json:"window_start"`
	Used          int64    `json:"used"`
	Reserved      int64    `json:"reserved"`
	Borrowed      int64    `json:"borrowed"`       // outstanding debt (child side)
	Lent          int64    `json:"lent"`           // outstanding lent amount (parent side)
	RepaymentDue  int64    `json:"repayment_due"`  // quota reserved from current window to repay debt
	Requests      int64    `json:"requests"`
	Denies        int64    `json:"denies"`
}

// Available returns the amount still available for new reservations.
// Borrowed is a statistical field (the portion of used that was borrowed);
// it is not subtracted here because the borrowed amount is already reflected
// in Used. Lent and RepaymentDue do reduce availability.
func (c *Counter) Available(limit int64) int64 {
	avail := limit - c.Used - c.Reserved - c.Lent - c.RepaymentDue
	if avail < 0 {
		return 0
	}
	return avail
}

// ReservationStatus is the lifecycle state of a reservation.
type ReservationStatus string

const (
	ReservationPending   ReservationStatus = "pending"
	ReservationConfirmed ReservationStatus = "confirmed"
	ReservationCancelled ReservationStatus = "cancelled"
	ReservationExpired   ReservationStatus = "expired"
)

// Reservation represents a pre-occupied quota amount.
type Reservation struct {
	ID          string            `json:"id"`
	TenantID    string            `json:"tenant_id"`
	TeamID      string            `json:"team_id"`
	CallerID    string            `json:"caller_id"`
	Resource       Resource          `json:"resource"`
	Amount         int64             `json:"amount"`
	Settled        int64             `json:"settled"`
	BorrowedAmount int64             `json:"borrowed_amount"`
	BorrowID       string            `json:"borrow_id"`
	Status         ReservationStatus `json:"status"`
	RuleVersion int64             `json:"rule_version"`
	CreatedAt   int64             `json:"created_at"`
	ExpiresAt   int64             `json:"expires_at"`
	ConfirmedAt int64             `json:"confirmed_at"`
}

// Plan is a quota tier (e.g. free/pro/enterprise).
type Plan struct {
	ID             string      `json:"id"`
	Name           string      `json:"name"`
	Quotas         []QuotaRule `json:"quotas"`
	BorrowEnabled  bool        `json:"borrow_enabled"`
	BorrowLimit    int64       `json:"borrow_limit"`
	GraceWindows   int         `json:"grace_windows"`    // windows allowed before default
	RepayRatio     float64     `json:"repay_ratio"`      // fraction of new window quota repayable per window
	DowngradePlan  string      `json:"downgrade_plan"`   // plan to switch to on default
}

// BorrowRecord tracks a borrow (burst) from a parent scope.
type BorrowRecord struct {
	ID             string   `json:"id"`
	TenantID       string   `json:"tenant_id"`
	ChildScope     Scope    `json:"child_scope"`
	ParentScope    Scope    `json:"parent_scope"`
	Resource       Resource `json:"resource"`
	Amount         int64    `json:"amount"`
	Repaid         int64    `json:"repaid"`
	WindowStart    int64    `json:"window_start"`
	WindowsOverdue int      `json:"windows_overdue"`
	Status         string   `json:"status"` // active|repaid|defaulted
	CreatedAt      int64    `json:"created_at"`
}

// RuleVersion is an immutable, self-consistent set of quota rules.
type RuleVersion struct {
	Version     int64       `json:"version"`
	Rules       []QuotaRule `json:"rules"`
	Status      string      `json:"status"` // draft|active|gray|retired
	GrayPercent int         `json:"gray_percent"`
	CreatedAt   int64       `json:"created_at"`
	ActivatedAt int64       `json:"activated_at"`
	GraceUntil  int64       `json:"grace_until"`
	Note        string      `json:"note"`
}

// PlanSwitch records a plan change for a tenant.
type PlanSwitch struct {
	TenantID  string `json:"tenant_id"`
	FromPlan  string `json:"from_plan"`
	ToPlan    string `json:"to_plan"`
	Mode      string `json:"mode"` // immediate|next_window
	At        int64  `json:"at"`
	WindowStart int64 `json:"window_start"`
}

// Event is a persisted mutation record for the write-ahead log.
type Event struct {
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	Payload   []byte `json:"payload"`
}

// Event types.
const (
	EventCounterDelta      = "counter_delta"
	EventReservationPut    = "reservation_put"
	EventReservationUpdate = "reservation_update"
	EventRuleVersionPut    = "rule_version_put"
	EventBorrowPut         = "borrow_put"
	EventBorrowUpdate      = "borrow_update"
	EventPlanPut           = "plan_put"
	EventPlanSwitch        = "plan_switch"
	EventSnapshot          = "snapshot"
)
