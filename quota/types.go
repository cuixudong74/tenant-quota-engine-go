// Package quota 提供面向多租户的配额判定、用量占用与结算、
// 层级配额、套餐变更、突发借用、规则版本化与可观测能力。
package quota

import "time"

// Decision 是配额判定的结果：允许或拒绝。
type Decision int

const (
	DecisionAllow Decision = iota
	DecisionDeny
)

// DenyReason 标识可区分的拒绝原因。
type DenyReason string

const (
	DenyNone               DenyReason = ""
	DenyTenantQuota        DenyReason = "TENANT_QUOTA_EXCEEDED"
	DenyTeamQuota          DenyReason = "TEAM_QUOTA_EXCEEDED"
	DenyCallerQuota        DenyReason = "CALLER_QUOTA_EXCEEDED"
	DenyBorrowLimit        DenyReason = "BORROW_LIMIT_EXCEEDED"
	DenyNoRule             DenyReason = "NO_MATCHING_RULE"
	DenyInvalidReservation DenyReason = "INVALID_RESERVATION"
	DenyOverdueDowngrade   DenyReason = "PLAN_DOWNGRADED_OVERDUE_BORROW"
)

// Result 是一次判定或操作的结果。
type Result struct {
	Decision    Decision
	Reason      DenyReason
	Detail      string
	RuleVersion uint64
}

// Allowed 报告判定是否放行。
func (r Result) Allowed() bool { return r.Decision == DecisionAllow }

func allow(version uint64) Result { return Result{Decision: DecisionAllow, RuleVersion: version} }

func deny(reason DenyReason, detail string, version uint64) Result {
	return Result{Decision: DecisionDeny, Reason: reason, Detail: detail, RuleVersion: version}
}

// Scope 标识配额作用的层级。
type Scope string

const (
	ScopeTenant Scope = "tenant"
	ScopeTeam   Scope = "team"
	ScopeCaller Scope = "caller"
)

// Subject 标识一次调用的主体：调用方属于某个团队，团队属于某个租户。
type Subject struct {
	TenantID string
	TeamID   string
	CallerID string
}

// CheckRequest 是一次配额判定/占用请求。
type CheckRequest struct {
	Subject  Subject
	Resource string
	Amount   int64
	Now      time.Time
	// RuleVersion 非零时表示请求方钉住的规则版本（宽限期内有效）。
	RuleVersion uint64
}

// Charge 是预留在各层级（含资源池）上的一条占用记录，
// 结算或归还时按 Charges 逐条反向冲销，保证不多扣也不少还。
type Charge struct {
	ScopeKey  string `json:"scope_key"`
	Resource  string `json:"resource"`
	WindowKey int64  `json:"window_key"`
	Amount    int64  `json:"amount"`
}

// Reservation 表示一笔已占用但未结算的额度。
type Reservation struct {
	ID        string
	Subject   Subject
	Resource  string
	Amount    int64
	WindowKey int64
	ExpiresAt time.Time
	Committed bool
	Released  bool
	// Charges 是本次占用在层级链与资源池上的明细。
	Charges []Charge `json:"charges,omitempty"`
	// Borrowed 是本次占用中来自借用的部分。
	Borrowed        int64 `json:"borrowed,omitempty"`
	BorrowWindowKey int64 `json:"borrow_window_key,omitempty"`
}
