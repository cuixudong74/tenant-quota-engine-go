package engine

import (
	"context"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

// borrowDecision describes the outcome of a borrow evaluation for one scope.
type borrowDecision struct {
	borrowedAmount int64       // amount covered by borrowing from the parent
	parentScope    model.Scope // scope that lent the amount
	parentKey      string      // counter key of the parent at borrow time
	shortfall      int64       // amount the child could not cover from its own quota
}

// tryBorrow computes how much of the shortfall can be covered by borrowing
// from the parent. It is capped by the child's borrow limit (max outstanding)
// and the parent's available surplus. A result of 0 means borrowing cannot
// cover the shortfall.
func tryBorrow(shortfall, currentBorrowed, borrowLimit, parentAvailable int64) int64 {
	can := shortfall
	if borrowLimit > 0 {
		if maxExtra := borrowLimit - currentBorrowed; maxExtra < can {
			can = maxExtra
		}
	}
	if can > parentAvailable {
		can = parentAvailable
	}
	if can < 0 {
		can = 0
	}
	return can
}

// processBorrows handles repayment and default for all active borrow records.
// It is called periodically by the reaper. Repayment is attempted when the
// child scope has crossed into a new window since the borrow; the new window's
// quota is reduced by the repayment amount (repayment_due), and the parent's
// lent amount is restored. If the debt remains outstanding after the plan's
// grace windows, the tenant is downgraded instead of overdrawing forever.
func (e *engine) processBorrows(ctx context.Context, now int64) (repaid int64, defaulted int, err error) {
	borrows, err := e.store.ListBorrows(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, br := range borrows {
		if br.Status != "active" {
			continue
		}
		outstanding := br.Amount - br.Repaid
		if outstanding <= 0 {
			br.Status = "repaid"
			if err := e.store.PutBorrow(ctx, br); err != nil {
				return 0, 0, err
			}
			continue
		}

		childRule := e.ruleFor(br.ChildScope, br.Resource)
		if childRule == nil {
			continue
		}
		childWS := windowStart(now, childRule.Window)
		if childWS <= br.WindowStart {
			continue // still within the borrow window
		}

		plan := e.planForTenant(ctx, br.TenantID)
		repayRatio := 0.0
		graceWindows := 0
		if plan != nil {
			repayRatio = plan.RepayRatio
			graceWindows = plan.GraceWindows
		}

		repay, rerr := e.repayFromWindow(ctx, br, childRule, childWS, now, repayRatio)
		if rerr != nil {
			return 0, 0, rerr
		}
		repaid += repay

		if br.Amount-br.Repaid > 0 {
			br.WindowsOverdue++
			if graceWindows > 0 && br.WindowsOverdue >= graceWindows {
				if err := e.markDefaulted(ctx, br, plan); err != nil {
					return 0, 0, err
				}
				defaulted++
			}
		}
	}
	return repaid, defaulted, nil
}

// repayFromWindow repays as much as possible from the new window's quota.
// The new window counter is debited via repayment_due (reducing its available),
// the borrow record is updated, and the parent's lent amount is restored.
func (e *engine) repayFromWindow(ctx context.Context, br *model.BorrowRecord, childRule *model.QuotaRule, newWS, now int64, repayRatio float64) (int64, error) {
	outstanding := br.Amount - br.Repaid
	newKey := counterKey(br.ChildScope, br.Resource, newWS)

	counters, err := e.store.GetCounters(ctx, []string{newKey})
	if err != nil {
		return 0, err
	}
	newCounter := counters[newKey]
	var newAvail int64
	if newCounter != nil {
		newAvail = newCounter.Available(childRule.Limit)
	} else {
		newAvail = childRule.Limit
	}

	maxRepay := outstanding
	if repayRatio > 0 {
		if r := int64(float64(childRule.Limit) * repayRatio); r < maxRepay {
			maxRepay = r
		}
	}
	if maxRepay > newAvail {
		maxRepay = newAvail
	}
	if maxRepay <= 0 {
		return 0, nil
	}

	if err := e.store.ApplyCounterDelta(ctx, newKey, store.CounterDelta{RepaymentDelta: maxRepay}); err != nil {
		return 0, err
	}

	br.Repaid += maxRepay
	if br.Repaid >= br.Amount {
		br.Status = "repaid"
	}
	if err := e.store.PutBorrow(ctx, br); err != nil {
		return 0, err
	}

	// Restore the parent's lent amount from the parent's current window.
	parentRule := e.ruleFor(br.ParentScope, br.Resource)
	if parentRule != nil {
		parentWS := windowStart(now, parentRule.Window)
		parentKey := counterKey(br.ParentScope, br.Resource, parentWS)
		if err := e.store.ApplyCounterDelta(ctx, parentKey, store.CounterDelta{LentDelta: -maxRepay}); err != nil {
			return 0, err
		}
	}
	return maxRepay, nil
}

// markDefaulted applies the configured downgrade policy for a tenant that
// cannot repay its borrow within the grace windows.
func (e *engine) markDefaulted(ctx context.Context, br *model.BorrowRecord, plan *model.Plan) error {
	br.Status = "defaulted"
	if err := e.store.PutBorrow(ctx, br); err != nil {
		return err
	}
	if plan != nil && plan.DowngradePlan != "" {
		if _, err := e.SwitchPlan(ctx, br.TenantID, plan.DowngradePlan, "immediate"); err != nil {
			return err
		}
	}
	return nil
}

// ruleFor returns the active rule for the given scope/resource, or nil.
func (e *engine) ruleFor(scope model.Scope, resource model.Resource) *model.QuotaRule {
	rules, _, err := e.rules.Resolve(context.Background(), tenantOf(scope), teamOf(scope), callerOf(scope), resource)
	if err != nil {
		return nil
	}
	return ruleForScope(rules, scope)
}

// planForTenant returns the plan configured for a tenant, or nil.
func (e *engine) planForTenant(ctx context.Context, tenantID string) *model.Plan {
	if tenantID == "" {
		return nil
	}
	p, err := e.store.GetPlan(ctx, tenantID)
	if err != nil {
		return nil
	}
	return p
}

// tenantOf extracts the tenant id from a scope.
func tenantOf(scope model.Scope) string {
	if scope.Type == model.ScopeTenant {
		return scope.ID
	}
	return ""
}

func teamOf(scope model.Scope) string {
	if scope.Type == model.ScopeTeam {
		return scope.ID
	}
	return ""
}

func callerOf(scope model.Scope) string {
	if scope.Type == model.ScopeCaller {
		return scope.ID
	}
	return ""
}

// ensureNow returns the current unix time, allowing tests to inject a clock.
func ensureNow(t time.Time) int64 {
	if t.IsZero() {
		return time.Now().Unix()
	}
	return t.Unix()
}
