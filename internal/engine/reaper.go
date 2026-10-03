package engine

import (
	"context"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// ReapExpired expires reservations past their TTL, processes borrow
// repayments/defaults, and applies pending next_window plan switches. It is
// safe to call periodically.
func (e *engine) ReapExpired(ctx context.Context) (int, error) {
	now := time.Now().Unix()
	expired := 0

	// 1. Expire pending reservations past their TTL.
	reservations, err := e.store.ListReservations(ctx)
	if err != nil {
		return 0, err
	}
	for _, res := range reservations {
		if res.Status != model.ReservationPending {
			continue
		}
		if res.ExpiresAt > now {
			continue
		}
		if _, err := e.Cancel(ctx, res.ID); err != nil {
			return 0, err
		}
		// Cancel sets the status to cancelled; mark as expired for clarity.
		res.Status = model.ReservationExpired
		if err := e.store.PutReservation(ctx, res); err != nil {
			return 0, err
		}
		expired++
	}

	// 2. Process borrow repayments and defaults.
	if _, _, err := e.processBorrows(ctx, now); err != nil {
		return 0, err
	}

	// 3. Apply pending next_window plan switches whose window has rolled over.
	if err := e.processPlanSwitches(ctx, now); err != nil {
		return 0, err
	}

	return expired, nil
}

// processPlanSwitches applies pending next_window plan switches once their
// window has rolled over.
func (e *engine) processPlanSwitches(ctx context.Context, now int64) error {
	plans, err := e.store.ListPlans(ctx)
	if err != nil {
		return err
	}
	for _, plan := range plans {
		switches, err := e.store.ListPlanSwitches(ctx, plan.ID)
		if err != nil {
			continue
		}
		// Find the latest unapplied next_window switch for this tenant.
		var latest *model.PlanSwitch
		for i := range switches {
			sw := switches[i]
			if sw.Mode != "next_window" {
				continue
			}
			if latest == nil || sw.At > latest.At {
				latest = sw
			}
		}
		if latest == nil {
			continue
		}
		toPlan, err := e.store.GetPlan(ctx, latest.ToPlan)
		if err != nil || toPlan == nil {
			continue
		}
		ws := e.planWindowStart(toPlan, now)
		if ws > latest.WindowStart {
			// Window rolled over: apply the new plan rules.
			if err := e.applyPlanRules(ctx, latest.TenantID, toPlan); err != nil {
				return err
			}
			latest.Mode = "applied"
			_ = e.store.PutPlanSwitch(ctx, latest)
		}
	}
	return nil
}
