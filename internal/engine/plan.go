package engine

import (
	"context"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// SwitchPlan upgrades or downgrades a tenant's plan.
//
//   - mode "immediate": the new quota limits take effect right away. A
//     downgrade never erases usage that already happened: if used exceeds the
//     new limit, new requests are denied until the current window rolls over.
//   - mode "next_window": the current window keeps the previous plan's limits;
//     the new limits apply from the next window onward.
func (e *engine) SwitchPlan(ctx context.Context, tenantID, planID, mode string) (*model.PlanSwitch, error) {
	if mode != "immediate" && mode != "next_window" {
		mode = "immediate"
	}
	toPlan, err := e.store.GetPlan(ctx, planID)
	if err != nil {
		return nil, err
	}
	fromPlan := e.planForTenant(ctx, tenantID)
	fromID := ""
	if fromPlan != nil {
		fromID = fromPlan.ID
	}

	now := time.Now().Unix()
	switchWindow := e.planWindowStart(toPlan, now)

	sw := &model.PlanSwitch{
		TenantID:    tenantID,
		FromPlan:    fromID,
		ToPlan:      planID,
		Mode:        mode,
		At:          now,
		WindowStart: switchWindow,
	}
	if err := e.store.PutPlanSwitch(ctx, sw); err != nil {
		return nil, err
	}

	if mode == "immediate" {
		if err := e.applyPlanRules(ctx, tenantID, toPlan); err != nil {
			return nil, err
		}
	}
	// next_window: rules are applied by the reaper when the window rolls over.
	return sw, nil
}

// applyPlanRules replaces the tenant's rules with the plan's rules.
func (e *engine) applyPlanRules(ctx context.Context, tenantID string, plan *model.Plan) error {
	tenantRules := make([]model.QuotaRule, 0, len(plan.Quotas))
	for _, r := range plan.Quotas {
		if r.Scope.Type == model.ScopeTenant && r.Scope.ID == tenantID {
			tenantRules = append(tenantRules, r)
		}
	}
	_, err := e.rules.SwitchTenantRules(ctx, tenantID, tenantRules)
	return err
}

// pendingPlanSwitch returns the most recent next_window switch whose window
// has not yet rolled over, or nil.
func (e *engine) pendingPlanSwitch(ctx context.Context, tenantID string, now int64) *model.PlanSwitch {
	switches, err := e.store.ListPlanSwitches(ctx, tenantID)
	if err != nil {
		return nil
	}
	var latest *model.PlanSwitch
	for i := range switches {
		sw := switches[i]
		if sw.Mode != "next_window" {
			continue
		}
		toPlan, err := e.store.GetPlan(ctx, sw.ToPlan)
		if err != nil || toPlan == nil {
			continue
		}
		ws := e.planWindowStart(toPlan, now)
		if ws <= sw.WindowStart {
			if latest == nil || sw.At > latest.At {
				latest = sw
			}
		}
	}
	return latest
}

// planWindowStart returns the window start for a plan at now.
func (e *engine) planWindowStart(plan *model.Plan, now int64) int64 {
	for _, r := range plan.Quotas {
		if r.Window > 0 {
			return windowStart(now, r.Window)
		}
	}
	return windowStart(now, time.Minute)
}

// effectiveLimit returns the limit to enforce for a scope, accounting for a
// pending next_window plan switch (the old plan's limit applies in the
// current window).
func (e *engine) effectiveLimit(ctx context.Context, scope model.Scope, resource model.Resource, rule *model.QuotaRule, now int64) int64 {
	if scope.Type != model.ScopeTenant {
		return rule.Limit
	}
	sw := e.pendingPlanSwitch(ctx, scope.ID, now)
	if sw == nil {
		return rule.Limit
	}
	fromPlan, err := e.store.GetPlan(ctx, sw.FromPlan)
	if err != nil || fromPlan == nil {
		return rule.Limit
	}
	for _, r := range fromPlan.Quotas {
		if r.Scope == scope && r.Resource == resource {
			return r.Limit
		}
	}
	return rule.Limit
}
