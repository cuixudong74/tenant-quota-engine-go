package engine

import (
	"context"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

func putPlan(t *testing.T, env *testEnv, plan *model.Plan) {
	t.Helper()
	if err := env.store.PutPlan(context.Background(), plan); err != nil {
		t.Fatalf("put plan: %v", err)
	}
}

// TestPlanUpgradeImmediate verifies that an upgrade takes effect immediately.
func TestPlanUpgradeImmediate(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	freePlan := &model.Plan{
		ID: "free", Name: "Free",
		Quotas: []model.QuotaRule{tenantRule("t1", "api_calls", 100, time.Minute)},
	}
	proPlan := &model.Plan{
		ID: "pro", Name: "Pro",
		Quotas: []model.QuotaRule{tenantRule("t1", "api_calls", 1000, time.Minute)},
	}
	putPlan(t, env, freePlan)
	putPlan(t, env, proPlan)

	// Start with free plan.
	env.engine.SwitchPlan(ctx, "t1", "free", "immediate")

	// Consume 100 (free limit).
	res, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 100,
	})
	env.engine.Confirm(ctx, res.Reservation.ID, 100)

	// 101 should be denied on free.
	res2, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 1,
	})
	if res2.Allowed {
		t.Fatalf("expected deny on free plan")
	}

	// Upgrade to pro: new limit 1000 takes effect immediately.
	env.engine.SwitchPlan(ctx, "t1", "pro", "immediate")

	// Now 500 should be allowed.
	res3, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 500,
	})
	if !res3.Allowed {
		t.Fatalf("expected allow after upgrade, got %s", res3.Reason)
	}
}

// TestPlanDowngradePreservesOverage verifies that a downgrade does not erase
// usage that already happened: the overage remains and new requests are denied.
func TestPlanDowngradePreservesOverage(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	proPlan := &model.Plan{
		ID: "pro", Name: "Pro",
		Quotas: []model.QuotaRule{tenantRule("t1", "api_calls", 1000, time.Minute)},
	}
	freePlan := &model.Plan{
		ID: "free", Name: "Free",
		Quotas: []model.QuotaRule{tenantRule("t1", "api_calls", 100, time.Minute)},
	}
	putPlan(t, env, proPlan)
	putPlan(t, env, freePlan)

	env.engine.SwitchPlan(ctx, "t1", "pro", "immediate")

	// Consume 500 on pro.
	res, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 500,
	})
	env.engine.Confirm(ctx, res.Reservation.ID, 500)

	// Downgrade to free (limit 100). The 500 usage remains.
	env.engine.SwitchPlan(ctx, "t1", "free", "immediate")

	// New requests should be denied because usage (500) > new limit (100).
	res2, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 1,
	})
	if res2.Allowed {
		t.Fatalf("expected deny after downgrade (overage preserved)")
	}
	if res2.Reason != model.DenyQuotaExhausted {
		t.Fatalf("expected quota_exhausted, got %s", res2.Reason)
	}

	// Usage should still show 500 (not erased).
	usage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if usage.Used != 500 {
		t.Fatalf("usage was erased after downgrade: got %d, want 500", usage.Used)
	}
}

// TestPlanDowngradeNextWindow verifies that next_window mode keeps the old
// limit for the current window and applies the new limit after rollover.
func TestPlanDowngradeNextWindow(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	proPlan := &model.Plan{
		ID: "pro", Name: "Pro",
		Quotas: []model.QuotaRule{tenantRule("t1", "api_calls", 1000, time.Minute)},
	}
	freePlan := &model.Plan{
		ID: "free", Name: "Free",
		Quotas: []model.QuotaRule{tenantRule("t1", "api_calls", 100, time.Minute)},
	}
	putPlan(t, env, proPlan)
	putPlan(t, env, freePlan)

	env.engine.SwitchPlan(ctx, "t1", "pro", "immediate")

	// Consume 200 on pro.
	res, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 200,
	})
	env.engine.Confirm(ctx, res.Reservation.ID, 200)

	// Downgrade to free in next_window mode.
	env.engine.SwitchPlan(ctx, "t1", "free", "next_window")

	// Current window: 200 used, old pro limit 1000. 300 more should pass.
	res2, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 300,
	})
	if !res2.Allowed {
		t.Fatalf("expected allow in current window (old limit), got %s", res2.Reason)
	}
}
