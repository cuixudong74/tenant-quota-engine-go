package engine

import (
	"context"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// TestHierarchyAllScopesChecked verifies that a request must satisfy every
// configured scope (caller, team, tenant) and that usage propagates to all.
func TestHierarchyAllScopesChecked(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 1000, time.Minute),
		teamRule("team1", "api_calls", 100, time.Minute),
		callerRule("c1", "api_calls", 10, time.Minute),
	})

	// Reserve 10: should pass all scopes.
	res, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 10,
	})
	if err != nil || !res.Allowed {
		t.Fatalf("reserve 10 failed: %v %v", err, res)
	}
	env.engine.Confirm(ctx, res.Reservation.ID, 10)

	// Caller limit is 10, so another 10 should be denied at caller scope.
	res2, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 10,
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res2.Allowed {
		t.Fatalf("expected deny at caller scope, got allowed")
	}
	if res2.DenyScope.Type != model.ScopeCaller {
		t.Fatalf("expected deny scope caller, got %s", res2.DenyScope.Type)
	}

	// Team usage should reflect 10 (propagated from caller).
	teamUsage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTeam, ID: "team1"}, "api_calls")
	if teamUsage.Used != 10 {
		t.Fatalf("team used mismatch: got %d, want 10", teamUsage.Used)
	}

	// Tenant usage should also reflect 10.
	tenantUsage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if tenantUsage.Used != 10 {
		t.Fatalf("tenant used mismatch: got %d, want 10", tenantUsage.Used)
	}
}

// TestHierarchyTeamLimitDenies verifies that a team-level limit denies even
// when the caller and tenant have capacity.
func TestHierarchyTeamLimitDenies(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 1000, time.Minute),
		teamRule("team1", "api_calls", 50, time.Minute),
		callerRule("c1", "api_calls", 100, time.Minute),
	})

	// Consume 50 at team level via caller c1.
	for i := 0; i < 5; i++ {
		res, _ := env.engine.Reserve(ctx, &ReserveRequest{
			TenantID: "t1", TeamID: "team1", CallerID: "c1",
			Resource: "api_calls", Amount: 10,
		})
		if !res.Allowed {
			t.Fatalf("reserve %d denied: %s", i, res.Reason)
		}
		env.engine.Confirm(ctx, res.Reservation.ID, 10)
	}

	// Now team is at 50/50. Another 10 from a different caller should be denied
	// at team scope.
	res, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c2",
		Resource: "api_calls", Amount: 10,
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res.Allowed {
		t.Fatalf("expected deny at team scope, got allowed")
	}
	if res.DenyScope.Type != model.ScopeTeam {
		t.Fatalf("expected deny scope team, got %s", res.DenyScope.Type)
	}
}

// TestHierarchyTenantLimitDenies verifies the tenant-level limit is the final
// arbiter.
func TestHierarchyTenantLimitDenies(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 30, time.Minute),
		teamRule("team1", "api_calls", 100, time.Minute),
		callerRule("c1", "api_calls", 100, time.Minute),
	})

	// Consume 30 at tenant level.
	for i := 0; i < 3; i++ {
		res, _ := env.engine.Reserve(ctx, &ReserveRequest{
			TenantID: "t1", TeamID: "team1", CallerID: "c1",
			Resource: "api_calls", Amount: 10,
		})
		if !res.Allowed {
			t.Fatalf("reserve %d denied: %s", i, res.Reason)
		}
		env.engine.Confirm(ctx, res.Reservation.ID, 10)
	}

	// Tenant is at limit. Should be denied at tenant scope.
	res, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team2", CallerID: "c3",
		Resource: "api_calls", Amount: 10,
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res.Allowed {
		t.Fatalf("expected deny at tenant scope, got allowed")
	}
	if res.DenyScope.Type != model.ScopeTenant {
		t.Fatalf("expected deny scope tenant, got %s", res.DenyScope.Type)
	}
}
