package engine

import (
	"context"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// TestBorrowRepaymentRestoresParentLent verifies that when a borrow is repaid,
// the parent's lent amount is restored.
func TestBorrowRepaymentRestoresParentLent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	callerR := callerRule("c1", "api_calls", 50, time.Second)
	callerR.BorrowEnabled = true
	callerR.BorrowLimit = 200
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 1000, time.Second),
		teamRule("team1", "api_calls", 200, time.Second),
		callerR,
	})

	// Borrow 80 (caller limit 50, so 30 borrowed).
	res, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 80,
	})
	if !res.Allowed {
		t.Fatalf("expected allow via borrow, got %s", res.Reason)
	}
	env.engine.Confirm(ctx, res.Reservation.ID, 80)

	// Team lent should be 30.
	teamUsage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTeam, ID: "team1"}, "api_calls")
	if teamUsage.Lent != 30 {
		t.Fatalf("team lent mismatch: got %d, want 30", teamUsage.Lent)
	}

	// Wait for window rollover and reap.
	time.Sleep(1100 * time.Millisecond)
	env.engine.ReapExpired(ctx)

	// After repayment, team lent should be reduced (or zero).
	teamUsage2, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTeam, ID: "team1"}, "api_calls")
	if teamUsage2.Lent >= 30 {
		t.Fatalf("team lent should decrease after repayment: got %d", teamUsage2.Lent)
	}
}

// TestConcurrentConfirmCancelIdempotent verifies that concurrent confirm and
// cancel on the same reservation are idempotent and don't corrupt state.
func TestConcurrentConfirmCancelIdempotent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 100, time.Minute),
	})

	res, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 50,
	})

	// Concurrent confirm and cancel.
	done := make(chan error, 2)
	go func() {
		_, err := env.engine.Confirm(ctx, res.Reservation.ID, 50)
		done <- err
	}()
	go func() {
		_, err := env.engine.Cancel(ctx, res.Reservation.ID)
		done <- err
	}()
	<-done
	<-done

	// Final state should be consistent: either confirmed or cancelled,
	// and reserved should be 0.
	usage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if usage.Reserved != 0 {
		t.Fatalf("reserved should be 0 after concurrent ops, got %d", usage.Reserved)
	}
}

// TestNoRuleConfigured verifies that requests without configured rules are
// rejected with a distinguishable reason.
func TestNoRuleConfigured(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	// Publish rules for a different tenant.
	env.publishTenantRules(t, "other", []model.QuotaRule{
		tenantRule("other", "api_calls", 100, time.Minute),
	})

	res, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 10,
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res.Allowed {
		t.Fatalf("expected deny (no rule), got allowed")
	}
	if res.Reason != model.DenyNoRule {
		t.Fatalf("expected no_rule_configured, got %s", res.Reason)
	}
}

// TestInvalidAmount verifies that zero or negative amounts are rejected.
func TestInvalidAmount(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 100, time.Minute),
	})

	res, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 0,
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res.Allowed || res.Reason != model.DenyAmountInvalid {
		t.Fatalf("expected amount_invalid, got allowed=%v reason=%s", res.Allowed, res.Reason)
	}
}
