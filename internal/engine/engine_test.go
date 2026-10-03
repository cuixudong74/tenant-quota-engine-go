package engine

import (
	"context"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

func TestReserveConfirm(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 100, time.Minute),
	})

	res, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 30,
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("expected allowed, got denied: %s", res.Reason)
	}
	if res.Reservation == nil || res.Reservation.Amount != 30 {
		t.Fatalf("unexpected reservation: %+v", res.Reservation)
	}

	// Confirm the reservation.
	confirmed, err := env.engine.Confirm(ctx, res.Reservation.ID, 30)
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if confirmed.Status != model.ReservationConfirmed || confirmed.Settled != 30 {
		t.Fatalf("unexpected confirmed state: %+v", confirmed)
	}

	// Usage should reflect 30 used.
	usage, err := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if err != nil {
		t.Fatalf("get usage: %v", err)
	}
	if usage.Used != 30 || usage.Remaining != 70 {
		t.Fatalf("expected used=30 remaining=70, got used=%d remaining=%d", usage.Used, usage.Remaining)
	}
}

func TestReserveCancelReleasesQuota(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 100, time.Minute),
	})

	res, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 60,
	})
	if err != nil || !res.Allowed {
		t.Fatalf("reserve failed: %v %v", err, res)
	}

	// Cancel should release the quota.
	cancelled, err := env.engine.Cancel(ctx, res.Reservation.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancelled.Status != model.ReservationCancelled {
		t.Fatalf("expected cancelled, got %s", cancelled.Status)
	}

	// Now we should be able to reserve again up to the limit.
	res2, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 100,
	})
	if err != nil || !res2.Allowed {
		t.Fatalf("reserve after cancel failed: %v %v", err, res2)
	}
}

func TestReserveDenyExhausted(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 100, time.Minute),
	})

	// Reserve 60, confirm -> 60 used.
	res, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 60,
	})
	env.engine.Confirm(ctx, res.Reservation.ID, 60)

	// Reserve 50 more -> should be denied (60+50 > 100).
	res2, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c2",
		Resource: "api_calls", Amount: 50,
	})
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if res2.Allowed {
		t.Fatalf("expected deny, got allowed")
	}
	if res2.Reason != model.DenyQuotaExhausted {
		t.Fatalf("expected quota_exhausted, got %s", res2.Reason)
	}
	if res2.DenyScope.Type != model.ScopeTenant {
		t.Fatalf("expected deny scope tenant, got %s", res2.DenyScope.Type)
	}

	// Deny count should be 1.
	usage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if usage.Denies != 1 {
		t.Fatalf("expected 1 deny, got %d", usage.Denies)
	}
}

func TestConfirmIdempotent(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 100, time.Minute),
	})

	res, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 30,
	})
	env.engine.Confirm(ctx, res.Reservation.ID, 30)
	// Confirm again -> should be no-op.
	_, err := env.engine.Confirm(ctx, res.Reservation.ID, 30)
	if err != nil {
		t.Fatalf("second confirm: %v", err)
	}
	usage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if usage.Used != 30 {
		t.Fatalf("expected used=30 after idempotent confirm, got %d", usage.Used)
	}
}

func TestPartialSettlement(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 100, time.Minute),
	})

	res, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 50,
	})
	// Settle only 20.
	env.engine.Confirm(ctx, res.Reservation.ID, 20)

	usage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if usage.Used != 20 {
		t.Fatalf("expected used=20, got %d", usage.Used)
	}
	if usage.Reserved != 0 {
		t.Fatalf("expected reserved=0 after partial settle, got %d", usage.Reserved)
	}
	if usage.Remaining != 80 {
		t.Fatalf("expected remaining=80, got %d", usage.Remaining)
	}
}
