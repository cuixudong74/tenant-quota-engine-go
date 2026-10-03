package engine

import (
	"context"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// TestReservationExpiryReleasesQuota verifies that expired reservations
// release their quota back.
func TestReservationExpiryReleasesQuota(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 100, time.Minute),
	})

	// Reserve with a very short TTL.
	res, err := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 80, TTL: 50 * time.Millisecond,
	})
	if err != nil || !res.Allowed {
		t.Fatalf("reserve failed: %v %v", err, res)
	}

	// Wait for expiry.
	time.Sleep(100 * time.Millisecond)

	// Reap expired reservations.
	expired, err := env.engine.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expected 1 expired, got %d", expired)
	}

	// The reservation should be expired and quota released.
	usage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if usage.Reserved != 0 {
		t.Fatalf("expected reserved=0 after expiry, got %d", usage.Reserved)
	}

	// Should be able to reserve again.
	res2, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 100,
	})
	if !res2.Allowed {
		t.Fatalf("expected allow after expiry, got %s", res2.Reason)
	}
}

// TestReapDoesNotAffectActiveReservations verifies that reaping does not
// touch reservations that are still valid.
func TestReapDoesNotAffectActiveReservations(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", 100, time.Minute),
	})

	res, _ := env.engine.Reserve(ctx, &ReserveRequest{
		TenantID: "t1", TeamID: "team1", CallerID: "c1",
		Resource: "api_calls", Amount: 50, TTL: 5 * time.Minute,
	})

	expired, err := env.engine.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if expired != 0 {
		t.Fatalf("expected 0 expired, got %d", expired)
	}

	// Reservation should still be pending.
	got, _ := env.store.GetReservation(ctx, res.Reservation.ID)
	if got.Status != model.ReservationPending {
		t.Fatalf("expected pending, got %s", got.Status)
	}
}
