package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// TestConcurrentReserveNoOvercommit hammers the engine with concurrent
// reservations and verifies that usage never exceeds the limit.
func TestConcurrentReserveNoOvercommit(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	const limit int64 = 1000
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", limit, time.Minute),
	})

	const goroutines = 200
	const perCall int64 = 10
	var wg sync.WaitGroup
	var allowedCount, deniedCount int64
	var totalAllowed int64
	var mu sync.Mutex

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := env.engine.Reserve(ctx, &ReserveRequest{
				TenantID: "t1", TeamID: "team1", CallerID: "c1",
				Resource: "api_calls", Amount: perCall,
			})
			if err != nil {
				t.Errorf("reserve: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if res.Allowed {
				allowedCount++
				totalAllowed += perCall
				_, _ = env.engine.Confirm(ctx, res.Reservation.ID, perCall)
			} else {
				deniedCount++
			}
		}()
	}
	wg.Wait()

	if totalAllowed > limit {
		t.Fatalf("overcommit: allowed %d > limit %d", totalAllowed, limit)
	}
	if allowedCount+deniedCount != goroutines {
		t.Fatalf("missing responses: allowed=%d denied=%d", allowedCount, deniedCount)
	}

	usage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if usage.Used != totalAllowed {
		t.Fatalf("used mismatch: got %d, want %d", usage.Used, totalAllowed)
	}
	if usage.Used > limit {
		t.Fatalf("used exceeds limit: %d > %d", usage.Used, limit)
	}
}

// TestConcurrentReserveCancelNoLeaks verifies that concurrent reserve+cancel
// cycles never leak quota.
func TestConcurrentReserveCancelNoLeaks(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	const limit int64 = 500
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", limit, time.Minute),
	})

	const goroutines = 40
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := env.engine.Reserve(ctx, &ReserveRequest{
				TenantID: "t1", TeamID: "team1", CallerID: "c1",
				Resource: "api_calls", Amount: 20,
			})
			if err != nil || !res.Allowed {
				return
			}
			// Cancel half, confirm half.
			if i%2 == 0 {
				_, _ = env.engine.Cancel(ctx, res.Reservation.ID)
			} else {
				_, _ = env.engine.Confirm(ctx, res.Reservation.ID, 20)
			}
		}()
	}
	wg.Wait()

	usage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	// After all operations, reserved must be 0 (no leaks).
	if usage.Reserved != 0 {
		t.Fatalf("leaked reserved quota: %d", usage.Reserved)
	}
	if usage.Used > limit {
		t.Fatalf("used exceeds limit: %d", usage.Used)
	}
}

// TestConcurrentMixedScopes verifies concurrent reservations across different
// callers all propagate to the tenant counter accurately.
func TestConcurrentMixedScopes(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	const tenantLimit int64 = 1000
	env.publishTenantRules(t, "t1", []model.QuotaRule{
		tenantRule("t1", "api_calls", tenantLimit, time.Minute),
	})

	const callers = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	totalUsed := int64(0)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			callerID := "c" + string(rune('a'+idx))
			res, err := env.engine.Reserve(ctx, &ReserveRequest{
				TenantID: "t1", TeamID: "team1", CallerID: callerID,
				Resource: "api_calls", Amount: 10,
			})
			if err != nil || !res.Allowed {
				return
			}
			_, _ = env.engine.Confirm(ctx, res.Reservation.ID, 10)
			mu.Lock()
			totalUsed += 10
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	usage, _ := env.engine.GetUsage(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if usage.Used != totalUsed {
		t.Fatalf("tenant used mismatch: got %d, want %d", usage.Used, totalUsed)
	}
}
