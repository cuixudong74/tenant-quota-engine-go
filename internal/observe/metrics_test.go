package observe

import (
	"context"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/rules"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

func TestMetricsQuery(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := rules.NewManager(st)
	ctx := context.Background()

	v, _ := rm.Publish(ctx, "test", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
	})
	rm.Activate(ctx, v.Version, 0, 0)

	m := NewMetrics(st, rm)

	// Apply some usage directly.
	key := "c:tenant:t1:api_calls:" + itoa(windowStart(time.Now().Unix(), time.Minute))
	st.ApplyCounterDelta(ctx, key, store.CounterDelta{UsedDelta: 30, DeniesDelta: 2, RequestsDelta: 5})

	u, err := m.Query(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if u.Used != 30 {
		t.Fatalf("expected used 30, got %d", u.Used)
	}
	if u.Denies != 2 {
		t.Fatalf("expected denies 2, got %d", u.Denies)
	}
	if u.Requests != 5 {
		t.Fatalf("expected requests 5, got %d", u.Requests)
	}
	if u.Remaining != 70 {
		t.Fatalf("expected remaining 70, got %d", u.Remaining)
	}
}

func TestMetricsDenyCount(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := rules.NewManager(st)
	ctx := context.Background()

	v, _ := rm.Publish(ctx, "test", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
	})
	rm.Activate(ctx, v.Version, 0, 0)

	m := NewMetrics(st, rm)
	key := "c:tenant:t1:api_calls:" + itoa(windowStart(time.Now().Unix(), time.Minute))
	st.ApplyCounterDelta(ctx, key, store.CounterDelta{DeniesDelta: 7})

	count, err := m.DenyCount(ctx, model.Scope{Type: model.ScopeTenant, ID: "t1"}, "api_calls")
	if err != nil {
		t.Fatalf("deny count: %v", err)
	}
	if count != 7 {
		t.Fatalf("expected 7 denies, got %d", count)
	}
}

func TestMetricsQueryTenant(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := rules.NewManager(st)
	ctx := context.Background()

	v, _ := rm.Publish(ctx, "test", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "tokens", Limit: 1000, Window: time.Hour},
	})
	rm.Activate(ctx, v.Version, 0, 0)

	m := NewMetrics(st, rm)
	usages, err := m.QueryTenant(ctx, "t1")
	if err != nil {
		t.Fatalf("query tenant: %v", err)
	}
	if len(usages) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(usages))
	}
}
