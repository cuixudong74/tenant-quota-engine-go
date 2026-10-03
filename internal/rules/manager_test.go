package rules

import (
	"context"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

func TestPublishAndActivate(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := NewManager(st)
	ctx := context.Background()

	v1, err := rm.Publish(ctx, "v1", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if v1.Status != "draft" {
		t.Fatalf("expected draft, got %s", v1.Status)
	}

	if _, err := rm.Activate(ctx, v1.Version, 0, 0); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if rm.ActiveVersion() != 1 {
		t.Fatalf("expected active version 1, got %d", rm.ActiveVersion())
	}

	rules, ver, err := rm.Resolve(ctx, "t1", "team1", "c1", "api_calls")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if ver != 1 || len(rules) != 1 || rules[0].Limit != 100 {
		t.Fatalf("unexpected resolve: ver=%d rules=%+v", ver, rules)
	}
}

func TestGrayRelease(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := NewManager(st)
	ctx := context.Background()

	// v1: limit 100
	v1, _ := rm.Publish(ctx, "v1", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
	})
	rm.Activate(ctx, v1.Version, 0, 0)

	// v2: limit 200, gray 50%
	v2, _ := rm.Publish(ctx, "v2", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 200, Window: time.Minute},
	})
	rm.Activate(ctx, v2.Version, 50, time.Hour)

	if rm.GrayVersion() != 2 {
		t.Fatalf("expected gray version 2, got %d", rm.GrayVersion())
	}

	// Resolve many callers: some should see v2 (limit 200), some v1 (limit 100).
	seenV1 := false
	seenV2 := false
	for i := 0; i < 100; i++ {
		callerID := "caller_" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		rules, ver, _ := rm.Resolve(ctx, "t1", "team1", callerID, "api_calls")
		if len(rules) == 0 {
			t.Fatalf("no rules for caller %s", callerID)
		}
		if ver == 2 {
			seenV2 = true
			if rules[0].Limit != 200 {
				t.Fatalf("v2 should have limit 200, got %d", rules[0].Limit)
			}
		} else {
			seenV1 = true
			if rules[0].Limit != 100 {
				t.Fatalf("v1 should have limit 100, got %d", rules[0].Limit)
			}
		}
	}
	if !seenV1 || !seenV2 {
		t.Fatalf("gray release should show both versions: v1=%v v2=%v", seenV1, seenV2)
	}
}

func TestGrayPromoteToFull(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := NewManager(st)
	ctx := context.Background()

	v1, _ := rm.Publish(ctx, "v1", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
	})
	rm.Activate(ctx, v1.Version, 0, 0)

	v2, _ := rm.Publish(ctx, "v2", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 200, Window: time.Minute},
	})
	rm.Activate(ctx, v2.Version, 50, time.Hour)

	// Promote gray to full.
	rm.Activate(ctx, v2.Version, 0, 0)
	if rm.ActiveVersion() != 2 {
		t.Fatalf("expected active version 2 after promote, got %d", rm.ActiveVersion())
	}
	if rm.GrayVersion() != 0 {
		t.Fatalf("expected no gray version after promote, got %d", rm.GrayVersion())
	}

	// All callers should now see v2.
	for i := 0; i < 10; i++ {
		rules, ver, _ := rm.Resolve(ctx, "t1", "team1", "c"+string(rune('0'+i)), "api_calls")
		if ver != 2 || rules[0].Limit != 200 {
			t.Fatalf("expected v2 limit 200 for all callers, got ver=%d limit=%d", ver, rules[0].Limit)
		}
	}
}

func TestRollback(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := NewManager(st)
	ctx := context.Background()

	v1, _ := rm.Publish(ctx, "v1", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
	})
	rm.Activate(ctx, v1.Version, 0, 0)

	v2, _ := rm.Publish(ctx, "v2", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 200, Window: time.Minute},
	})
	rm.Activate(ctx, v2.Version, 0, 0)

	// Rollback to v1.
	restored, err := rm.Rollback(ctx)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if restored.Version != 1 {
		t.Fatalf("expected restored version 1, got %d", restored.Version)
	}
	if rm.ActiveVersion() != 1 {
		t.Fatalf("expected active version 1 after rollback, got %d", rm.ActiveVersion())
	}
}

func TestConsistentVersion(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := NewManager(st)
	ctx := context.Background()

	// v1 has two rules for the same resource at different scopes.
	v1, _ := rm.Publish(ctx, "v1", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
		{Scope: model.Scope{Type: model.ScopeTeam, ID: "team1"}, Resource: "api_calls", Limit: 50, Window: time.Minute},
	})
	rm.Activate(ctx, v1.Version, 0, 0)

	rules, ver, _ := rm.Resolve(ctx, "t1", "team1", "c1", "api_calls")
	if len(rules) != 2 {
		t.Fatalf("expected 2 rules from same version, got %d", len(rules))
	}
	// All rules must share the same version.
	for _, r := range rules {
		if r.Version != ver {
			t.Fatalf("rule version %d != resolved version %d", r.Version, ver)
		}
	}
}
