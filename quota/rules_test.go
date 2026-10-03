package quota

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// 灰度发布：0% 时全部走旧版本，100% 全部走新版本，
// 中间比例按租户稳定分流（同一租户多次判定结果一致）。
func TestRuleGrayRollout(t *testing.T) {
	e := newTestEngine(t, Options{})
	ctx := context.Background()

	publishRules(e, 1, 100, Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 10})
	publishRules(e, 2, 0, Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 20})

	// 灰度 0%：所有租户仍按 v1（limit 10）判定。
	if _, r := e.Reserve(ctx, CheckRequest{Subject: subject("ta", "g", "c"), Resource: "api", Amount: 11}); r.Allowed() {
		t.Fatal("gray 0%: v2 limit must not apply")
	}

	// 灰度 100%：v2 生效。
	publishRules(e, 3, 100, Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 20})
	if _, r := e.Reserve(ctx, CheckRequest{Subject: subject("tb", "g", "c"), Resource: "api", Amount: 11}); !r.Allowed() {
		t.Fatal("gray 100%: v2 limit should apply")
	}

	// 灰度 50%：每个租户的分流结果稳定，且两个版本都有租户命中。
	publishRules(e, 4, 50, Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 30})
	seen := map[uint64]int{}
	for i := 0; i < 200; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		rs1 := e.rules.For(tenant, 0, time.Now())
		rs2 := e.rules.For(tenant, 0, time.Now())
		if rs1.Version != rs2.Version {
			t.Fatalf("tenant %s saw inconsistent versions %d/%d", tenant, rs1.Version, rs2.Version)
		}
		seen[rs1.Version]++
	}
	if len(seen) != 2 {
		t.Fatalf("gray 50%% should split tenants across 2 versions, got %v", seen)
	}
}

// 宽限期：钉住旧版本的请求在宽限期内按旧版本判定，过期后切换到新版本。
func TestRulePinGracePeriod(t *testing.T) {
	now := t0
	e := newTestEngine(t, Options{Now: func() time.Time { return now }})
	ctx := context.Background()

	e.UpdateRules(RuleSet{
		Version: 1, GrayPercent: 100, CreatedAt: now, GracePeriod: time.Minute,
		Rules: []Rule{{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 10}},
	})
	now = now.Add(30 * time.Second)
	e.UpdateRules(RuleSet{
		Version: 2, GrayPercent: 100, CreatedAt: now, GracePeriod: time.Minute,
		Rules: []Rule{{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 100}},
	})

	// 宽限期内：钉住 v1 的请求按 limit=10 判定。
	_, r := e.Reserve(ctx, CheckRequest{
		Subject: subject("t1", "g", "c"), Resource: "api", Amount: 11, RuleVersion: 1,
	})
	if r.Allowed() || r.RuleVersion != 1 {
		t.Fatalf("pinned v1 within grace: want deny with version 1, got %+v", r)
	}
	// 未钉住的请求按 v2（limit=100）放行。
	if _, r := e.Reserve(ctx, CheckRequest{
		Subject: subject("t2", "g", "c"), Resource: "api", Amount: 11,
	}); !r.Allowed() {
		t.Fatalf("unpinned request should use v2: %+v", r)
	}
	// 宽限期过后：v1 钉住失效，按 v2 判定。
	now = now.Add(2 * time.Minute)
	if _, r := e.Reserve(ctx, CheckRequest{
		Subject: subject("t3", "g", "c"), Resource: "api", Amount: 11, RuleVersion: 1,
	}); !r.Allowed() {
		t.Fatalf("pin beyond grace should fall back to latest: %+v", r)
	}
}
