package quota

import (
	"context"
	"testing"
	"time"
)

// 层级配额：子级用量计入所有上级；任一层耗尽按 租户>团队>调用方 优先级拒绝。
func TestHierarchyConstraints(t *testing.T) {
	e := newTestEngine(t, Options{})
	publishRules(e, 1, toGray100(),
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 10},
		Rule{Scope: ScopeTeam, Resource: "api", Window: WindowMinute, Limit: 6},
		Rule{Scope: ScopeCaller, Resource: "api", Window: WindowMinute, Limit: 3},
	)
	ctx := context.Background()

	// 调用方 c1 最多 3。
	for i := 0; i < 3; i++ {
		if _, r := e.Reserve(ctx, CheckRequest{Subject: subject("t1", "g1", "c1"), Resource: "api", Amount: 1}); !r.Allowed() {
			t.Fatalf("c1 call %d: %+v", i, r)
		}
	}
	_, r := e.Reserve(ctx, CheckRequest{Subject: subject("t1", "g1", "c1"), Resource: "api", Amount: 1})
	if r.Allowed() || r.Reason != DenyCallerQuota {
		t.Fatalf("c1 4th call: want CALLER_QUOTA_EXCEEDED, got %+v", r)
	}

	// 同团队另一个调用方 c2：受团队层 6 约束（c1 已占 3）。
	for i := 0; i < 3; i++ {
		if _, r := e.Reserve(ctx, CheckRequest{Subject: subject("t1", "g1", "c2"), Resource: "api", Amount: 1}); !r.Allowed() {
			t.Fatalf("c2 call %d: %+v", i, r)
		}
	}
	_, r = e.Reserve(ctx, CheckRequest{Subject: subject("t1", "g1", "c2"), Resource: "api", Amount: 1})
	if r.Allowed() || r.Reason != DenyTeamQuota {
		t.Fatalf("c2 4th call: want TEAM_QUOTA_EXCEEDED, got %+v", r)
	}

	// 另一个团队 g2：团队额度独立，但共享租户层（已用 6/10）。
	// 用多个调用方绕开调用方层上限，把租户层占满。
	for i := 0; i < 3; i++ {
		if _, r := e.Reserve(ctx, CheckRequest{Subject: subject("t1", "g2", "c3"), Resource: "api", Amount: 1}); !r.Allowed() {
			t.Fatalf("g2/c3 call %d: %+v", i, r)
		}
	}
	if _, r := e.Reserve(ctx, CheckRequest{Subject: subject("t1", "g2", "c4"), Resource: "api", Amount: 1}); !r.Allowed() {
		t.Fatalf("g2/c4 call: %+v", r)
	}
	_, r = e.Reserve(ctx, CheckRequest{Subject: subject("t1", "g2", "c4"), Resource: "api", Amount: 1})
	if r.Allowed() || r.Reason != DenyTenantQuota {
		t.Fatalf("tenant exhaustion: want TENANT_QUOTA_EXCEEDED, got %+v", r)
	}

	// 子级用量确实计入了租户层。
	rep := e.Usage("t1", "api", time.Now())
	if rep.Reserved != 10 {
		t.Fatalf("tenant reserved = %d, want 10 (children roll up)", rep.Reserved)
	}
}

func toGray100() int { return 100 }
