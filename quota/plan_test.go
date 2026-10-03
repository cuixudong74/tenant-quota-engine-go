package quota

import (
	"context"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// 升级：窗口中段切换到更高套餐，新增额度按折算立即生效。
func TestPlanUpgradeMidWindow(t *testing.T) {
	now := t0
	e := newTestEngine(t, Options{Now: func() time.Time { return now }})
	publishRules(e, 1, 100,
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 100},
	)
	ctx := context.Background()
	sub := subject("t1", "g1", "c1")

	if r := e.ChangePlan(ctx, "t1", "free", now); !r.Allowed() {
		t.Fatalf("assign free: %+v", r)
	}
	// 用掉 60。
	res, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 60})
	if !r.Allowed() {
		t.Fatalf("reserve 60: %+v", r)
	}
	if r := e.Commit(ctx, res.ID); !r.Allowed() {
		t.Fatalf("commit: %+v", r)
	}
	// 窗口过半时升级到 pro（1000）：折算额度 = 100*0.5 + 1000*0.5 = 550。
	now = t0.Add(30 * time.Second)
	if r := e.ChangePlan(ctx, "t1", "pro", now); !r.Allowed() {
		t.Fatalf("upgrade: %+v", r)
	}
	rep := e.Usage("t1", "api", now)
	if rep.Limit != 550 {
		t.Fatalf("prorated limit = %d, want 550", rep.Limit)
	}
	if rep.Remaining != 490 {
		t.Fatalf("remaining = %d, want 490 (new quota effective immediately)", rep.Remaining)
	}
	// 新增额度立即可用。
	if _, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 490}); !r.Allowed() {
		t.Fatalf("reserve 490 after upgrade: %+v", r)
	}
}

// 降级：已发生的超额用量不消失，剩余为 0，下一窗口才按新额度恢复。
func TestPlanDowngradeKeepsOverage(t *testing.T) {
	now := t0
	e := newTestEngine(t, Options{Now: func() time.Time { return now }})
	publishRules(e, 1, 100,
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 1000},
	)
	ctx := context.Background()
	sub := subject("t1", "g1", "c1")

	if r := e.ChangePlan(ctx, "t1", "pro", now); !r.Allowed() {
		t.Fatalf("assign pro: %+v", r)
	}
	// 用掉 600。
	res, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 600})
	if !r.Allowed() {
		t.Fatalf("reserve 600: %+v", r)
	}
	if r := e.Commit(ctx, res.ID); !r.Allowed() {
		t.Fatalf("commit: %+v", r)
	}
	// 窗口过半降级到 free（100）：折算 = 1000*0.5+100*0.5=550 < 已用 600，
	// 生效额度被抬到 600——超额用量不凭空消失，但剩余为 0。
	now = t0.Add(30 * time.Second)
	if r := e.ChangePlan(ctx, "t1", "free", now); !r.Allowed() {
		t.Fatalf("downgrade: %+v", r)
	}
	rep := e.Usage("t1", "api", now)
	if rep.Used != 600 {
		t.Fatalf("used = %d, want 600 (overage must not vanish)", rep.Used)
	}
	if rep.Limit != 600 || rep.Remaining != 0 {
		t.Fatalf("limit=%d remaining=%d, want 600/0", rep.Limit, rep.Remaining)
	}
	if _, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 1}); !r.Allowed() && r.Reason == DenyTenantQuota {
		// ok: 拒绝
	} else if r.Allowed() {
		t.Fatal("downgraded tenant with overage must be denied")
	}
	// 下一窗口按新套餐 100 生效。
	now = t0.Add(time.Minute)
	rep = e.Usage("t1", "api", now)
	if rep.Limit != 100 || rep.Used != 0 {
		t.Fatalf("next window limit=%d used=%d, want 100/0", rep.Limit, rep.Used)
	}
}
