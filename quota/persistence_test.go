package quota

import (
	"context"
	"testing"
	"time"
)

// 进程重启后：用量、预留、套餐与拒绝统计不丢失、不翻倍。
func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	sub := subject("t1", "g1", "c1")

	build := func(t *testing.T) *Engine {
		e, err := NewEngine(Options{DataDir: dir, Plans: testPlans})
		if err != nil {
			t.Fatalf("NewEngine: %v", err)
		}
		publishRules(e, 1, 100,
			Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 100},
		)
		return e
	}

	// 第一次启动：提交 40，挂起预留 10，制造 3 次拒绝，分配套餐。
	e1 := build(t)
	if r := e1.ChangePlan(ctx, "t1", "free", time.Now()); !r.Allowed() {
		t.Fatalf("change plan: %+v", r)
	}
	for i := 0; i < 4; i++ {
		res, r := e1.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 10})
		if !r.Allowed() {
			t.Fatalf("reserve %d: %+v", i, r)
		}
		if i < 3 {
			if r := e1.Commit(ctx, res.ID); !r.Allowed() {
				t.Fatalf("commit %d: %+v", i, r)
			}
		}
	}
	for i := 0; i < 3; i++ {
		e1.Check(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 1000})
	}
	e1.Close()

	// 重启：状态必须完整恢复。
	e2 := build(t)
	defer e2.Close()
	rep := e2.Usage("t1", "api", time.Now())
	if rep.Used != 30 {
		t.Fatalf("used after restart = %d, want 30", rep.Used)
	}
	if rep.Reserved != 10 {
		t.Fatalf("reserved after restart = %d, want 10", rep.Reserved)
	}
	if rep.Denied[DenyTenantQuota] != 3 {
		t.Fatalf("denied after restart = %d, want 3", rep.Denied[DenyTenantQuota])
	}
	if rep.Limit != 100 {
		t.Fatalf("plan limit after restart = %d, want 100 (free)", rep.Limit)
	}

	// 再重启一次：不得因重放而翻倍。
	e2.Close()
	e3 := build(t)
	defer e3.Close()
	rep = e3.Usage("t1", "api", time.Now())
	if rep.Used != 30 || rep.Reserved != 10 {
		t.Fatalf("after 2nd restart used=%d reserved=%d, want 30/10 (no double count)", rep.Used, rep.Reserved)
	}
}

// 快照截断 WAL 后，重放不得重复计数。
func TestSnapshotNoDoubleReplay(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	sub := subject("t1", "g1", "c1")

	e, err := NewEngine(Options{DataDir: dir, Plans: testPlans})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	publishRules(e, 1, 100,
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 100000},
	)
	// 制造超过 snapshotEvery(512) 条事件，触发至少一次快照。
	for i := 0; i < 300; i++ {
		res, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 1})
		if !r.Allowed() {
			t.Fatalf("reserve %d: %+v", i, r)
		}
		if r := e.Commit(ctx, res.ID); !r.Allowed() {
			t.Fatalf("commit %d: %+v", i, r)
		}
	}
	e.Close()

	e2, err := NewEngine(Options{DataDir: dir, Plans: testPlans})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	publishRules(e2, 1, 100,
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 100000},
	)
	rep := e2.Usage("t1", "api", time.Now())
	if rep.Used != 300 {
		t.Fatalf("used after snapshot+replay = %d, want 300", rep.Used)
	}
}
