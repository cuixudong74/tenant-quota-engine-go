package quota

import (
	"context"
	"testing"
	"time"
)

// 突发借用：上级资源池有余量时允许超额，后续窗口自动归还。
func TestBorrowAndRepay(t *testing.T) {
	now := t0
	e := newTestEngine(t, Options{
		Now:        func() time.Time { return now },
		Borrow:     BorrowPolicy{Enabled: true, MaxBorrowRatio: 0.5, MaxOverdueWindows: 2},
		PoolLimits: map[string]int64{"api": 1000},
	})
	publishRules(e, 1, 100,
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 100},
	)
	ctx := context.Background()
	sub := subject("t1", "g1", "c1")
	if r := e.ChangePlan(ctx, "t1", "free", now); !r.Allowed() {
		t.Fatalf("assign free: %+v", r)
	}

	// 用满自身 100。
	res, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 100})
	if !r.Allowed() {
		t.Fatalf("reserve 100: %+v", r)
	}
	if r := e.Commit(ctx, res.ID); !r.Allowed() {
		t.Fatalf("commit: %+v", r)
	}
	// 突发 30：借用放行。
	res, r = e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 30})
	if !r.Allowed() {
		t.Fatalf("borrowed reserve should be allowed: %+v", r)
	}
	if res.Borrowed != 30 {
		t.Fatalf("borrowed = %d, want 30", res.Borrowed)
	}
	if r := e.Commit(ctx, res.ID); !r.Allowed() {
		t.Fatalf("commit borrowed: %+v", r)
	}
	rep := e.Usage("t1", "api", now)
	if rep.Borrowed != 30 {
		t.Fatalf("outstanding borrow = %d, want 30", rep.Borrowed)
	}
	// 借用超过比例上限（50）被拒绝，原因可区分。
	_, r = e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 25})
	if r.Allowed() || r.Reason != DenyBorrowLimit {
		t.Fatalf("want BORROW_LIMIT_EXCEEDED, got %+v", r)
	}

	// 下一窗口：自动用新额度归还 30，可用只剩 70。
	now = t0.Add(time.Minute)
	rep = e.Usage("t1", "api", now)
	if rep.Borrowed != 0 {
		t.Fatalf("outstanding after repay = %d, want 0", rep.Borrowed)
	}
	if rep.Used != 30 {
		t.Fatalf("repayment should consume new window quota: used = %d, want 30", rep.Used)
	}
	if rep.Remaining != 70 {
		t.Fatalf("remaining after repayment = %d, want 70", rep.Remaining)
	}
}

// 长期归还不上的租户按既定策略自动降级，而不是无限透支。
func TestBorrowOverdueAutoDowngrade(t *testing.T) {
	now := t0
	e := newTestEngine(t, Options{
		Now:        func() time.Time { return now },
		Borrow:     BorrowPolicy{Enabled: true, MaxBorrowRatio: 2.5, MaxOverdueWindows: 2},
		PoolLimits: map[string]int64{"api": 100000},
	})
	publishRules(e, 1, 100,
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 1000},
	)
	ctx := context.Background()
	sub := subject("t1", "g1", "c1")
	if r := e.ChangePlan(ctx, "t1", "pro", now); !r.Allowed() {
		t.Fatalf("assign pro: %+v", r)
	}

	// 窗口 0：用满 1000 后借 2500（比例上限 2.5 倍），欠款超过单窗口额度。
	now = t0
	res, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 1000})
	if !r.Allowed() {
		t.Fatalf("reserve 1000: %+v", r)
	}
	if r := e.Commit(ctx, res.ID); !r.Allowed() {
		t.Fatalf("commit: %+v", r)
	}
	res, r = e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 2500})
	if !r.Allowed() || res.Borrowed != 2500 {
		t.Fatalf("borrow 2500: %+v borrowed=%d", r, res.Borrowed)
	}
	if r := e.Commit(ctx, res.ID); !r.Allowed() {
		t.Fatalf("commit borrow: %+v", r)
	}
	// 窗口 1：归还 1000，仍欠 1500，逾期 1 次。
	now = t0.Add(time.Minute)
	if rep := e.Usage("t1", "api", now); rep.Borrowed != 1500 {
		t.Fatalf("window1 outstanding = %d, want 1500", rep.Borrowed)
	}
	// 逾期期间禁止新增借用，原因可区分。
	if _, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 2000}); r.Reason != DenyOverdueDowngrade {
		t.Fatalf("overdue borrow: want PLAN_DOWNGRADED_OVERDUE_BORROW, got %+v", r)
	}
	// 窗口 2：再还 1000，仍欠 500，逾期 2 次，达到阈值自动降级 pro -> free。
	// 当窗口还款已占 1000，生效额度被已用兜底（用量不消失），剩余为 0。
	now = t0.Add(2 * time.Minute)
	rep := e.Usage("t1", "api", now)
	if rep.Borrowed != 500 {
		t.Fatalf("window2 outstanding = %d, want 500", rep.Borrowed)
	}
	if rep.Remaining != 0 {
		t.Fatalf("window2 remaining = %d, want 0", rep.Remaining)
	}
	// 窗口 3：欠款 500 用 free 额度（100）部分归还，剩余欠款 400；
	// 再往后逐窗口归还，且额度已切换为 free 的 100。
	now = t0.Add(3 * time.Minute)
	rep = e.Usage("t1", "api", now)
	if rep.Limit != 100 {
		t.Fatalf("limit after auto downgrade = %d, want 100 (free)", rep.Limit)
	}
	if rep.Borrowed != 400 {
		t.Fatalf("window3 outstanding = %d, want 400", rep.Borrowed)
	}
}
