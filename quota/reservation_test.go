package quota

import (
	"context"
	"testing"
	"time"
)

// 预留-结算路径：占用后 Commit 转为用量；Release 归还；任何路径不泄漏、不重复扣减。
func TestReserveCommitRelease(t *testing.T) {
	e := newTestEngine(t, Options{})
	publishRules(e, 1, 100,
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 10},
	)
	ctx := context.Background()
	sub := subject("t1", "g1", "c1")

	// 占满 10。
	var ids []string
	for i := 0; i < 10; i++ {
		res, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 1})
		if !r.Allowed() {
			t.Fatalf("reserve %d denied: %+v", i, r)
		}
		ids = append(ids, res.ID)
	}
	// 第 11 个被拒绝。
	if _, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 1}); r.Allowed() {
		t.Fatal("expected deny when reserved out")
	}
	// 归还 4 个，额度恢复。
	for _, id := range ids[:4] {
		if r := e.Release(ctx, id); !r.Allowed() {
			t.Fatalf("release: %+v", r)
		}
	}
	// 重复归还被拒绝，不会重复返还。
	if r := e.Release(ctx, ids[0]); r.Allowed() || r.Reason != DenyInvalidReservation {
		t.Fatalf("double release should be denied, got %+v", r)
	}
	// 结算 6 个。
	for _, id := range ids[4:] {
		if r := e.Commit(ctx, id); !r.Allowed() {
			t.Fatalf("commit: %+v", r)
		}
	}
	// 重复结算被拒绝，不会重复扣减。
	if r := e.Commit(ctx, ids[4]); r.Allowed() || r.Reason != DenyInvalidReservation {
		t.Fatalf("double commit should be denied, got %+v", r)
	}
	// 已归还的预留不能再结算。
	if r := e.Commit(ctx, ids[0]); r.Allowed() {
		t.Fatal("commit after release must fail")
	}
	rep := e.Usage("t1", "api", time.Now())
	if rep.Used != 6 || rep.Reserved != 0 || rep.Remaining != 4 {
		t.Fatalf("got used=%d reserved=%d remaining=%d, want 6/0/4", rep.Used, rep.Reserved, rep.Remaining)
	}
}

// 过期预留被惰性回收，额度自动归还，绝不泄漏。
func TestReservationExpiry(t *testing.T) {
	now := time.Now()
	clock := &now
	e := newTestEngine(t, Options{
		ReservationTTL: time.Second,
		Now:            func() time.Time { return *clock },
	})
	publishRules(e, 1, 100,
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 5},
	)
	ctx := context.Background()
	sub := subject("t1", "g1", "c1")

	for i := 0; i < 5; i++ {
		if _, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 1}); !r.Allowed() {
			t.Fatalf("reserve %d: %+v", i, r)
		}
	}
	// 时钟前进超过 TTL，预留全部过期。
	*clock = now.Add(2 * time.Second)
	// 下一次请求触发惰性回收，额度应已归还。
	for i := 0; i < 5; i++ {
		if _, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 1}); !r.Allowed() {
			t.Fatalf("after expiry reserve %d should succeed: %+v", i, r)
		}
	}
	rep := e.Usage("t1", "api", *clock)
	if rep.Reserved != 5 || rep.Used != 0 {
		t.Fatalf("got reserved=%d used=%d, want 5/0", rep.Reserved, rep.Used)
	}
}
