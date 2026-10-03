package quota

import (
	"context"
	"sync"
	"testing"
	"time"
)

var testPlans = []Plan{
	{Name: "free", Rank: 1, Window: WindowMinute, Limits: map[string]int64{"api": 100}},
	{Name: "pro", Rank: 2, Window: WindowMinute, Limits: map[string]int64{"api": 1000}},
	{Name: "ent", Rank: 3, Window: WindowMinute, Limits: map[string]int64{"api": 10000}},
}

func newTestEngine(t *testing.T, opts Options) *Engine {
	t.Helper()
	if opts.Plans == nil {
		opts.Plans = testPlans
	}
	e, err := NewEngine(opts)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(e.Close)
	return e
}

func publishRules(e *Engine, version uint64, gray int, rules ...Rule) {
	e.UpdateRules(RuleSet{
		Version:     version,
		Rules:       rules,
		GrayPercent: gray,
		CreatedAt:   time.Now(),
		GracePeriod: time.Minute,
	})
}

func subject(tenant, team, caller string) Subject {
	return Subject{TenantID: tenant, TeamID: team, CallerID: caller}
}

// 并发争用：同一窗口内并发 Reserve+Commit，
// 成功数不得超过额度，最终用量必须等于真实提交数（不多不少）。
func TestConcurrentContention(t *testing.T) {
	e := newTestEngine(t, Options{})
	publishRules(e, 1, 100,
		Rule{Scope: ScopeTenant, Resource: "api", Window: WindowMinute, Limit: 500},
		Rule{Scope: ScopeTeam, Resource: "api", Window: WindowMinute, Limit: 500},
		Rule{Scope: ScopeCaller, Resource: "api", Window: WindowMinute, Limit: 500},
	)

	ctx := context.Background()
	const goroutines = 64
	const perG = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	committed := 0
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			sub := subject("t1", "g1", "c1")
			for i := 0; i < perG; i++ {
				res, r := e.Reserve(ctx, CheckRequest{Subject: sub, Resource: "api", Amount: 1})
				if !r.Allowed() {
					continue
				}
				if cr := e.Commit(ctx, res.ID); !cr.Allowed() {
					t.Errorf("commit failed: %+v", cr)
					continue
				}
				mu.Lock()
				committed++
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	if committed > 500 {
		t.Fatalf("committed %d exceeds limit 500", committed)
	}
	if committed != 500 {
		t.Fatalf("expected exactly 500 committed under contention, got %d", committed)
	}
	rep := e.Usage("t1", "api", time.Now())
	if rep.Used != 500 {
		t.Fatalf("used = %d, want 500 (no leak, no double count)", rep.Used)
	}
	if rep.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", rep.Remaining)
	}
	// 超额请求必须全部拒绝且原因可区分。
	_, r := e.Reserve(ctx, CheckRequest{Subject: subject("t1", "g1", "c1"), Resource: "api", Amount: 1})
	if r.Allowed() || r.Reason != DenyTenantQuota {
		t.Fatalf("expected TENANT_QUOTA_EXCEEDED, got %+v", r)
	}
}
