package engine

import (
	"context"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/rules"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

// testEnv holds a fully wired engine for tests.
type testEnv struct {
	store  store.Store
	rules  *rules.Manager
	engine Engine
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	st := store.NewMemoryStore(64)
	rm := rules.NewManager(st)
	eng := New(st, rm, DefaultConfig())
	return &testEnv{store: st, rules: rm, engine: eng}
}

// publishTenantRules publishes and activates a rule version with the given
// tenant-level rules.
func (env *testEnv) publishTenantRules(t *testing.T, tenantID string, rules []model.QuotaRule) {
	t.Helper()
	ctx := context.Background()
	v, err := env.rules.Publish(ctx, "test", rules)
	if err != nil {
		t.Fatalf("publish rules: %v", err)
	}
	if _, err := env.rules.Activate(ctx, v.Version, 0, 0); err != nil {
		t.Fatalf("activate rules: %v", err)
	}
}

func tenantRule(tenantID string, resource model.Resource, limit int64, window time.Duration) model.QuotaRule {
	return model.QuotaRule{
		Scope:    model.Scope{Type: model.ScopeTenant, ID: tenantID},
		Resource: resource,
		Limit:    limit,
		Window:   window,
	}
}

func callerRule(callerID string, resource model.Resource, limit int64, window time.Duration) model.QuotaRule {
	return model.QuotaRule{
		Scope:    model.Scope{Type: model.ScopeCaller, ID: callerID},
		Resource: resource,
		Limit:    limit,
		Window:   window,
	}
}

func teamRule(teamID string, resource model.Resource, limit int64, window time.Duration) model.QuotaRule {
	return model.QuotaRule{
		Scope:    model.Scope{Type: model.ScopeTeam, ID: teamID},
		Resource: resource,
		Limit:    limit,
		Window:   window,
	}
}
