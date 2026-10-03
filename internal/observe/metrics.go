// Package observe collects and queries quota usage and denial metrics.
//
// All metrics are derived from persisted counters, so restarting the process
// neither loses nor double-counts statistics.
package observe

import (
	"context"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/rules"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

// Metrics provides queryable usage and denial statistics.
type Metrics struct {
	store store.Store
	rules *rules.Manager
}

// NewMetrics creates a Metrics backed by the given store and rule manager.
func NewMetrics(s store.Store, rm *rules.Manager) *Metrics {
	return &Metrics{store: s, rules: rm}
}

// Usage is an observable usage snapshot for a scope/resource.
type Usage struct {
	Scope        model.Scope    `json:"scope"`
	Resource     model.Resource `json:"resource"`
	WindowStart  int64          `json:"window_start"`
	WindowEnd    int64          `json:"window_end"`
	Limit        int64          `json:"limit"`
	Used         int64          `json:"used"`
	Reserved     int64          `json:"reserved"`
	Borrowed     int64          `json:"borrowed"`
	Lent         int64          `json:"lent"`
	RepaymentDue int64          `json:"repayment_due"`
	Remaining    int64          `json:"remaining"`
	Requests     int64          `json:"requests"`
	Denies       int64          `json:"denies"`
}

// Query returns the current usage for the given scope/resource.
func (m *Metrics) Query(ctx context.Context, scope model.Scope, resource model.Resource) (*Usage, error) {
	rules, _, err := m.rules.Resolve(ctx, tenantOf(scope), teamOf(scope), callerOf(scope), resource)
	if err != nil {
		return nil, err
	}
	var rule *model.QuotaRule
	for i := range rules {
		if rules[i].Scope == scope {
			rule = &rules[i]
			break
		}
	}
	if rule == nil {
		return nil, store.ErrNotFound
	}

	now := time.Now().Unix()
	ws := windowStart(now, rule.Window)
	key := counterKey(scope, resource, ws)
	counter, err := m.store.GetCounter(ctx, key)
	if err != nil {
		return nil, err
	}

	u := &Usage{
		Scope:       scope,
		Resource:    resource,
		WindowStart: ws,
		WindowEnd:   ws + int64(rule.Window.Seconds()),
		Limit:       rule.Limit,
		Remaining:   rule.Limit,
	}
	if counter != nil {
		u.Used = counter.Used
		u.Reserved = counter.Reserved
		u.Borrowed = counter.Borrowed
		u.Lent = counter.Lent
		u.RepaymentDue = counter.RepaymentDue
		u.Requests = counter.Requests
		u.Denies = counter.Denies
		u.Remaining = counter.Available(rule.Limit)
	}
	return u, nil
}

// QueryTenant returns usage for every resource the tenant has rules for.
func (m *Metrics) QueryTenant(ctx context.Context, tenantID string) ([]*Usage, error) {
	// Collect all resources configured for this tenant from every version.
	versions := m.rules.Versions()
	seen := make(map[model.Resource]struct{})
	for _, v := range versions {
		for _, r := range v.Rules {
			if r.Scope.Type == model.ScopeTenant && r.Scope.ID == tenantID {
				seen[r.Resource] = struct{}{}
			}
		}
	}
	out := make([]*Usage, 0, len(seen))
	for res := range seen {
		u, err := m.Query(ctx, model.Scope{Type: model.ScopeTenant, ID: tenantID}, res)
		if err != nil {
			continue
		}
		out = append(out, u)
	}
	return out, nil
}

// DenyCount returns the total deny count for a scope/resource.
func (m *Metrics) DenyCount(ctx context.Context, scope model.Scope, resource model.Resource) (int64, error) {
	u, err := m.Query(ctx, scope, resource)
	if err != nil {
		return 0, err
	}
	return u.Denies, nil
}

func tenantOf(scope model.Scope) string {
	if scope.Type == model.ScopeTenant {
		return scope.ID
	}
	return ""
}

func teamOf(scope model.Scope) string {
	if scope.Type == model.ScopeTeam {
		return scope.ID
	}
	return ""
}

func callerOf(scope model.Scope) string {
	if scope.Type == model.ScopeCaller {
		return scope.ID
	}
	return ""
}

func windowStart(now int64, window time.Duration) int64 {
	w := int64(window.Seconds())
	if w <= 0 {
		w = 1
	}
	return (now / w) * w
}

func counterKey(scope model.Scope, resource model.Resource, ws int64) string {
	return "c:" + string(scope.Type) + ":" + scope.ID + ":" + string(resource) + ":" + itoa(ws)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
