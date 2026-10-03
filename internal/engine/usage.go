package engine

import (
	"context"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

// GetUsage returns the current usage snapshot for a scope/resource.
func (e *engine) GetUsage(ctx context.Context, scope model.Scope, resource model.Resource) (*UsageInfo, error) {
	rules, _, err := e.rules.Resolve(ctx, tenantOf(scope), teamOf(scope), callerOf(scope), resource)
	if err != nil {
		return nil, err
	}
	rule := ruleForScope(rules, scope)
	if rule == nil {
		return nil, store.ErrNotFound
	}

	now := time.Now().Unix()
	ws := windowStart(now, rule.Window)
	key := counterKey(scope, resource, ws)
	counter, err := e.store.GetCounter(ctx, key)
	if err != nil {
		return nil, err
	}

	limit := e.effectiveLimit(ctx, scope, resource, rule, now)
	info := &UsageInfo{
		Scope:       scope,
		Resource:    resource,
		WindowStart: ws,
		WindowEnd:   ws + int64(rule.Window.Seconds()),
		Limit:       limit,
		Remaining:   limit,
	}
	if counter != nil {
		info.Used = counter.Used
		info.Reserved = counter.Reserved
		info.Borrowed = counter.Borrowed
		info.Lent = counter.Lent
		info.RepaymentDue = counter.RepaymentDue
		info.Requests = counter.Requests
		info.Denies = counter.Denies
		info.Remaining = counter.Available(limit)
	}
	return info, nil
}
