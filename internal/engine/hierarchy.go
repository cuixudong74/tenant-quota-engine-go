package engine

import (
	"fmt"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

// scopeChain returns the scopes from leaf (caller) to root (tenant) for a
// request. Usage propagates to every scope in the chain.
func scopeChain(tenantID, teamID, callerID string) []model.Scope {
	return []model.Scope{
		{Type: model.ScopeCaller, ID: callerID},
		{Type: model.ScopeTeam, ID: teamID},
		{Type: model.ScopeTenant, ID: tenantID},
	}
}

// counterKey builds the storage key for a scope/resource/window counter.
func counterKey(scope model.Scope, resource model.Resource, windowStart int64) string {
	return fmt.Sprintf("c:%s:%s:%s:%d", scope.Type, scope.ID, resource, windowStart)
}

// windowStart aligns unix seconds to the start of the fixed window containing
// now. A window <= 0 is treated as a 1-second window (safety).
func windowStart(now int64, window time.Duration) int64 {
	w := int64(window.Seconds())
	if w <= 0 {
		w = 1
	}
	return (now / w) * w
}

// ruleForScope returns the rule matching the given scope from a versioned
// rule set, or nil if the scope is not configured.
func ruleForScope(rules []model.QuotaRule, scope model.Scope) *model.QuotaRule {
	for i := range rules {
		if rules[i].Scope == scope {
			return &rules[i]
		}
	}
	return nil
}

// parentScope returns the parent of the given scope in the tenant hierarchy.
func parentScope(scope model.Scope, tenantID, teamID string) (model.Scope, bool) {
	switch scope.Type {
	case model.ScopeCaller:
		return model.Scope{Type: model.ScopeTeam, ID: teamID}, true
	case model.ScopeTeam:
		return model.Scope{Type: model.ScopeTenant, ID: tenantID}, true
	default:
		return model.Scope{}, false
	}
}

// scopePriority returns the denial priority of a scope type. Lower values are
// checked first (caller before team before tenant).
func scopePriority(scope model.Scope) int {
	switch scope.Type {
	case model.ScopeCaller:
		return 0
	case model.ScopeTeam:
		return 1
	default:
		return 2
	}
}

// sortScopesByPriority sorts scopes leaf-first (caller, team, tenant).
func sortScopesByPriority(scopes []model.Scope) {
	for i := 1; i < len(scopes); i++ {
		for j := i; j > 0 && scopePriority(scopes[j]) < scopePriority(scopes[j-1]); j-- {
			scopes[j], scopes[j-1] = scopes[j-1], scopes[j]
		}
	}
}
