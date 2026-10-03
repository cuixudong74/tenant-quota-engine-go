package quota

// scopeKey 生成各层级的计数键，子级键包含全部上级，保证子级用量计入所有上级。
func tenantKey(tenantID string) string { return "tenant:" + tenantID }
func teamKey(s Subject) string         { return "team:" + s.TenantID + "/" + s.TeamID }
func callerKey(s Subject) string       { return "caller:" + s.TenantID + "/" + s.TeamID + "/" + s.CallerID }

// chain 返回主体从租户到调用方的层级链（含每层拒绝原因），按既定优先级排序。
func chain(s Subject) []level {
	return []level{
		{scope: ScopeTenant, key: tenantKey(s.TenantID), reason: DenyTenantQuota},
		{scope: ScopeTeam, key: teamKey(s), reason: DenyTeamQuota},
		{scope: ScopeCaller, key: callerKey(s), reason: DenyCallerQuota},
	}
}

type level struct {
	scope  Scope
	key    string
	reason DenyReason
}
