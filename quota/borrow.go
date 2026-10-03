package quota

// BorrowPolicy 配置突发借用策略：允许租户在上级资源池有余量时
// 临时借用额度，并在后续窗口用自身额度归还；长期归还不上的租户
// 会被自动降级，而不是无限透支。
type BorrowPolicy struct {
	Enabled bool
	// MaxBorrowRatio 单次窗口内累计借用占自身窗口额度的比例上限 (0,1]。
	MaxBorrowRatio float64
	// MaxOverdueWindows 连续多少个窗口未归还后触发自动降级。
	MaxOverdueWindows int
}

// poolScopeKey 是上级资源池（平台级）的计数键前缀。
const poolScopeKey = "pool"

// poolKey 返回资源池在某窗口的计数键。
func poolKey(resource string, windowKey int64) usageKey {
	return usageKey{scopeKey: poolScopeKey, resource: resource, windowKey: windowKey}
}

// borrowedKey 返回租户借用台账的计数键。
func borrowedKey(tenantID, resource string, windowKey int64) usageKey {
	return usageKey{scopeKey: "borrow:" + tenantID, resource: resource, windowKey: windowKey}
}

// outstandingKey 返回租户某资源当前未归还借用总额的计数键（跨窗口）。
func outstandingKey(tenantID, resource string) usageKey {
	return usageKey{scopeKey: "outstanding:" + tenantID, resource: resource, windowKey: 0}
}
