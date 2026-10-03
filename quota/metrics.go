package quota

// UsageReport 是某租户某资源的可观测报表。
type UsageReport struct {
	TenantID  string
	Resource  string
	Used      int64
	Reserved  int64
	Limit     int64
	Remaining int64
	Allowed   int64
	Denied    map[DenyReason]int64
	// Borrowed 是当前未归还的借用总额。
	Borrowed int64
}
