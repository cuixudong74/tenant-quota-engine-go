package quota

import (
	"hash/fnv"
	"sync"
	"time"
)

// Rule 定义某一层级、某一资源在一个时间窗口内的额度上限。
type Rule struct {
	Scope    Scope
	Resource string
	Window   Window
	Limit    int64
}

// RuleSet 是一个自洽的规则版本快照，发布后不可变。
type RuleSet struct {
	Version uint64
	Rules   []Rule
	// GrayPercent 表示按租户哈希灰度到新版本的百分比 [0,100]。
	GrayPercent int
	CreatedAt   time.Time
	// GracePeriod 内，钉住旧版本的请求仍按旧版本判定。
	GracePeriod time.Duration
}

// find 查找指定层级与资源的规则。
func (rs *RuleSet) find(scope Scope, resource string) (Rule, bool) {
	for _, r := range rs.Rules {
		if r.Scope == scope && r.Resource == resource {
			return r, true
		}
	}
	return Rule{}, false
}

// RuleManager 管理规则版本与灰度发布。
// 读取方通过 For 拿到的始终是某个完整版本的快照，不会看到半更新状态。
type RuleManager struct {
	mu       sync.RWMutex
	versions map[uint64]*RuleSet
	latest   uint64
}

// NewRuleManager 创建空的规则管理器。
func NewRuleManager() *RuleManager {
	return &RuleManager{versions: make(map[uint64]*RuleSet)}
}

// Update 发布一个新版本规则集。版本号必须大于当前最新版本。
func (m *RuleManager) Update(rs RuleSet) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := rs
	cp.Rules = append([]Rule(nil), rs.Rules...)
	m.versions[rs.Version] = &cp
	if rs.Version > m.latest {
		m.latest = rs.Version
	}
}

// inGray 按租户哈希稳定地判定其是否落入新版本的灰度比例。
func inGray(tenantID string, version uint64, percent int) bool {
	if percent >= 100 {
		return true
	}
	if percent <= 0 {
		return false
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(tenantID))
	_, _ = h.Write([]byte{byte(version), byte(version >> 8), byte(version >> 16), byte(version >> 24)})
	return h.Sum64()%100 < uint64(percent)
}

// For 返回 tenant 在 now 时刻应使用的规则快照：
//   - pin 非零且该版本仍处于宽限期内时，返回被钉住的旧版本；
//   - 否则按灰度比例在最新版与上一版之间做稳定选择。
func (m *RuleManager) For(tenantID string, pin uint64, now time.Time) *RuleSet {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if pin != 0 {
		if rs, ok := m.versions[pin]; ok && now.Sub(rs.CreatedAt) <= rs.GracePeriod {
			return rs
		}
	}

	latest, ok := m.versions[m.latest]
	if !ok {
		return nil
	}
	if inGray(tenantID, latest.Version, latest.GrayPercent) {
		return latest
	}
	// 未命中灰度：回退到次新版本（若存在）。
	var prev *RuleSet
	for _, rs := range m.versions {
		if rs.Version < latest.Version && (prev == nil || rs.Version > prev.Version) {
			prev = rs
		}
	}
	if prev != nil {
		return prev
	}
	return latest
}
