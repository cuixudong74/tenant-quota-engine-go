package quota

import (
	"fmt"
	"time"
)

// Plan 定义一个套餐等级：各资源的窗口额度。
type Plan struct {
	Name   string
	Rank   int // 越大等级越高，用于判定升级/降级
	Limits map[string]int64
	Window Window
}

// planManager 维护套餐表与租户套餐的生效计算。
type planManager struct {
	plans map[string]Plan
}

func newPlanManager(plans []Plan) *planManager {
	m := &planManager{plans: make(map[string]Plan)}
	for _, p := range plans {
		m.plans[p.Name] = p
	}
	return m
}

func (m *planManager) get(name string) (Plan, bool) {
	p, ok := m.plans[name]
	return p, ok
}

// lower 返回比 name 低一级的套餐（用于借用长期逾期后的自动降级）。
func (m *planManager) lower(name string) (Plan, bool) {
	cur, ok := m.plans[name]
	if !ok {
		return Plan{}, false
	}
	var best Plan
	found := false
	for _, p := range m.plans {
		if p.Rank < cur.Rank && (!found || p.Rank > best.Rank) {
			best, found = p, true
		}
	}
	return best, found
}

// effectiveLimit 计算租户某资源在 now 时刻的生效额度：
//   - 套餐变更发生在当前窗口内时，按 旧额度*已过比例 + 新额度*剩余比例 折算；
//     升级因此立即获得新增额度，降级则平滑收紧；
//   - 折算结果下限为当前已用量，保证降级后已发生的超额用量不消失、
//     也不会因折算出现"负额度"错乱——剩余为 0，后续请求被拒绝直到下一窗口。
func (m *planManager) effectiveLimit(st planState, resource string, used int64, now time.Time) (int64, bool) {
	cur, ok := m.plans[st.Plan]
	if !ok {
		return 0, false
	}
	newLimit, ok := cur.Limits[resource]
	if !ok {
		return 0, false
	}
	w := cur.Window
	key := w.Key(now)
	start, _ := w.Bounds(key)
	// 变更不在当前窗口内：直接按新额度生效。
	if st.ChangedAt.Before(start) || st.Prev == "" {
		return newLimit, true
	}
	prev, ok := m.plans[st.Prev]
	if !ok {
		return newLimit, true
	}
	oldLimit, ok := prev.Limits[resource]
	if !ok {
		oldLimit = 0
	}
	elapsed := float64(st.ChangedAt.Sub(start)) / float64(w.Size)
	remain := 1 - elapsed
	eff := int64(float64(oldLimit)*elapsed + float64(newLimit)*remain)
	if eff < used {
		eff = used // 已发生用量不凭空消失
	}
	return eff, true
}

// changePlan 执行套餐切换并返回持久化的新状态。
func (m *planManager) changePlan(st planState, newPlan string, now time.Time) (planState, error) {
	if _, ok := m.plans[newPlan]; !ok {
		return st, fmt.Errorf("unknown plan %q", newPlan)
	}
	return planState{Plan: newPlan, Prev: st.Plan, ChangedAt: now}, nil
}
