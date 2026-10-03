package quota

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Options 配置引擎行为。
type Options struct {
	// DataDir 非空时启用 WAL + 快照持久化。
	DataDir string
	// ReservationTTL 是预留的默认过期时间，过期未结算自动归还。
	ReservationTTL time.Duration
	// Plans 是可用套餐表，按 Rank 排序判定升降级。
	Plans []Plan
	// Borrow 是突发借用策略。
	Borrow BorrowPolicy
	// PoolLimits 是上级资源池（平台级）各资源在租户窗口内的总额度，
	// 借用只有在资源池有余量时才被允许。
	PoolLimits map[string]int64
	// Now 可注入时钟，便于测试；为空使用真实时间。
	Now func() time.Time
}

// Engine 是配额服务的核心入口，所有方法并发安全。
// mu 把"判定 + 占用"序列化为原子操作，杜绝检查与生效之间的竞态。
type Engine struct {
	mu    sync.Mutex
	opts  Options
	st    *store
	rules *RuleManager
	plans *planManager
}

// NewEngine 创建引擎；启用持久化时会从 DataDir 恢复状态。
func NewEngine(opts Options) (*Engine, error) {
	e := &Engine{
		opts:  opts,
		st:    newStore(),
		rules: NewRuleManager(),
		plans: newPlanManager(opts.Plans),
	}
	if opts.ReservationTTL <= 0 {
		e.opts.ReservationTTL = time.Minute
	}
	if opts.DataDir != "" {
		if err := e.st.open(opts.DataDir); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// Close 落盘状态并关闭存储。
func (e *Engine) Close() { e.st.Close() }

func (e *Engine) now(t time.Time) time.Time {
	if !t.IsZero() {
		return t
	}
	if e.opts.Now != nil {
		return e.opts.Now()
	}
	return time.Now()
}

func newReservationID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// levelState 是层级链上某一层的求值结果。
type levelState struct {
	level
	key         usageKey
	limit       int64
	used        int64
	reserved    int64
	constrained bool // 该层是否有生效的额度约束
}

// decision 是一次判定的完整结论与待应用的占用计划。
type decision struct {
	result Result
	levels []levelState
	borrow int64
	pool   usageKey // borrow > 0 时有效
}

// evaluate 对请求做只读判定，给出结论与（若放行）待应用的占用计划。
// 判定优先级：租户 > 团队 > 调用方，任一层耗尽即按该层原因拒绝。
func (e *Engine) evaluate(req CheckRequest, now time.Time) decision {
	rs := e.rules.For(req.Subject.TenantID, req.RuleVersion, now)
	if rs == nil {
		return decision{result: deny(DenyNoRule, "no ruleset published", 0)}
	}
	sub := req.Subject

	// 新窗口首次触及该租户时，先做借用归还结算。
	e.settleBorrow(sub.TenantID, req.Resource, rs, now)

	levels := make([]levelState, 0, 3)
	for _, lv := range chain(sub) {
		ls := levelState{level: lv}
		var rule Rule
		var ok bool
		if lv.scope == ScopeTenant {
			rule, ls.limit, ok = e.tenantLimit(sub.TenantID, req.Resource, rs, now)
		} else {
			rule, ok = rs.find(lv.scope, req.Resource)
			ls.limit = rule.Limit
		}
		if !ok {
			levels = append(levels, ls) // 该层无约束
			continue
		}
		ls.constrained = true
		ls.key = usageKey{scopeKey: lv.key, resource: req.Resource, windowKey: rule.Window.Key(now)}
		ls.used, ls.reserved = e.st.get(ls.key)
		levels = append(levels, ls)
	}

	// 逐层检查，父级优先；记录第一个耗尽的层级。
	var failed *levelState
	for i := range levels {
		ls := &levels[i]
		if !ls.constrained {
			continue
		}
		if ls.used+ls.reserved+req.Amount > ls.limit {
			failed = ls
			break
		}
	}
	if failed == nil {
		return decision{result: allow(rs.Version), levels: levels}
	}
	// 仅当唯一缺口在租户层时才允许借用；子层（团队/调用方）耗尽不可借用。
	if failed.scope == ScopeTenant && e.opts.Borrow.Enabled {
		othersOK := true
		for i := range levels {
			ls := &levels[i]
			if ls.constrained && ls.scope != ScopeTenant &&
				ls.used+ls.reserved+req.Amount > ls.limit {
				othersOK = false
				break
			}
		}
		if othersOK {
			// 借用可行则放行；借用能力不足时返回可区分的借用拒绝原因。
			return e.tryBorrow(req, rs, *failed, levels, now)
		}
	}
	return decision{result: deny(failed.reason, "quota exceeded at "+string(failed.scope)+" scope", rs.Version)}
}

// tenantLimit 返回租户层的生效额度与对应窗口规则。
// 已分配套餐时以套餐（含窗口内折算）为准，否则回退到规则集中的租户规则。
func (e *Engine) tenantLimit(tenantID, resource string, rs *RuleSet, now time.Time) (Rule, int64, bool) {
	if st, ok := e.st.getPlan(tenantID); ok {
		if p, ok := e.plans.get(st.Plan); ok {
			w := p.Window
			key := usageKey{scopeKey: tenantKey(tenantID), resource: resource, windowKey: w.Key(now)}
			used, _ := e.st.get(key)
			if eff, ok := e.plans.effectiveLimit(st, resource, used, now); ok {
				return Rule{Scope: ScopeTenant, Resource: resource, Window: w, Limit: eff}, eff, true
			}
		}
	}
	rule, ok := rs.find(ScopeTenant, resource)
	if !ok {
		return Rule{}, 0, false
	}
	return rule, rule.Limit, true
}

// tryBorrow 在租户层额度不足时尝试从上级资源池借用。
// 只有借用能完全覆盖缺口时才放行，避免部分借用造成的语义模糊。
func (e *Engine) tryBorrow(req CheckRequest, rs *RuleSet, ls levelState, levels []levelState, now time.Time) decision {
	// 处于逾期状态的租户禁止新增借用，直到欠款还清。
	if e.st.getOverdue(req.Subject.TenantID) > 0 {
		return decision{result: deny(DenyOverdueDowngrade, "tenant has overdue borrows", rs.Version)}
	}
	poolLimit, ok := e.opts.PoolLimits[req.Resource]
	if !ok || poolLimit <= 0 {
		return decision{result: deny(DenyBorrowLimit, "no resource pool configured", rs.Version)}
	}
	shortfall := ls.used + ls.reserved + req.Amount - ls.limit
	if shortfall <= 0 {
		return decision{result: deny(ls.reason, "quota exceeded", rs.Version)}
	}
	tenant := req.Subject.TenantID
	winKey := ls.key.windowKey
	// 借用上限：自身窗口额度的一定比例。
	capLeft := int64(float64(ls.limit)*e.opts.Borrow.MaxBorrowRatio) -
		e.st.getBorrowed(borrowedKey(tenant, req.Resource, winKey))
	// 资源池余量。
	pk := poolKey(req.Resource, winKey)
	poolUsed, poolReserved := e.st.get(pk)
	poolLeft := poolLimit - poolUsed - poolReserved
	if capLeft < shortfall || poolLeft < shortfall {
		return decision{result: deny(DenyBorrowLimit, "borrow capacity or pool headroom insufficient", rs.Version)}
	}
	return decision{result: allow(rs.Version), levels: levels, borrow: shortfall, pool: pk}
}

// settleBorrow 在新窗口首次触及租户时做借用归还：
// 用本窗口额度的空余部分归还历史欠款；归还不足记一次逾期，
// 连续逾期达到阈值自动降级，绝不无限透支。
func (e *Engine) settleBorrow(tenantID, resource string, rs *RuleSet, now time.Time) {
	if !e.opts.Borrow.Enabled {
		return
	}
	rule, limit, ok := e.tenantLimit(tenantID, resource, rs, now)
	if !ok {
		return
	}
	winKey := rule.Window.Key(now)
	if e.st.lastSettled(tenantID, resource) >= winKey {
		return
	}
	outstanding := e.st.getBorrowed(outstandingKey(tenantID, resource))
	if outstanding > 0 {
		tk := usageKey{scopeKey: tenantKey(tenantID), resource: resource, windowKey: winKey}
		used, reserved := e.st.get(tk)
		headroom := limit - used - reserved
		repay := outstanding
		if repay > headroom {
			repay = headroom
		}
		if repay < 0 {
			repay = 0
		}
		if repay > 0 {
			e.st.addBorrowed(outstandingKey(tenantID, resource), -repay)
			e.st.addUsed(tk, repay) // 归还占用本窗口额度
		}
		if outstanding-repay > 0 {
			overdue := e.st.getOverdue(tenantID) + 1
			e.st.setOverdue(tenantID, overdue)
			if e.opts.Borrow.MaxOverdueWindows > 0 && overdue >= e.opts.Borrow.MaxOverdueWindows {
				e.autoDowngrade(tenantID, now)
				e.st.setOverdue(tenantID, 0)
			}
		} else {
			e.st.setOverdue(tenantID, 0)
		}
	}
	e.st.markSettled(tenantID, resource, winKey)
}

// autoDowngrade 把租户套餐下调一级（借用长期逾期的既定处置）。
func (e *Engine) autoDowngrade(tenantID string, now time.Time) {
	st, ok := e.st.getPlan(tenantID)
	if !ok {
		return
	}
	lower, ok := e.plans.lower(st.Plan)
	if !ok {
		return
	}
	ns, err := e.plans.changePlan(st, lower.Name, now)
	if err != nil {
		return
	}
	e.st.setPlan(tenantID, ns)
}

// expireReservations 惰性归还所有已过期的预留，防止占用泄漏。
func (e *Engine) expireReservations(now time.Time) {
	for _, r := range e.st.listPending() {
		if now.After(r.ExpiresAt) {
			e.release(r.ID)
		}
	}
}

// Check 只做判定不占用额度。
func (e *Engine) Check(ctx context.Context, req CheckRequest) Result {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now(req.Now)
	e.expireReservations(now)
	d := e.evaluate(req, now)
	if d.result.Allowed() {
		e.st.incAllowed(req.Subject.TenantID, req.Resource)
	} else {
		e.st.incDenied(req.Subject.TenantID, req.Resource, d.result.Reason)
	}
	return d.result
}

// Reserve 判定通过则占用额度并返回预留单；占用计入层级链每一级。
func (e *Engine) Reserve(ctx context.Context, req CheckRequest) (Reservation, Result) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now(req.Now)
	e.expireReservations(now)
	d := e.evaluate(req, now)
	if !d.result.Allowed() {
		e.st.incDenied(req.Subject.TenantID, req.Resource, d.result.Reason)
		return Reservation{}, d.result
	}
	res := Reservation{
		ID:        newReservationID(),
		Subject:   req.Subject,
		Resource:  req.Resource,
		Amount:    req.Amount,
		WindowKey: d.levels[0].key.windowKey,
		ExpiresAt: now.Add(e.opts.ReservationTTL),
		Borrowed:  d.borrow,
	}
	for _, ls := range d.levels {
		if !ls.constrained {
			continue
		}
		res.Charges = append(res.Charges, Charge{
			ScopeKey:  ls.key.scopeKey,
			Resource:  ls.key.resource,
			WindowKey: ls.key.windowKey,
			Amount:    req.Amount,
		})
	}
	if d.borrow > 0 {
		res.BorrowWindowKey = d.pool.windowKey
		res.Charges = append(res.Charges, Charge{
			ScopeKey:  d.pool.scopeKey,
			Resource:  d.pool.resource,
			WindowKey: d.pool.windowKey,
			Amount:    d.borrow,
		})
	}
	// 应用占用：先落预留单，再逐层加预留计数。
	e.st.putReservation(&res)
	for _, c := range res.Charges {
		e.st.addReserved(usageKey{scopeKey: c.ScopeKey, resource: c.Resource, windowKey: c.WindowKey}, c.Amount)
	}
	if d.borrow > 0 {
		e.st.addBorrowed(outstandingKey(req.Subject.TenantID, req.Resource), d.borrow)
		e.st.addBorrowed(borrowedKey(req.Subject.TenantID, req.Resource, d.pool.windowKey), d.borrow)
	}
	e.st.incAllowed(req.Subject.TenantID, req.Resource)
	return res, d.result
}

// Commit 结算一笔预留：预留转为实际用量。重复结算返回拒绝，不会重复扣减。
func (e *Engine) Commit(ctx context.Context, reservationID string) Result {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.st.settleReservation(reservationID, true)
	if !ok {
		return deny(DenyInvalidReservation, "reservation not found or already settled", 0)
	}
	for _, c := range r.Charges {
		key := usageKey{scopeKey: c.ScopeKey, resource: c.Resource, windowKey: c.WindowKey}
		e.st.addReserved(key, -c.Amount)
		e.st.addUsed(key, c.Amount)
	}
	return allow(0)
}

// Release 归还一笔预留（失败或取消的调用）。重复归还返回拒绝，不会重复返还。
func (e *Engine) Release(ctx context.Context, reservationID string) Result {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.release(reservationID)
}

func (e *Engine) release(reservationID string) Result {
	r, ok := e.st.settleReservation(reservationID, false)
	if !ok {
		return deny(DenyInvalidReservation, "reservation not found or already settled", 0)
	}
	for _, c := range r.Charges {
		key := usageKey{scopeKey: c.ScopeKey, resource: c.Resource, windowKey: c.WindowKey}
		e.st.addReserved(key, -c.Amount)
	}
	if r.Borrowed > 0 {
		// 未结算的借用直接冲销，不计入欠款。
		e.st.addBorrowed(outstandingKey(r.Subject.TenantID, r.Resource), -r.Borrowed)
		e.st.addBorrowed(borrowedKey(r.Subject.TenantID, r.Resource, r.BorrowWindowKey), -r.Borrowed)
	}
	return allow(0)
}

// ChangePlan 切换租户套餐等级。升级立即生效（按窗口内折算），
// 降级平滑收紧且已发生的超额用量不消失。
func (e *Engine) ChangePlan(ctx context.Context, tenantID, plan string, now time.Time) Result {
	e.mu.Lock()
	defer e.mu.Unlock()
	now = e.now(now)
	st, _ := e.st.getPlan(tenantID)
	ns, err := e.plans.changePlan(st, plan, now)
	if err != nil {
		return deny(DenyNoRule, err.Error(), 0)
	}
	e.st.setPlan(tenantID, ns)
	return allow(0)
}

// UpdateRules 发布新版本规则集（支持灰度）。
func (e *Engine) UpdateRules(rs RuleSet) { e.rules.Update(rs) }

// Usage 查询租户某资源当前的用量、剩余与拒绝统计。
func (e *Engine) Usage(tenantID, resource string, now time.Time) UsageReport {
	e.mu.Lock()
	defer e.mu.Unlock()
	now = e.now(now)
	rep := UsageReport{TenantID: tenantID, Resource: resource, Denied: map[DenyReason]int64{}}
	rs := e.rules.For(tenantID, 0, now)
	if rs == nil {
		return rep
	}
	// 报表前先做借用归还结算，保证读到的是结算后的真实状态。
	e.settleBorrow(tenantID, resource, rs, now)
	rule, limit, ok := e.tenantLimit(tenantID, resource, rs, now)
	if !ok {
		return rep
	}
	used, reserved, _, allowed, denied := e.st.report(tenantID, resource, rule.Window.Key(now))
	rep.Used = used
	rep.Reserved = reserved
	rep.Limit = limit
	rep.Remaining = limit - used - reserved
	rep.Allowed = allowed
	rep.Denied = denied
	rep.Borrowed = e.st.getBorrowed(outstandingKey(tenantID, resource))
	return rep
}
