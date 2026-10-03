package quota

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// usageKey 唯一标识一条用量计数：层级键 + 资源 + 窗口编号。
type usageKey struct {
	scopeKey  string // 例如 "tenant:t1" / "team:t1/g1" / "caller:t1/g1/c1"
	resource  string
	windowKey int64
}

// walEvent 是一条持久化变更事件。所有状态变更先写 WAL 再应用，
// 重启时按 seq 精确重放一次，保证不丢失、不翻倍。
type walEvent struct {
	Seq       uint64       `json:"seq"`
	Type      string       `json:"type"` // use/reserve/release/commit/plan/borrow/overdue/allow/deny
	ScopeKey  string       `json:"scope_key,omitempty"`
	Resource  string       `json:"resource,omitempty"`
	Window    int64        `json:"window,omitempty"`
	Delta     int64        `json:"delta,omitempty"`
	Resv      *Reservation `json:"resv,omitempty"`
	ResvID    string       `json:"resv_id,omitempty"`
	Tenant    string       `json:"tenant,omitempty"`
	Plan      string       `json:"plan,omitempty"`
	PrevPlan  string       `json:"prev_plan,omitempty"`
	At        time.Time    `json:"at,omitempty"`
	Reason    DenyReason   `json:"reason,omitempty"`
	MetricKey string       `json:"metric_key,omitempty"`
	N         int          `json:"n,omitempty"`
}

// planState 记录租户当前套餐及上一次变更，用于窗口内折算。
type planState struct {
	Plan      string    `json:"plan"`
	Prev      string    `json:"prev"`
	ChangedAt time.Time `json:"changed_at"`
}

// snapshot 是某一 seq 时刻的全量状态。
type snapshot struct {
	Seq      uint64                          `json:"seq"`
	Used     map[string]int64                `json:"used"` // key: scopeKey|resource|window
	Reserved map[string]int64                `json:"reserved"`
	Borrowed map[string]int64                `json:"borrowed"`
	Resv     map[string]*Reservation         `json:"resv"`
	Plans    map[string]*planState           `json:"plans"`
	Overdue  map[string]int                  `json:"overdue"`
	Allowed  map[string]int64                `json:"allowed"`
	Denied   map[string]map[DenyReason]int64 `json:"denied"`
	Settled  map[string]int64                `json:"settled"`
}

func encodeUsageKey(k usageKey) string {
	return fmt.Sprintf("%s|%s|%d", k.scopeKey, k.resource, k.windowKey)
}

func decodeUsageKey(s string) (usageKey, error) {
	var k usageKey
	parts := splitLast2(s)
	if len(parts) != 3 {
		return k, errors.New("bad usage key: " + s)
	}
	k.scopeKey = parts[0]
	k.resource = parts[1]
	if _, err := fmt.Sscanf(parts[2], "%d", &k.windowKey); err != nil {
		return k, err
	}
	return k, nil
}

func splitLast2(s string) []string {
	i, j := -1, -1
	for x := len(s) - 1; x >= 0; x-- {
		if s[x] == '|' {
			if j == -1 {
				j = x
			} else {
				i = x
				break
			}
		}
	}
	if i < 0 || j < 0 {
		return nil
	}
	return []string{s[:i], s[i+1 : j], s[j+1:]}
}

// store 是引擎的并发安全状态存储，可选地通过 WAL + 快照持久化。
type store struct {
	mu sync.Mutex

	used     map[usageKey]int64
	reserved map[usageKey]int64
	resv     map[string]*Reservation

	plans    map[string]*planState // tenantID -> 套餐状态
	overdue  map[string]int        // tenantID -> 连续未归还窗口数
	borrowed map[usageKey]int64
	settled  map[string]int64 // tenant|resource -> 已完成借用结算的窗口编号

	allowed map[string]int64 // tenantID|resource -> 放行次数
	denied  map[string]map[DenyReason]int64

	// 持久化
	dir           string
	wal           *os.File
	walw          *bufio.Writer
	seq           uint64
	sinceSnapshot int
	snapshotEvery int
}

func newStore() *store {
	return &store{
		used:     make(map[usageKey]int64),
		reserved: make(map[usageKey]int64),
		resv:     make(map[string]*Reservation),
		plans:    make(map[string]*planState),
		overdue:  make(map[string]int),
		borrowed: make(map[usageKey]int64),
		settled:  make(map[string]int64),
		allowed:  make(map[string]int64),
		denied:   make(map[string]map[DenyReason]int64),
	}
}

// ---------- 持久化 ----------

const (
	walFileName      = "quota.wal"
	snapshotFileName = "quota.snapshot.json"
	snapshotEvery    = 512 // 每多少条事件触发一次快照
)

// open 打开（必要时创建）持久化目录，并恢复已有状态。
func (s *store) open(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	s.dir = dir
	s.snapshotEvery = snapshotEvery
	if err := s.loadSnapshot(); err != nil {
		return err
	}
	if err := s.replayWAL(); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, walFileName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	s.wal = f
	s.walw = bufio.NewWriter(f)
	return nil
}

func (s *store) loadSnapshot() error {
	b, err := os.ReadFile(filepath.Join(s.dir, snapshotFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}
	s.seq = snap.Seq
	for k, v := range snap.Used {
		uk, err := decodeUsageKey(k)
		if err != nil {
			return err
		}
		s.used[uk] = v
	}
	for k, v := range snap.Reserved {
		uk, err := decodeUsageKey(k)
		if err != nil {
			return err
		}
		s.reserved[uk] = v
	}
	for k, v := range snap.Borrowed {
		uk, err := decodeUsageKey(k)
		if err != nil {
			return err
		}
		s.borrowed[uk] = v
	}
	for id, r := range snap.Resv {
		rc := *r
		s.resv[id] = &rc
	}
	for k, v := range snap.Plans {
		vc := *v
		s.plans[k] = &vc
	}
	for k, v := range snap.Overdue {
		s.overdue[k] = v
	}
	for k, v := range snap.Allowed {
		s.allowed[k] = v
	}
	for k, m := range snap.Denied {
		if s.denied[k] == nil {
			s.denied[k] = make(map[DenyReason]int64)
		}
		for r, v := range m {
			s.denied[k][r] = v
		}
	}
	for k, v := range snap.Settled {
		s.settled[k] = v
	}
	return nil
}

func (s *store) replayWAL() error {
	f, err := os.Open(filepath.Join(s.dir, walFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev walEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			return fmt.Errorf("replay wal: %w", err)
		}
		if ev.Seq <= s.seq {
			continue // 已包含在快照中，跳过避免重复计数
		}
		s.apply(ev)
		s.seq = ev.Seq
	}
	return sc.Err()
}

// append 写一条 WAL 事件并应用。调用方必须持有 s.mu。
func (s *store) append(ev walEvent) {
	s.seq++
	ev.Seq = s.seq
	if s.walw != nil {
		b, _ := json.Marshal(ev)
		s.walw.Write(b)
		s.walw.WriteByte('\n')
		s.walw.Flush()
	}
	s.apply(ev)
	s.sinceSnapshot++
	if s.dir != "" && s.sinceSnapshot >= s.snapshotEvery {
		s.writeSnapshot()
	}
}

// apply 把事件应用到内存状态。必须幂等：同一 seq 只会被应用一次。
func (s *store) apply(ev walEvent) {
	key := usageKey{scopeKey: ev.ScopeKey, resource: ev.Resource, windowKey: ev.Window}
	switch ev.Type {
	case "use":
		s.used[key] += ev.Delta
	case "reserve":
		s.reserved[key] += ev.Delta
		if ev.Resv != nil {
			rc := *ev.Resv
			s.resv[ev.ResvID] = &rc
		}
	case "commit":
		if r, ok := s.resv[ev.ResvID]; ok {
			r.Committed = true
		}
	case "release":
		if r, ok := s.resv[ev.ResvID]; ok {
			r.Released = true
		}
	case "plan":
		s.plans[ev.Tenant] = &planState{Plan: ev.Plan, Prev: ev.PrevPlan, ChangedAt: ev.At}
	case "borrow":
		s.borrowed[key] += ev.Delta
	case "overdue":
		s.overdue[ev.Tenant] = ev.N
	case "settled":
		s.settled[ev.MetricKey] = ev.Window
	case "allow":
		s.allowed[ev.MetricKey]++
	case "deny":
		if s.denied[ev.MetricKey] == nil {
			s.denied[ev.MetricKey] = make(map[DenyReason]int64)
		}
		s.denied[ev.MetricKey][ev.Reason]++
	}
}

// writeSnapshot 原子地写全量快照并截断 WAL。调用方必须持有 s.mu。
func (s *store) writeSnapshot() {
	snap := snapshot{
		Seq:      s.seq,
		Used:     make(map[string]int64, len(s.used)),
		Reserved: make(map[string]int64, len(s.reserved)),
		Borrowed: make(map[string]int64, len(s.borrowed)),
		Resv:     make(map[string]*Reservation, len(s.resv)),
		Plans:    s.plans,
		Overdue:  s.overdue,
		Allowed:  s.allowed,
		Denied:   s.denied,
		Settled:  s.settled,
	}
	for k, v := range s.used {
		snap.Used[encodeUsageKey(k)] = v
	}
	for k, v := range s.reserved {
		snap.Reserved[encodeUsageKey(k)] = v
	}
	for k, v := range s.borrowed {
		snap.Borrowed[encodeUsageKey(k)] = v
	}
	for id, r := range s.resv {
		rc := *r
		snap.Resv[id] = &rc
	}
	b, err := json.Marshal(snap)
	if err != nil {
		return
	}
	tmp := filepath.Join(s.dir, snapshotFileName+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, snapshotFileName)); err != nil {
		return
	}
	// 快照完成后截断 WAL：旧事件已全部包含在快照中。
	if s.wal != nil {
		s.wal.Close()
	}
	f, err := os.OpenFile(filepath.Join(s.dir, walFileName), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err == nil {
		s.wal = f
		s.walw = bufio.NewWriter(f)
	}
	s.sinceSnapshot = 0
}

// Close 落盘最终快照并关闭 WAL。
func (s *store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dir != "" {
		s.writeSnapshot()
	}
	if s.wal != nil {
		s.walw.Flush()
		s.wal.Close()
		s.wal = nil
	}
}

// ---------- 读写辅助（均需在调用方保证的并发语义下使用，内部自带锁） ----------

func metricKey(tenantID, resource string) string { return tenantID + "|" + resource }

// putReservation 记录一笔预留单（只记一次，重复 ID 覆盖）。
func (s *store) putReservation(r *Reservation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.append(walEvent{Type: "reserve", Resv: r, ResvID: r.ID})
}

// addReserved 调整某键的预留占用量（delta 可为负）。
func (s *store) addReserved(key usageKey, delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.append(walEvent{Type: "reserve", ScopeKey: key.scopeKey, Resource: key.resource,
		Window: key.windowKey, Delta: delta})
}

// settleReservation 把预留单标记为已结算或已归还。
// 返回 false 表示预留不存在或已终结，保证任何路径都不会重复扣减。
// 成功后调用方负责按层级链调整 reserved/used 计数。
func (s *store) settleReservation(id string, commit bool) (*Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.resv[id]
	if !ok || r.Committed || r.Released {
		return nil, false
	}
	if commit {
		s.append(walEvent{Type: "commit", ResvID: id})
	} else {
		s.append(walEvent{Type: "release", ResvID: id})
	}
	rc := *r
	return &rc, true
}

// addUsed 直接增加实际用量（层级链上各级共用）。
func (s *store) addUsed(key usageKey, delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.append(walEvent{Type: "use", ScopeKey: key.scopeKey, Resource: key.resource,
		Window: key.windowKey, Delta: delta})
}

// get 返回某键当前已用与预留量。
func (s *store) get(key usageKey) (used, reserved int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used[key], s.reserved[key]
}

func (s *store) setPlan(tenantID string, st planState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.append(walEvent{Type: "plan", Tenant: tenantID, Plan: st.Plan, PrevPlan: st.Prev, At: st.ChangedAt})
}

func (s *store) getPlan(tenantID string) (planState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.plans[tenantID]
	if !ok {
		return planState{}, false
	}
	return *p, true
}

func (s *store) addBorrowed(key usageKey, delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.append(walEvent{Type: "borrow", ScopeKey: key.scopeKey, Resource: key.resource,
		Window: key.windowKey, Delta: delta})
}

func (s *store) getBorrowed(key usageKey) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.borrowed[key]
}

func (s *store) setOverdue(tenantID string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.append(walEvent{Type: "overdue", Tenant: tenantID, N: n})
}

func (s *store) getOverdue(tenantID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.overdue[tenantID]
}

func (s *store) incAllowed(tenantID, resource string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.append(walEvent{Type: "allow", MetricKey: metricKey(tenantID, resource)})
}

func (s *store) incDenied(tenantID, resource string, reason DenyReason) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.append(walEvent{Type: "deny", MetricKey: metricKey(tenantID, resource), Reason: reason})
}

// report 汇总某租户某资源在指定窗口的可观测数据。
func (s *store) report(tenantID, resource string, windowKey int64) (used, reserved, borrowed, allowed int64, denied map[DenyReason]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := usageKey{scopeKey: tenantKey(tenantID), resource: resource, windowKey: windowKey}
	mk := metricKey(tenantID, resource)
	denied = make(map[DenyReason]int64)
	for r, v := range s.denied[mk] {
		denied[r] = v
	}
	return s.used[key], s.reserved[key], s.borrowed[key], s.allowed[mk], denied
}

// markSettled 记录某租户某资源已完成借用结算的窗口编号。
func (s *store) markSettled(tenantID, resource string, windowKey int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.append(walEvent{Type: "settled", MetricKey: metricKey(tenantID, resource), Window: windowKey})
}

func (s *store) lastSettled(tenantID, resource string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.settled[metricKey(tenantID, resource)]
}

// listPending 返回所有未结算也未归还的预留单快照。
func (s *store) listPending() []Reservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Reservation, 0, len(s.resv))
	for _, r := range s.resv {
		if !r.Committed && !r.Released {
			out = append(out, *r)
		}
	}
	return out
}
