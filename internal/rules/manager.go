// Package rules manages quota rule versioning and gray (canary) releases.
//
// Readers always observe a self-consistent version: all rules returned for a
// single request come from the same immutable RuleVersion. When a new version
// is activated with a gray percentage, a stable hash of the caller id
// determines membership; non-gray traffic keeps reading the previous active
// version during the grace period.
package rules

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

// Rule manager errors.
var (
	ErrNoActiveVersion = errors.New("rules: no active version")
	ErrInvalidVersion  = errors.New("rules: invalid version")
)

// Manager owns rule versions and resolves the applicable rule set for a
// request in a consistent, versioned way.
type Manager struct {
	store store.Store
	mu    sync.RWMutex

	versions    map[int64]*model.RuleVersion
	active      int64 // currently active version number
	gray        int64 // currently gray version number (0 = none)
	nextVersion int64
}

// NewManager creates a rule manager backed by the given store.
func NewManager(s store.Store) *Manager {
	return &Manager{
		store:       s,
		versions:    make(map[int64]*model.RuleVersion),
		nextVersion: 1,
	}
}

// Publish creates a new draft rule version from the given rules. Each rule is
// stamped with the new version number.
func (m *Manager) Publish(ctx context.Context, note string, rules []model.QuotaRule) (*model.RuleVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v := &model.RuleVersion{
		Version:   m.nextVersion,
		Rules:     make([]model.QuotaRule, len(rules)),
		Status:    "draft",
		CreatedAt: time.Now().Unix(),
		Note:      note,
	}
	for i, r := range rules {
		r.Version = v.Version
		v.Rules[i] = r
	}
	m.nextVersion++
	m.versions[v.Version] = v

	if err := m.store.PutRuleVersion(ctx, v); err != nil {
		return nil, err
	}
	return v, nil
}

// Activate makes a version the active one.
//
//   - If grayPercent > 0, the version is rolled out to that percentage of
//     callers while the previous active version stays readable for the grace
//     period (gray release).
//   - If grayPercent == 0 and the version is currently gray, it is promoted to
//     full active (the previous active version is retired).
//   - Otherwise the version becomes fully active immediately.
func (m *Manager) Activate(ctx context.Context, version int64, grayPercent int, grace time.Duration) (*model.RuleVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, ok := m.versions[version]
	if !ok {
		return nil, ErrInvalidVersion
	}
	if grayPercent < 0 || grayPercent > 100 {
		return nil, errors.New("rules: gray percent must be in [0,100]")
	}

	now := time.Now().Unix()
	if grayPercent > 0 {
		// Gray release: previous active version stays active during grace.
		v.Status = "gray"
		v.GrayPercent = grayPercent
		v.GraceUntil = now + int64(grace.Seconds())
		m.gray = version
	} else {
		// Full activation: retire the previous active version.
		if m.active != 0 && m.active != version {
			if old := m.versions[m.active]; old != nil {
				old.Status = "retired"
				_ = m.store.PutRuleVersion(ctx, old)
			}
		}
		v.Status = "active"
		v.ActivatedAt = now
		m.active = version
		m.gray = 0
	}

	if err := m.store.PutRuleVersion(ctx, v); err != nil {
		return nil, err
	}
	return v, nil
}

// Rollback retires the active version and restores the most recent retired
// version to active. It returns the restored version (nil if none).
func (m *Manager) Rollback(ctx context.Context) (*model.RuleVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active == 0 {
		return nil, ErrNoActiveVersion
	}
	cur := m.versions[m.active]
	cur.Status = "retired"

	var prev *model.RuleVersion
	for _, v := range m.versions {
		if v.Status == "retired" && v.Version < cur.Version {
			if prev == nil || v.Version > prev.Version {
				prev = v
			}
		}
	}
	if prev == nil {
		m.active = 0
	} else {
		prev.Status = "active"
		prev.ActivatedAt = time.Now().Unix()
		m.active = prev.Version
		if err := m.store.PutRuleVersion(ctx, prev); err != nil {
			return nil, err
		}
	}
	if err := m.store.PutRuleVersion(ctx, cur); err != nil {
		return nil, err
	}
	return prev, nil
}

// Resolve returns the self-consistent rule set applicable to the given
// caller. All returned rules belong to the same immutable version.
func (m *Manager) Resolve(ctx context.Context, tenantID, teamID, callerID string, resource model.Resource) ([]model.QuotaRule, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	version := m.active
	if m.gray != 0 {
		if gv := m.versions[m.gray]; gv != nil {
			if hashCaller(tenantID, teamID, callerID) < gv.GrayPercent {
				version = m.gray
			}
		}
	}
	if version == 0 {
		return nil, 0, nil
	}
	v := m.versions[version]
	if v == nil {
		return nil, 0, nil
	}
	out := make([]model.QuotaRule, 0, len(v.Rules))
	for _, r := range v.Rules {
		if r.Resource == resource {
			out = append(out, r)
		}
	}
	return out, version, nil
}

// ActiveVersion returns the currently active version number.
func (m *Manager) ActiveVersion() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active
}

// GrayVersion returns the currently gray version number (0 if none).
func (m *Manager) GrayVersion() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.gray
}

// Versions returns all known versions ordered by version number.
func (m *Manager) Versions() []*model.RuleVersion {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*model.RuleVersion, 0, len(m.versions))
	for _, v := range m.versions {
		cp := *v
		cp.Rules = append([]model.QuotaRule(nil), v.Rules...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// SwitchTenantRules atomically replaces the rules for one tenant and activates
// a new immutable version containing the change. Rules for other tenants are
// carried over unchanged, so readers always observe a self-consistent version.
func (m *Manager) SwitchTenantRules(ctx context.Context, tenantID string, tenantRules []model.QuotaRule) (*model.RuleVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var base []model.QuotaRule
	if m.active != 0 {
		if v := m.versions[m.active]; v != nil {
			base = append(base, v.Rules...)
		}
	}
	newRules := make([]model.QuotaRule, 0, len(base)+len(tenantRules))
	for _, r := range base {
		if r.Scope.Type == model.ScopeTenant && r.Scope.ID == tenantID {
			continue
		}
		newRules = append(newRules, r)
	}
	for _, r := range tenantRules {
		r.Version = m.nextVersion
		newRules = append(newRules, r)
	}

	v := &model.RuleVersion{
		Version:     m.nextVersion,
		Rules:       newRules,
		Status:      "active",
		CreatedAt:   time.Now().Unix(),
		ActivatedAt: time.Now().Unix(),
		Note:        "plan switch for tenant " + tenantID,
	}
	m.nextVersion++
	if m.active != 0 && m.active != v.Version {
		if old := m.versions[m.active]; old != nil {
			old.Status = "retired"
			_ = m.store.PutRuleVersion(ctx, old)
		}
	}
	m.versions[v.Version] = v
	m.active = v.Version
	m.gray = 0

	if err := m.store.PutRuleVersion(ctx, v); err != nil {
		return nil, err
	}
	return v, nil
}

// Load rebuilds the manager state from the store (used after restart).
func (m *Manager) Load(ctx context.Context) error {
	versions, err := m.store.ListRuleVersions(ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.versions = make(map[int64]*model.RuleVersion, len(versions))
	m.active = 0
	m.gray = 0
	var maxVer int64
	for _, v := range versions {
		cp := *v
		cp.Rules = append([]model.QuotaRule(nil), v.Rules...)
		m.versions[cp.Version] = &cp
		if cp.Version > maxVer {
			maxVer = cp.Version
		}
		switch cp.Status {
		case "active":
			m.active = cp.Version
		case "gray":
			m.gray = cp.Version
		}
	}
	m.nextVersion = maxVer + 1
	return nil
}

// hashCaller returns a stable bucket in [0, 100) for gray membership.
func hashCaller(tenantID, teamID, callerID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tenantID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(teamID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(callerID))
	return int(h.Sum32() % 100)
}
