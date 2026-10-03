package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

// Reserve pre-occupies quota. The decision is atomic across the whole
// tenant/team/caller hierarchy: either every scope's counter is updated or
// none is. Usage propagates to every ancestor scope.
func (e *engine) Reserve(ctx context.Context, req *ReserveRequest) (*ReserveResult, error) {
	if req.Amount <= 0 {
		return &ReserveResult{Allowed: false, Reason: model.DenyAmountInvalid}, nil
	}
	if req.TenantID == "" || req.CallerID == "" {
		return &ReserveResult{Allowed: false, Reason: model.DenyAmountInvalid}, nil
	}

	now := time.Now().Unix()
	ttl := req.TTL
	if ttl == 0 {
		ttl = e.config.DefaultTTL
	}

	// 1. Resolve the self-consistent rule version (all rules share one version).
	rules, version, err := e.rules.Resolve(ctx, req.TenantID, req.TeamID, req.CallerID, req.Resource)
	if err != nil {
		return nil, err
	}
	if len(rules) == 0 {
		return &ReserveResult{Allowed: false, Reason: model.DenyNoRule, RuleVersion: version}, nil
	}

	// 2. Build scope chain (leaf to root) and per-scope rule/window/key.
	scopes := scopeChain(req.TenantID, req.TeamID, req.CallerID)
	sortScopesByPriority(scopes)

	type scopeCtx struct {
		scope  model.Scope
		rule   *model.QuotaRule
		window int64
		key    string
	}
	sctxs := make([]scopeCtx, 0, len(scopes))
	keys := make([]string, 0, len(scopes))
	for _, sc := range scopes {
		rule := ruleForScope(rules, sc)
		if rule == nil {
			continue
		}
		ws := windowStart(now, rule.Window)
		key := counterKey(sc, req.Resource, ws)
		sctxs = append(sctxs, scopeCtx{scope: sc, rule: rule, window: ws, key: key})
		keys = append(keys, key)
	}
	if len(sctxs) == 0 {
		return &ReserveResult{Allowed: false, Reason: model.DenyNoRule, RuleVersion: version}, nil
	}

	// 3. Atomic check-and-act under all scope locks.
	var result *ReserveResult
	var borrowToCreate *model.BorrowRecord

	err = e.store.WithLocks(ctx, keys, func() error {
		counters, err := e.store.GetCounters(ctx, keys)
		if err != nil {
			return err
		}

		var denyScope model.Scope
		var denyKey string
		var denyReason model.DenyReason
		var borrow *borrowDecision

		for i := range sctxs {
			sc := sctxs[i]
			c := counters[sc.key]
			if c == nil {
				c = &model.Counter{Key: sc.key, Scope: sc.scope, Resource: req.Resource, WindowStart: sc.window}
			}
			limit := e.effectiveLimit(ctx, sc.scope, req.Resource, sc.rule, now)
			avail := c.Available(limit)
			if avail >= req.Amount {
				continue
			}
			// Insufficient: try to borrow from the parent scope.
			shortfall := req.Amount - avail
			if sc.rule.BorrowEnabled {
				parentIdx := i + 1
				if parentIdx < len(sctxs) {
					pc := counters[sctxs[parentIdx].key]
					if pc == nil {
						pc = &model.Counter{Key: sctxs[parentIdx].key}
					}
					parentLimit := e.effectiveLimit(ctx, sctxs[parentIdx].scope, req.Resource, sctxs[parentIdx].rule, now)
					parentAvail := pc.Available(parentLimit)
					canBorrow := tryBorrow(shortfall, c.Borrowed, sc.rule.BorrowLimit, parentAvail)
					if canBorrow == shortfall {
						borrow = &borrowDecision{
							borrowedAmount: canBorrow,
							parentScope:    sctxs[parentIdx].scope,
							parentKey:      sctxs[parentIdx].key,
							shortfall:      shortfall,
						}
						continue
					}
				}
			}
			denyScope = sc.scope
			denyKey = sc.key
			denyReason = model.DenyQuotaExhausted
			break
		}

		if denyReason != "" {
			_ = e.store.ApplyCounterDelta(ctx, denyKey, store.CounterDelta{DeniesDelta: 1})
			result = &ReserveResult{Allowed: false, Reason: denyReason, RuleVersion: version, DenyScope: denyScope}
			return nil
		}

		// All scopes pass: apply counter updates atomically.
		for i := range sctxs {
			sc := sctxs[i]
			delta := store.CounterDelta{RequestsDelta: 1}
			if borrow != nil && sc.key == borrow.parentKey {
				// Parent scope: record the lent amount.
				delta.LentDelta = borrow.borrowedAmount
			} else {
				// All scopes (including the leaf): reserve the full amount.
				delta.ReservedDelta = req.Amount
			}
			if err := e.store.ApplyCounterDelta(ctx, sc.key, delta); err != nil {
				return err
			}
		}

		borrowedAmt := int64(0)
		borrowID := ""
		if borrow != nil {
			borrowedAmt = borrow.borrowedAmount
			borrowID = newID("brw")
		}
		res := &model.Reservation{
			ID:             newID("res"),
			TenantID:       req.TenantID,
			TeamID:         req.TeamID,
			CallerID:       req.CallerID,
			Resource:       req.Resource,
			Amount:         req.Amount,
			BorrowedAmount: borrowedAmt,
			BorrowID:       borrowID,
			Status:         model.ReservationPending,
			RuleVersion:    version,
			CreatedAt:      now,
			ExpiresAt:      now + int64(ttl.Seconds()),
		}
		if err := e.store.PutReservation(ctx, res); err != nil {
			return err
		}

		if borrow != nil {
			borrowToCreate = &model.BorrowRecord{
				ID:          borrowID,
				TenantID:    req.TenantID,
				ChildScope:  sctxs[0].scope,
				ParentScope: borrow.parentScope,
				Resource:    req.Resource,
				Amount:      borrow.borrowedAmount,
				WindowStart: sctxs[0].window,
				Status:      "active",
				CreatedAt:   now,
			}
		}

		leafCounter, _ := e.store.GetCounter(ctx, sctxs[0].key)
		leafLimit := e.effectiveLimit(ctx, sctxs[0].scope, req.Resource, sctxs[0].rule, now)
		remaining := int64(0)
		if leafCounter != nil {
			remaining = leafCounter.Available(leafLimit)
		}

		result = &ReserveResult{
			Allowed:     true,
			Reservation: res,
			RuleVersion: version,
			Remaining:   remaining,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if borrowToCreate != nil {
		if err := e.store.PutBorrow(ctx, borrowToCreate); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// newID generates a unique identifier with the given prefix.
func newID(prefix string) string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}
