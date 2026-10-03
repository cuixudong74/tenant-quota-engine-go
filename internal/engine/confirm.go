package engine

import (
	"context"
	"errors"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

// Confirm settles a pending reservation. Settled may be less than or equal to
// the reserved amount; the difference is released. Confirming an already
// confirmed reservation is a no-op (idempotent).
func (e *engine) Confirm(ctx context.Context, reservationID string, settled int64) (*model.Reservation, error) {
	res, err := e.store.GetReservation(ctx, reservationID)
	if err != nil {
		return nil, err
	}
	if res.Status == model.ReservationConfirmed {
		return res, nil // idempotent
	}
	if res.Status != model.ReservationPending {
		return nil, store.ErrInvalidState
	}
	if settled <= 0 {
		settled = res.Amount
	}
	if settled > res.Amount {
		return nil, errors.New("settled amount exceeds reserved amount")
	}

	now := time.Now().Unix()
	settledBorrowed := int64(0)
	if res.BorrowedAmount > 0 {
		settledBorrowed = res.BorrowedAmount * settled / res.Amount
	}

	childScope, parentScope, br := e.resolveBorrowScopes(ctx, res)

	keys, deltas := e.buildSettlementDeltas(ctx, res, now, settled, settledBorrowed, childScope, parentScope, br, true)

	err = e.store.WithLocks(ctx, keys, func() error {
		for key, delta := range deltas {
			if err := e.store.ApplyCounterDelta(ctx, key, delta); err != nil {
				return err
			}
		}
		res.Status = model.ReservationConfirmed
		res.Settled = settled
		res.ConfirmedAt = now
		return e.store.PutReservation(ctx, res)
	})
	if err != nil {
		return nil, err
	}

	if res.BorrowID != "" {
		err = e.store.UpdateBorrow(ctx, res.BorrowID, func(b *model.BorrowRecord) (*model.BorrowRecord, error) {
			b.Repaid += settledBorrowed
			if b.Repaid >= b.Amount {
				b.Status = "repaid"
			}
			return b, nil
		})
	}
	return res, nil
}

// Cancel releases a pending reservation's quota. Cancelling an already
// cancelled reservation is a no-op (idempotent).
func (e *engine) Cancel(ctx context.Context, reservationID string) (*model.Reservation, error) {
	res, err := e.store.GetReservation(ctx, reservationID)
	if err != nil {
		return nil, err
	}
	if res.Status == model.ReservationCancelled {
		return res, nil // idempotent
	}
	if res.Status != model.ReservationPending {
		return nil, store.ErrInvalidState
	}

	now := time.Now().Unix()
	childScope, parentScope, br := e.resolveBorrowScopes(ctx, res)

	keys, deltas := e.buildSettlementDeltas(ctx, res, now, 0, 0, childScope, parentScope, br, false)

	err = e.store.WithLocks(ctx, keys, func() error {
		for key, delta := range deltas {
			if err := e.store.ApplyCounterDelta(ctx, key, delta); err != nil {
				return err
			}
		}
		res.Status = model.ReservationCancelled
		return e.store.PutReservation(ctx, res)
	})
	if err != nil {
		return nil, err
	}

	if res.BorrowID != "" {
		err = e.store.UpdateBorrow(ctx, res.BorrowID, func(b *model.BorrowRecord) (*model.BorrowRecord, error) {
			b.Status = "cancelled"
			return b, nil
		})
	}
	return res, nil
}

// resolveBorrowScopes returns the child (borrowing) scope, parent (lending)
// scope, and the borrow record for a reservation.
func (e *engine) resolveBorrowScopes(ctx context.Context, res *model.Reservation) (childScope, parentScope model.Scope, br *model.BorrowRecord) {
	childScope = model.Scope{Type: model.ScopeCaller, ID: res.CallerID}
	if res.BorrowID != "" {
		br, _ = e.store.GetBorrow(ctx, res.BorrowID)
		if br != nil {
			childScope = br.ChildScope
			parentScope = br.ParentScope
		}
	}
	return childScope, parentScope, br
}

// buildSettlementDeltas computes the per-counter deltas for confirm or cancel.
func (e *engine) buildSettlementDeltas(
	ctx context.Context,
	res *model.Reservation,
	now int64,
	settled int64,
	settledBorrowed int64,
	childScope, parentScope model.Scope,
	br *model.BorrowRecord,
	isConfirm bool,
) ([]string, map[string]store.CounterDelta) {
	rules, _, _ := e.rules.Resolve(ctx, res.TenantID, res.TeamID, res.CallerID, res.Resource)
	scopes := scopeChain(res.TenantID, res.TeamID, res.CallerID)
	sortScopesByPriority(scopes)

	keys := make([]string, 0, len(scopes))
	deltas := make(map[string]store.CounterDelta)

	for _, sc := range scopes {
		rule := ruleForScope(rules, sc)
		if rule == nil {
			continue
		}
		ws := windowStart(now, rule.Window)
		key := counterKey(sc, res.Resource, ws)
		keys = append(keys, key)

		delta := store.CounterDelta{}
		switch {
		case sc == childScope && br != nil:
			if isConfirm {
				delta.ReservedDelta = -res.Amount
				delta.UsedDelta = settled
				delta.BorrowedDelta = settledBorrowed
			} else {
				delta.ReservedDelta = -res.Amount
			}
		case br != nil && sc == parentScope:
			// Confirm: lent stays (debt not repaid yet). Cancel: restore lent.
			if !isConfirm {
				delta.LentDelta = -res.BorrowedAmount
			}
		default:
			if isConfirm {
				delta.ReservedDelta = -res.Amount
				delta.UsedDelta = settled
			} else {
				delta.ReservedDelta = -res.Amount
			}
		}
		deltas[key] = delta
	}
	return keys, deltas
}
