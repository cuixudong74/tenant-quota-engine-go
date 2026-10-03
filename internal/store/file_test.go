package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "quota-test-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestFileStoreRestartPreservesUsage verifies that usage counters survive a
// process restart without loss or doubling.
func TestFileStoreRestartPreservesUsage(t *testing.T) {
	dir := tempDir(t)
	ctx := context.Background()

	// First "process": create store, apply deltas.
	s1, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	key := "c:tenant:t1:api_calls:1000000"
	if err := s1.ApplyCounterDelta(ctx, key, CounterDelta{UsedDelta: 42, RequestsDelta: 5}); err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	s1.Close()

	// Second "process": reopen and verify.
	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer s2.Close()

	c, err := s2.GetCounter(ctx, key)
	if err != nil {
		t.Fatalf("get counter: %v", err)
	}
	if c == nil {
		t.Fatalf("counter not found after restart")
	}
	if c.Used != 42 {
		t.Fatalf("used mismatch after restart: got %d, want 42", c.Used)
	}
	if c.Requests != 5 {
		t.Fatalf("requests mismatch after restart: got %d, want 5", c.Requests)
	}
}

// TestFileStoreSnapshotCompaction verifies that snapshots compact the WAL and
// state remains consistent.
func TestFileStoreSnapshotCompaction(t *testing.T) {
	dir := tempDir(t)
	ctx := context.Background()

	s1, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	key := "c:tenant:t1:api_calls:1000000"
	for i := 0; i < 100; i++ {
		if err := s1.ApplyCounterDelta(ctx, key, CounterDelta{UsedDelta: 1}); err != nil {
			t.Fatalf("apply delta: %v", err)
		}
	}
	if err := s1.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s1.Close()

	// Reopen after snapshot.
	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	c, _ := s2.GetCounter(ctx, key)
	if c == nil || c.Used != 100 {
		t.Fatalf("used mismatch after snapshot+restart: %+v", c)
	}
}

// TestFileStoreReservationRestart verifies reservations survive restart.
func TestFileStoreReservationRestart(t *testing.T) {
	dir := tempDir(t)
	ctx := context.Background()

	s1, _ := NewFileStore(dir)
	res := &model.Reservation{
		ID: "res_1", TenantID: "t1", Resource: "api_calls",
		Amount: 50, Status: model.ReservationPending,
		CreatedAt: time.Now().Unix(), ExpiresAt: time.Now().Unix() + 300,
	}
	s1.PutReservation(ctx, res)
	s1.Close()

	s2, _ := NewFileStore(dir)
	defer s2.Close()
	got, err := s2.GetReservation(ctx, "res_1")
	if err != nil {
		t.Fatalf("get reservation: %v", err)
	}
	if got.Amount != 50 || got.Status != model.ReservationPending {
		t.Fatalf("reservation mismatch after restart: %+v", got)
	}
}

// TestFileStoreWALFileExists verifies the WAL file is created on disk.
func TestFileStoreWALFileExists(t *testing.T) {
	dir := tempDir(t)
	ctx := context.Background()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer s.Close()

	_ = s.ApplyCounterDelta(ctx, "c:tenant:t1:api_calls:1", CounterDelta{UsedDelta: 1})

	walPath := filepath.Join(dir, "wal.log")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("wal file not found: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("wal file is empty after write")
	}
}
