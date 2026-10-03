package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/engine"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/observe"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/rules"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

func newTestServer(t *testing.T) (*Server, store.Store, *rules.Manager) {
	t.Helper()
	st := store.NewMemoryStore(64)
	rm := rules.NewManager(st)
	eng := engine.New(st, rm, engine.DefaultConfig())
	metrics := observe.NewMetrics(st, rm)
	srv := New(":0", eng, metrics)
	return srv, st, rm
}

func publishRules(t *testing.T, rm *rules.Manager, tenantID string, limit int64) {
	t.Helper()
	ctx := context.Background()
	v, err := rm.Publish(ctx, "test", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: tenantID}, Resource: "api_calls", Limit: limit, Window: time.Minute},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := rm.Activate(ctx, v.Version, 0, 0); err != nil {
		t.Fatalf("activate: %v", err)
	}
}

func doJSON(t *testing.T, srv *Server, method, path string, body interface{}) (int, map[string]interface{}) {
	t.Helper()
	var reqBody []byte
	if body != nil {
		reqBody, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	return rec.Code, resp
}

func TestHealth(t *testing.T) {
	srv, _, _ := newTestServer(t)
	code, resp := doJSON(t, srv, "GET", "/v1/health", nil)
	if code != http.StatusOK || resp["status"] != "ok" {
		t.Fatalf("health check failed: code=%d resp=%v", code, resp)
	}
}

func TestReserveAPI(t *testing.T) {
	srv, _, rm := newTestServer(t)
	publishRules(t, rm, "t1", 100)

	code, resp := doJSON(t, srv, "POST", "/v1/reserve", map[string]interface{}{
		"tenant_id": "t1",
		"team_id":   "team1",
		"caller_id": "c1",
		"resource":  "api_calls",
		"amount":    30,
	})
	if code != http.StatusOK {
		t.Fatalf("reserve failed: code=%d resp=%v", code, resp)
	}
	if resp["allowed"] != true {
		t.Fatalf("expected allowed, got %v", resp)
	}
	resv := resp["reservation"].(map[string]interface{})
	if resv["amount"].(float64) != 30 {
		t.Fatalf("expected amount 30, got %v", resv["amount"])
	}
}

func TestReserveDenyAPI(t *testing.T) {
	srv, _, rm := newTestServer(t)
	publishRules(t, rm, "t1", 10)

	code, resp := doJSON(t, srv, "POST", "/v1/reserve", map[string]interface{}{
		"tenant_id": "t1",
		"team_id":   "team1",
		"caller_id": "c1",
		"resource":  "api_calls",
		"amount":    50,
	})
	if code != http.StatusOK {
		t.Fatalf("reserve failed: code=%d resp=%v", code, resp)
	}
	if resp["allowed"] != false {
		t.Fatalf("expected deny, got %v", resp)
	}
	if resp["reason"] != "quota_exhausted" {
		t.Fatalf("expected quota_exhausted, got %v", resp["reason"])
	}
}

func TestConfirmCancelAPI(t *testing.T) {
	srv, _, rm := newTestServer(t)
	publishRules(t, rm, "t1", 100)

	// Reserve
	_, resp := doJSON(t, srv, "POST", "/v1/reserve", map[string]interface{}{
		"tenant_id": "t1", "team_id": "team1", "caller_id": "c1",
		"resource": "api_calls", "amount": 30,
	})
	resv := resp["reservation"].(map[string]interface{})
	resID := resv["id"].(string)

	// Confirm
	code, resp := doJSON(t, srv, "POST", "/v1/confirm", map[string]interface{}{
		"reservation_id": resID,
		"settled":        30,
	})
	if code != http.StatusOK {
		t.Fatalf("confirm failed: code=%d resp=%v", code, resp)
	}
	if resp["status"] != "confirmed" {
		t.Fatalf("expected confirmed, got %v", resp["status"])
	}
}

func TestUsageAPI(t *testing.T) {
	srv, _, rm := newTestServer(t)
	publishRules(t, rm, "t1", 100)

	// Reserve and confirm
	_, resp := doJSON(t, srv, "POST", "/v1/reserve", map[string]interface{}{
		"tenant_id": "t1", "team_id": "team1", "caller_id": "c1",
		"resource": "api_calls", "amount": 40,
	})
	resv := resp["reservation"].(map[string]interface{})
	doJSON(t, srv, "POST", "/v1/confirm", map[string]interface{}{
		"reservation_id": resv["id"].(string), "settled": 40,
	})

	// Query usage
	code, resp := doJSON(t, srv, "GET", "/v1/usage?scope_type=tenant&scope_id=t1&resource=api_calls", nil)
	if code != http.StatusOK {
		t.Fatalf("usage failed: code=%d resp=%v", code, resp)
	}
	if resp["used"].(float64) != 40 {
		t.Fatalf("expected used 40, got %v", resp["used"])
	}
	if resp["remaining"].(float64) != 60 {
		t.Fatalf("expected remaining 60, got %v", resp["remaining"])
	}
}

func TestPlanSwitchAPI(t *testing.T) {
	srv, st, rm := newTestServer(t)
	publishRules(t, rm, "t1", 100)

	// Put plans
	ctx := context.Background()
	st.PutPlan(ctx, &model.Plan{ID: "free", Name: "Free", Quotas: []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
	}})
	st.PutPlan(ctx, &model.Plan{ID: "pro", Name: "Pro", Quotas: []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 1000, Window: time.Minute},
	}})

	code, resp := doJSON(t, srv, "POST", "/v1/plans/switch", map[string]interface{}{
		"tenant_id": "t1",
		"plan_id":   "pro",
		"mode":      "immediate",
	})
	if code != http.StatusOK {
		t.Fatalf("plan switch failed: code=%d resp=%v", code, resp)
	}
	if resp["to_plan"] != "pro" {
		t.Fatalf("expected to_plan pro, got %v", resp["to_plan"])
	}
}

func TestRuleVersionsAPI(t *testing.T) {
	srv, _, _ := newTestServer(t)

	// Publish rules
	code, resp := doJSON(t, srv, "POST", "/v1/rules/versions", map[string]interface{}{
		"note": "test",
		"rules": []map[string]interface{}{
			{"scope": map[string]string{"type": "tenant", "id": "t1"}, "resource": "api_calls", "limit": 100, "window": 60000000000},
		},
	})
	if code != http.StatusOK {
		t.Fatalf("publish rules failed: code=%d resp=%v", code, resp)
	}
	if resp["status"] != "draft" {
		t.Fatalf("expected draft, got %v", resp["status"])
	}

	// List versions (returns array)
	req := httptest.NewRequest("GET", "/v1/rules/versions", nil)
	rec := httptest.NewRecorder()
	srv.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list versions failed: code=%d", rec.Code)
	}
	var versions []map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &versions)
	if len(versions) != 1 {
		t.Fatalf("expected 1 version, got %d", len(versions))
	}
}
