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

// TestEndToEndQuotaLifecycle tests the full quota lifecycle over HTTP:
// publish rules -> reserve -> confirm -> query usage -> deny when exhausted.
func TestEndToEndQuotaLifecycle(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := rules.NewManager(st)
	eng := engine.New(st, rm, engine.DefaultConfig())
	metrics := observe.NewMetrics(st, rm)
	srv := New(":0", eng, metrics)
	ts := httptest.NewServer(srv.http.Handler)
	defer ts.Close()

	ctx := context.Background()

	// Publish and activate rules.
	v, err := rm.Publish(ctx, "init", []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	rm.Activate(ctx, v.Version, 0, 0)

	// Reserve 30.
	res := postJSON(t, ts.URL+"/v1/reserve", map[string]interface{}{
		"tenant_id": "t1", "team_id": "team1", "caller_id": "c1",
		"resource": "api_calls", "amount": 30,
	})
	if res["allowed"] != true {
		t.Fatalf("expected allow, got %v", res)
	}
	resv := res["reservation"].(map[string]interface{})
	resID := resv["id"].(string)

	// Confirm.
	conf := postJSON(t, ts.URL+"/v1/confirm", map[string]interface{}{
		"reservation_id": resID, "settled": 30,
	})
	if conf["status"] != "confirmed" {
		t.Fatalf("expected confirmed, got %v", conf)
	}

	// Query usage.
	usage := getJSON(t, ts.URL+"/v1/usage?scope_type=tenant&scope_id=t1&resource=api_calls")
	if usage["used"].(float64) != 30 {
		t.Fatalf("expected used 30, got %v", usage["used"])
	}
	if usage["remaining"].(float64) != 70 {
		t.Fatalf("expected remaining 70, got %v", usage["remaining"])
	}

	// Reserve 80 more -> should be denied (30+80 > 100).
	denied := postJSON(t, ts.URL+"/v1/reserve", map[string]interface{}{
		"tenant_id": "t1", "team_id": "team1", "caller_id": "c1",
		"resource": "api_calls", "amount": 80,
	})
	if denied["allowed"] != false {
		t.Fatalf("expected deny, got %v", denied)
	}
	if denied["reason"] != "quota_exhausted" {
		t.Fatalf("expected quota_exhausted, got %v", denied["reason"])
	}

	// Deny count should be 1.
	usage2 := getJSON(t, ts.URL+"/v1/usage?scope_type=tenant&scope_id=t1&resource=api_calls")
	if usage2["denies"].(float64) != 1 {
		t.Fatalf("expected 1 deny, got %v", usage2["denies"])
	}
}

// TestEndToEndPlanUpgrade tests plan upgrade over HTTP.
func TestEndToEndPlanUpgrade(t *testing.T) {
	st := store.NewMemoryStore(64)
	rm := rules.NewManager(st)
	eng := engine.New(st, rm, engine.DefaultConfig())
	metrics := observe.NewMetrics(st, rm)
	srv := New(":0", eng, metrics)
	ts := httptest.NewServer(srv.http.Handler)
	defer ts.Close()

	ctx := context.Background()
	st.PutPlan(ctx, &model.Plan{ID: "free", Name: "Free", Quotas: []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 100, Window: time.Minute},
	}})
	st.PutPlan(ctx, &model.Plan{ID: "pro", Name: "Pro", Quotas: []model.QuotaRule{
		{Scope: model.Scope{Type: model.ScopeTenant, ID: "t1"}, Resource: "api_calls", Limit: 1000, Window: time.Minute},
	}})

	// Switch to free.
	postJSON(t, ts.URL+"/v1/plans/switch", map[string]interface{}{
		"tenant_id": "t1", "plan_id": "free", "mode": "immediate",
	})

	// Reserve 100 (free limit).
	r1 := postJSON(t, ts.URL+"/v1/reserve", map[string]interface{}{
		"tenant_id": "t1", "team_id": "team1", "caller_id": "c1",
		"resource": "api_calls", "amount": 100,
	})
	if r1["allowed"] != true {
		t.Fatalf("expected allow on free, got %v", r1)
	}

	// 1 more should be denied.
	r2 := postJSON(t, ts.URL+"/v1/reserve", map[string]interface{}{
		"tenant_id": "t1", "team_id": "team1", "caller_id": "c1",
		"resource": "api_calls", "amount": 1,
	})
	if r2["allowed"] != false {
		t.Fatalf("expected deny on free, got %v", r2)
	}

	// Upgrade to pro.
	postJSON(t, ts.URL+"/v1/plans/switch", map[string]interface{}{
		"tenant_id": "t1", "plan_id": "pro", "mode": "immediate",
	})

	// Now 500 should be allowed.
	r3 := postJSON(t, ts.URL+"/v1/reserve", map[string]interface{}{
		"tenant_id": "t1", "team_id": "team1", "caller_id": "c1",
		"resource": "api_calls", "amount": 500,
	})
	if r3["allowed"] != true {
		t.Fatalf("expected allow after upgrade, got %v", r3)
	}
}

func postJSON(t *testing.T, url string, body interface{}) map[string]interface{} {
	t.Helper()
	data, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result
}

func getJSON(t *testing.T, url string) map[string]interface{} {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result
}
