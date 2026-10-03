// Package api exposes the quota engine over HTTP/JSON.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/engine"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/model"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/observe"
)

// Server is the HTTP API server.
type Server struct {
	engine  engine.Engine
	metrics *observe.Metrics
	http    *http.Server
}

// New creates an API server bound to addr.
func New(addr string, eng engine.Engine, metrics *observe.Metrics) *Server {
	s := &Server{engine: eng, metrics: metrics}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/reserve", s.handleReserve)
	mux.HandleFunc("/v1/confirm", s.handleConfirm)
	mux.HandleFunc("/v1/cancel", s.handleCancel)
	mux.HandleFunc("/v1/plans/switch", s.handlePlanSwitch)
	mux.HandleFunc("/v1/usage", s.handleUsage)
	mux.HandleFunc("/v1/rules/versions", s.handleRuleVersions)
	mux.HandleFunc("/v1/rules/activate", s.handleRuleActivate)
	mux.HandleFunc("/v1/health", s.handleHealth)
	s.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// ListenAndServe starts the HTTP server.
func (s *Server) ListenAndServe() error {
	return s.http.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

// --- Request/response types ---

type reserveRequest struct {
	TenantID string `json:"tenant_id"`
	TeamID   string `json:"team_id"`
	CallerID string `json:"caller_id"`
	Resource string `json:"resource"`
	Amount   int64  `json:"amount"`
	TTL      string `json:"ttl"`
}

type reserveResponse struct {
	Allowed     bool                `json:"allowed"`
	Reason      model.DenyReason    `json:"reason,omitempty"`
	Reservation *model.Reservation  `json:"reservation,omitempty"`
	RuleVersion int64               `json:"rule_version"`
	DenyScope   *model.Scope        `json:"deny_scope,omitempty"`
	Remaining   int64               `json:"remaining"`
}

type confirmRequest struct {
	ReservationID string `json:"reservation_id"`
	Settled       int64  `json:"settled"`
}

type cancelRequest struct {
	ReservationID string `json:"reservation_id"`
}

type planSwitchRequest struct {
	TenantID string `json:"tenant_id"`
	PlanID   string `json:"plan_id"`
	Mode     string `json:"mode"`
}

type publishRulesRequest struct {
	Note  string             `json:"note"`
	Rules []model.QuotaRule  `json:"rules"`
}

type activateRulesRequest struct {
	Version     int64  `json:"version"`
	GrayPercent int    `json:"gray_percent"`
	Grace       string `json:"grace"`
}

// --- Handlers ---

func (s *Server) handleReserve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req reserveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	ttl := time.Duration(0)
	if req.TTL != "" {
		var err error
		ttl, err = time.ParseDuration(req.TTL)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid ttl")
			return
		}
	}
	result, err := s.engine.Reserve(r.Context(), &engine.ReserveRequest{
		TenantID: req.TenantID,
		TeamID:   req.TeamID,
		CallerID: req.CallerID,
		Resource: model.Resource(req.Resource),
		Amount:   req.Amount,
		TTL:      ttl,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := reserveResponse{
		Allowed:     result.Allowed,
		Reason:      result.Reason,
		Reservation: result.Reservation,
		RuleVersion: result.RuleVersion,
		Remaining:   result.Remaining,
	}
	if result.DenyScope.Type != "" {
		resp.DenyScope = &result.DenyScope
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleConfirm(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req confirmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	res, err := s.engine.Confirm(r.Context(), req.ReservationID, req.Settled)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req cancelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	res, err := s.engine.Cancel(r.Context(), req.ReservationID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handlePlanSwitch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req planSwitchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	sw, err := s.engine.SwitchPlan(r.Context(), req.TenantID, req.PlanID, req.Mode)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sw)
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	scope := model.Scope{
		Type: model.ScopeType(r.URL.Query().Get("scope_type")),
		ID:   r.URL.Query().Get("scope_id"),
	}
	resource := model.Resource(r.URL.Query().Get("resource"))
	u, err := s.metrics.Query(r.Context(), scope, resource)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, u)
}

func (s *Server) handleRuleVersions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.engine.Rules().Versions())
	case http.MethodPost:
		var req publishRulesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		v, err := s.engine.Rules().Publish(r.Context(), req.Note, req.Rules)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, v)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleRuleActivate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req activateRulesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	grace := time.Duration(0)
	if req.Grace != "" {
		var err error
		grace, err = time.ParseDuration(req.Grace)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid grace")
			return
		}
	}
	v, err := s.engine.Rules().Activate(r.Context(), req.Version, req.GrayPercent, grace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
