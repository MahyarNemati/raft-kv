// Package server exposes the key-value store over HTTP/JSON.
//
//	GET    /v1/kv/{key}?consistency=linearizable|stale
//	PUT    /v1/kv/{key}      {"value": "...", "client_id": "...", "seq": 1}
//	DELETE /v1/kv/{key}      {"client_id": "...", "seq": 2}   (body optional)
//	POST   /v1/cas/{key}     {"expected": "...", "expect_absent": false, "value": "...", ...}
//	GET    /v1/status
//	GET    /healthz, /readyz
//
// Writes and linearizable reads on a follower fail with 421 Misdirected
// Request and a leader hint; clients retry against the leader.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/kv"
	"github.com/MahyarNemati/raft-kv/internal/raft"
)

const maxRequestBody = kv.MaxValueLen*2 + 4096

// StatusSource reports node state for leader hints and health checks.
type StatusSource interface {
	Status() raft.Status
}

// Server serves the client API.
type Server struct {
	store   *kv.Store
	node    StatusSource
	peers   map[raft.NodeID]string
	timeout time.Duration
	logger  *slog.Logger
}

// New builds a server. peers maps node IDs to the base URLs clients should
// use to reach them, which is how leader hints become redirect targets.
func New(store *kv.Store, node StatusSource, peers map[raft.NodeID]string, timeout time.Duration, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{store: store, node: node, peers: peers, timeout: timeout, logger: logger}
}

// Handler returns the routed HTTP handler with request logging.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key...}", s.handleGet)
	mux.HandleFunc("PUT /v1/kv/{key...}", s.handlePut)
	mux.HandleFunc("DELETE /v1/kv/{key...}", s.handleDelete)
	mux.HandleFunc("POST /v1/cas/{key...}", s.handleCAS)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.handleReady)
	return s.logRequests(mux)
}

// WriteRequest is the body of PUT, DELETE and CAS.
type WriteRequest struct {
	Value        string `json:"value"`
	Expected     string `json:"expected,omitempty"`
	ExpectAbsent bool   `json:"expect_absent,omitempty"`
	ClientID     string `json:"client_id,omitempty"`
	Seq          uint64 `json:"seq,omitempty"`
}

// GetResponse is the body of a successful GET.
type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
}

// ErrorResponse is returned for every non-2xx status.
type ErrorResponse struct {
	Error     string `json:"error"`
	Code      string `json:"code"`
	LeaderID  string `json:"leader_id,omitempty"`
	LeaderURL string `json:"leader_url,omitempty"`
}

// Error codes clients can switch on.
const (
	CodeInvalid       = "invalid_request"
	CodeNotLeader     = "not_leader"
	CodeUnavailable   = "unavailable"
	CodeTimeout       = "timeout"
	CodeRejected      = "rejected"
	CodeInternal      = "internal"
	CodeNotFound      = "not_found"
	CodeOutcomeUnsure = "outcome_unknown"
)

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	cons, err := kv.ParseConsistency(r.URL.Query().Get("consistency"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	v, found, err := s.store.Get(ctx, key, cons)
	if err != nil {
		s.writeError(w, err)
		return
	}
	status := http.StatusOK
	if !found {
		status = http.StatusNotFound
	}
	writeJSON(w, status, GetResponse{Key: key, Value: v, Found: found})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	s.write(w, r, kv.OpPut, true)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	s.write(w, r, kv.OpDelete, false)
}

func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request) {
	s.write(w, r, kv.OpCAS, true)
}

func (s *Server) write(w http.ResponseWriter, r *http.Request, op kv.Op, bodyRequired bool) {
	var req WriteRequest
	if err := decodeBody(r, &req, bodyRequired); err != nil {
		s.writeError(w, err)
		return
	}
	cmd := kv.Command{
		Op:           op,
		Key:          r.PathValue("key"),
		Value:        req.Value,
		Expected:     req.Expected,
		ExpectAbsent: req.ExpectAbsent,
		ClientID:     req.ClientID,
		Seq:          req.Seq,
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	res, err := s.store.Execute(ctx, cmd)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.node.Status())
}

// handleReady reports ready once the node knows a leader, so load balancers
// stop routing to a node that is partitioned away.
func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	st := s.node.Status()
	if st.Leader == "" {
		writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{Error: "no known leader", Code: CodeUnavailable})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "leader": st.Leader, "role": st.Role})
}

var errBadBody = errors.New("bad request body")

func decodeBody(r *http.Request, dst any, required bool) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxRequestBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) && !required {
			return nil
		}
		return errors.Join(kv.ErrInvalid, errBadBody, err)
	}
	return nil
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	resp := ErrorResponse{Error: err.Error()}
	var status int
	switch leader, notLeader := raft.IsNotLeader(err); {
	case errors.Is(err, kv.ErrInvalid):
		status, resp.Code = http.StatusBadRequest, CodeInvalid
	case notLeader:
		status, resp.Code = http.StatusMisdirectedRequest, CodeNotLeader
		if leader != "" {
			resp.LeaderID = string(leader)
			resp.LeaderURL = s.peers[leader]
			w.Header().Set("X-Raft-Leader", resp.LeaderURL)
		}
	case errors.Is(err, kv.ErrRejected):
		status, resp.Code = http.StatusConflict, CodeRejected
	case errors.Is(err, raft.ErrLeadershipLost):
		status, resp.Code = http.StatusServiceUnavailable, CodeOutcomeUnsure
	case errors.Is(err, raft.ErrStopped):
		status, resp.Code = http.StatusServiceUnavailable, CodeUnavailable
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		status, resp.Code = http.StatusGatewayTimeout, CodeTimeout
	default:
		status, resp.Code = http.StatusInternalServerError, CodeInternal
		s.logger.Error("request failed", "err", err)
	}
	writeJSON(w, status, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		level := slog.LevelDebug
		if rec.status >= 500 {
			level = slog.LevelWarn
		}
		s.logger.Log(r.Context(), level, "http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration_ms", float64(time.Since(start).Microseconds())/1000)
	})
}
