// Package httpjson carries Raft RPCs as JSON over HTTP. It favors
// debuggability (curl-able, readable on the wire) over raw throughput.
package httpjson

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/raft"
)

const (
	pathVote     = "/raft/v1/request-vote"
	pathAppend   = "/raft/v1/append-entries"
	pathSnapshot = "/raft/v1/install-snapshot"

	maxBody = 256 << 20
)

// Client implements raft.Transport over HTTP.
type Client struct {
	peers map[raft.NodeID]string // id -> base URL
	hc    *http.Client
}

// NewClient returns a transport that reaches each peer at its base URL,
// for example "http://10.0.0.2:7001".
func NewClient(peers map[raft.NodeID]string) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 16
	tr.IdleConnTimeout = 90 * time.Second
	urls := make(map[raft.NodeID]string, len(peers))
	for id, u := range peers {
		urls[id] = strings.TrimRight(u, "/")
	}
	return &Client{peers: urls, hc: &http.Client{Transport: tr}}
}

func (c *Client) RequestVote(ctx context.Context, to raft.NodeID, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	var resp raft.RequestVoteResponse
	return &resp, c.call(ctx, to, pathVote, req, &resp)
}

func (c *Client) AppendEntries(ctx context.Context, to raft.NodeID, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	var resp raft.AppendEntriesResponse
	return &resp, c.call(ctx, to, pathAppend, req, &resp)
}

func (c *Client) InstallSnapshot(ctx context.Context, to raft.NodeID, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	var resp raft.InstallSnapshotResponse
	return &resp, c.call(ctx, to, pathSnapshot, req, &resp)
}

func (c *Client) call(ctx context.Context, to raft.NodeID, path string, in, out any) error {
	base, ok := c.peers[to]
	if !ok {
		return fmt.Errorf("httpjson: unknown peer %q", to)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("httpjson: encode: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("httpjson: %s %s: %s: %s", to, path, resp.Status, bytes.TrimSpace(msg))
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(out); err != nil {
		return fmt.Errorf("httpjson: decode: %w", err)
	}
	return nil
}

// Handler serves inbound RPCs for a raft.Handler.
func Handler(h raft.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+pathVote, serve(h.HandleRequestVote))
	mux.HandleFunc("POST "+pathAppend, serve(h.HandleAppendEntries))
	mux.HandleFunc("POST "+pathSnapshot, serve(h.HandleInstallSnapshot))
	return mux
}

func serve[Req, Resp any](fn func(*Req) (*Resp, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Req
		if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := fn(&req)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, raft.ErrStopped) {
				status = http.StatusServiceUnavailable
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
