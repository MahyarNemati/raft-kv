// Package client is a Go client for the raftkv HTTP API. It follows leader
// hints, retries transient failures, and tags every write with a client ID
// and sequence number so that a retried write executes at most once.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/kv"
	"github.com/MahyarNemati/raft-kv/internal/server"
)

var (
	// ErrOutcomeUnknown is returned when a write may or may not have been
	// applied: the deadline expired while retrying an ambiguous failure.
	ErrOutcomeUnknown = errors.New("client: outcome unknown")
	// ErrRejected is returned when the cluster definitively refused a request.
	ErrRejected = errors.New("client: request rejected")
)

// Client talks to a raftkv cluster. It is safe for concurrent use, but writes
// are serialized: the server keeps one deduplication slot per client, so a
// client must not have two writes in flight. Use several clients for
// parallel write throughput.
type Client struct {
	endpoints []string
	hc        *http.Client
	id        string
	backoff   time.Duration

	writeMu sync.Mutex
	seq     uint64

	mu     sync.Mutex
	leader string
	next   int
}

// Option customizes a Client.
type Option func(*Client)

// WithHTTPClient replaces the underlying HTTP client.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.hc = hc } }

// WithClientID fixes the client ID instead of generating a random one.
func WithClientID(id string) Option { return func(c *Client) { c.id = id } }

// WithBackoff sets the pause between retries against unavailable nodes.
func WithBackoff(d time.Duration) Option { return func(c *Client) { c.backoff = d } }

// New creates a client for the given node base URLs.
func New(endpoints []string, opts ...Option) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("client: at least one endpoint is required")
	}
	c := &Client{
		hc:      &http.Client{Timeout: 10 * time.Second},
		backoff: 50 * time.Millisecond,
	}
	for _, e := range endpoints {
		u, err := url.Parse(e)
		if err != nil || u.Host == "" {
			return nil, fmt.Errorf("client: bad endpoint %q", e)
		}
		c.endpoints = append(c.endpoints, strings.TrimRight(e, "/"))
	}
	for _, o := range opts {
		o(c)
	}
	if c.id == "" {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, fmt.Errorf("client: generate id: %w", err)
		}
		c.id = hex.EncodeToString(b[:])
	}
	return c, nil
}

// ID returns the client's deduplication identity.
func (c *Client) ID() string { return c.id }

// Get reads key. found is false when the key does not exist.
func (c *Client) Get(ctx context.Context, key string, cons kv.Consistency) (value string, found bool, err error) {
	q := ""
	if cons == kv.Stale {
		q = "?consistency=stale"
	}
	var resp server.GetResponse
	err = c.do(ctx, http.MethodGet, "/v1/kv/"+escapeKey(key)+q, nil, &resp, cons == kv.Linearizable, false)
	if errors.Is(err, errNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return resp.Value, resp.Found, nil
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value string) (kv.Result, error) {
	return c.write(ctx, http.MethodPut, "/v1/kv/", key, server.WriteRequest{Value: value})
}

// Delete removes key. Result.OK reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (kv.Result, error) {
	return c.write(ctx, http.MethodDelete, "/v1/kv/", key, server.WriteRequest{})
}

// CAS sets key to value if its current value equals *expected, or if it is
// absent when expected is nil. Result.OK reports whether the swap happened.
func (c *Client) CAS(ctx context.Context, key string, expected *string, value string) (kv.Result, error) {
	req := server.WriteRequest{Value: value}
	if expected == nil {
		req.ExpectAbsent = true
	} else {
		req.Expected = *expected
	}
	return c.write(ctx, http.MethodPost, "/v1/cas/", key, req)
}

// Status fetches /v1/status from a specific endpoint.
func (c *Client) Status(ctx context.Context, endpoint string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(endpoint, "/")+"/v1/status", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("client: decode status: %w", err)
	}
	return out, nil
}

// Endpoints returns the configured node URLs.
func (c *Client) Endpoints() []string { return append([]string(nil), c.endpoints...) }

func (c *Client) write(ctx context.Context, method, prefix, key string, body server.WriteRequest) (kv.Result, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.seq++
	body.ClientID, body.Seq = c.id, c.seq
	var res kv.Result
	err := c.do(ctx, method, prefix+escapeKey(key), body, &res, true, true)
	return res, err
}

var errNotFound = errors.New("not found")

// do runs one logical request, retrying against other nodes until it gets a
// definitive answer or ctx expires. needLeader routes to the cached leader.
func (c *Client) do(ctx context.Context, method, path string, body, out any, needLeader, isWrite bool) error {
	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("client: encode: %w", err)
		}
		payload = b
	}
	var (
		ambiguous bool
		lastErr   error
	)
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if isWrite && ambiguous {
				return fmt.Errorf("%w: %w (last error: %v)", ErrOutcomeUnknown, err, lastErr)
			}
			if lastErr != nil {
				return fmt.Errorf("client: %w (last error: %v)", err, lastErr)
			}
			return err
		}
		ep := c.pick(needLeader)
		retry, amb, err := c.attempt(ctx, method, ep+path, payload, out)
		if err == nil || !retry {
			return err
		}
		ambiguous = ambiguous || amb
		lastErr = err
		if attempt > 0 && attempt%len(c.endpoints) == 0 {
			sleep(ctx, c.backoff)
		}
	}
}

// attempt performs a single HTTP exchange. retry says whether another node
// might succeed; ambiguous says whether a write might have been applied.
func (c *Client) attempt(ctx context.Context, method, u string, payload []byte, out any) (retry, ambiguous bool, err error) {
	var rdr io.Reader
	if payload != nil {
		rdr = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return false, false, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		c.forgetLeader()
		// The request may have reached the server before the connection broke.
		return true, true, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, kv.MaxValueLen*2+4096))
	if err != nil {
		return true, true, err
	}

	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(data, out); err != nil {
			return false, false, fmt.Errorf("client: decode response: %w", err)
		}
		return false, false, nil
	}
	var e server.ErrorResponse
	_ = json.Unmarshal(data, &e)
	if e.Error == "" {
		e.Error = strings.TrimSpace(string(data))
	}
	err = fmt.Errorf("client: %s %s: %s (%s)", method, u, resp.Status, e.Error)

	switch {
	case resp.StatusCode == http.StatusNotFound:
		// A GET miss carries a GetResponse body with found=false.
		return false, false, errNotFound
	case e.Code == server.CodeNotLeader:
		c.setLeader(e.LeaderURL)
		return true, false, err
	case e.Code == server.CodeInvalid:
		return false, false, fmt.Errorf("%w: %s", kv.ErrInvalid, e.Error)
	case e.Code == server.CodeRejected:
		return false, false, fmt.Errorf("%w: %s", ErrRejected, e.Error)
	case e.Code == server.CodeOutcomeUnsure, e.Code == server.CodeTimeout:
		c.forgetLeader()
		return true, true, err
	case resp.StatusCode >= 500:
		c.forgetLeader()
		return true, resp.StatusCode != http.StatusServiceUnavailable, err
	default:
		return false, false, err
	}
}

func (c *Client) pick(needLeader bool) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if needLeader && c.leader != "" {
		return c.leader
	}
	ep := c.endpoints[c.next%len(c.endpoints)]
	c.next++
	return ep
}

func (c *Client) setLeader(u string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = strings.TrimRight(u, "/")
}

func (c *Client) forgetLeader() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = ""
}

func escapeKey(k string) string {
	parts := strings.Split(k, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
