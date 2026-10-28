// Package cluster runs a complete raftkv cluster inside one process: Raft
// traffic goes over the fault-injecting in-memory network and each node
// serves the real HTTP API on a loopback listener. It backs the fault
// injection tests, the benchmarks and the raftkv-bench tool.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/kv"
	"github.com/MahyarNemati/raft-kv/internal/raft"
	"github.com/MahyarNemati/raft-kv/internal/server"
	"github.com/MahyarNemati/raft-kv/internal/storage"
	"github.com/MahyarNemati/raft-kv/internal/transport/inmem"
)

// Options configures a cluster.
type Options struct {
	Size int
	Seed uint64
	// Raft tweaks each node's config after the test-friendly defaults.
	Raft func(*raft.Config)
	// Storage opens durable state for a node. It is called again on restart
	// and must return state that survived the crash. Defaults to one
	// in-memory store per node, reused across restarts.
	Storage func(id raft.NodeID) (raft.Storage, error)
	// HTTP starts an API server per node. Benchmarks of the bare consensus
	// path turn it off.
	HTTP           bool
	RequestTimeout time.Duration
	Logger         *slog.Logger
}

type member struct {
	id      raft.NodeID
	srv     *httptest.Server
	handler atomic.Pointer[http.Handler]

	storage raft.Storage
	node    *raft.Node
	fsm     *kv.FSM
	store   *kv.Store
}

// Cluster is a running in-process cluster.
type Cluster struct {
	opts    Options
	net     *inmem.Network
	ids     []raft.NodeID
	logger  *slog.Logger
	peerURL map[raft.NodeID]string

	mu      sync.Mutex
	members map[raft.NodeID]*member
}

// FastRaft is a config tuned for in-process clusters: short timeouts so that
// failover in tests takes milliseconds rather than seconds.
func FastRaft(c *raft.Config) {
	c.ElectionTimeout = 150 * time.Millisecond
	c.HeartbeatInterval = 30 * time.Millisecond
	c.RPCTimeout = 100 * time.Millisecond
}

// Start boots a cluster and returns once every node is running. It does not
// wait for a leader; use WaitLeader.
func Start(opts Options) (*Cluster, error) {
	if opts.Size <= 0 {
		return nil, errors.New("cluster: size must be positive")
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = 2 * time.Second
	}
	if opts.Storage == nil {
		mem := map[raft.NodeID]*storage.Memory{}
		var mu sync.Mutex
		opts.Storage = func(id raft.NodeID) (raft.Storage, error) {
			mu.Lock()
			defer mu.Unlock()
			if mem[id] == nil {
				mem[id] = storage.NewMemory()
			}
			return mem[id], nil
		}
	}
	c := &Cluster{
		opts:    opts,
		net:     inmem.NewNetwork(opts.Seed),
		logger:  opts.Logger,
		peerURL: map[raft.NodeID]string{},
		members: map[raft.NodeID]*member{},
	}
	for i := 1; i <= opts.Size; i++ {
		c.ids = append(c.ids, raft.NodeID(fmt.Sprintf("n%d", i)))
	}
	for _, id := range c.ids {
		m := &member{id: id}
		if opts.HTTP {
			m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				h := m.handler.Load()
				if h == nil {
					// A crashed node: drop the connection like a dead process would.
					panic(http.ErrAbortHandler)
				}
				(*h).ServeHTTP(w, r)
			}))
			c.peerURL[id] = m.srv.URL
		}
		c.members[id] = m
	}
	for _, id := range c.ids {
		if err := c.Restart(id); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

// IDs returns the member IDs.
func (c *Cluster) IDs() []raft.NodeID { return append([]raft.NodeID(nil), c.ids...) }

// Network exposes the fault injection controls.
func (c *Cluster) Network() *inmem.Network { return c.net }

// Endpoints returns the HTTP base URLs of every node, crashed or not.
func (c *Cluster) Endpoints() []string {
	out := make([]string, 0, len(c.ids))
	for _, id := range c.ids {
		out = append(out, c.peerURL[id])
	}
	return out
}

// Restart starts id from its durable state. Starting a running node is an error.
func (c *Cluster) Restart(id raft.NodeID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.members[id]
	if !ok {
		return fmt.Errorf("cluster: unknown node %s", id)
	}
	if m.node != nil {
		return fmt.Errorf("cluster: node %s already running", id)
	}
	st, err := c.opts.Storage(id)
	if err != nil {
		return fmt.Errorf("cluster: open storage for %s: %w", id, err)
	}
	cfg := raft.DefaultConfig(id, c.ids)
	FastRaft(&cfg)
	cfg.Logger = c.logger
	if c.opts.Raft != nil {
		c.opts.Raft(&cfg)
	}
	fsm := kv.NewFSM()
	node, err := raft.NewNode(cfg, fsm, st, c.net.Transport(id))
	if err != nil {
		return fmt.Errorf("cluster: start %s: %w", id, err)
	}
	m.storage, m.node, m.fsm = st, node, fsm
	m.store = kv.NewStore(node, fsm)
	c.net.Register(id, node)
	node.Start()
	if c.opts.HTTP {
		h := server.New(m.store, node, c.peerURL, c.opts.RequestTimeout, c.logger).Handler()
		m.handler.Store(&h)
	}
	return nil
}

// Crash stops id abruptly. Its durable storage is kept for Restart.
func (c *Cluster) Crash(id raft.NodeID) {
	c.mu.Lock()
	m := c.members[id]
	if m == nil || m.node == nil {
		c.mu.Unlock()
		return
	}
	node, st := m.node, m.storage
	m.handler.Store(nil)
	m.node, m.store, m.fsm, m.storage = nil, nil, nil, nil
	c.mu.Unlock()

	c.net.Disconnect(id)
	node.Stop()
	if err := st.Close(); err != nil {
		c.logger.Warn("close storage", "node", string(id), "err", err)
	}
}

// Running reports whether id is up.
func (c *Cluster) Running(id raft.NodeID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[id]
	return m != nil && m.node != nil
}

// Node returns the running Raft node for id, or nil.
func (c *Cluster) Node(id raft.NodeID) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.members[id]; m != nil {
		return m.node
	}
	return nil
}

// Store returns the KV service of a running node, or nil.
func (c *Cluster) Store(id raft.NodeID) *kv.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.members[id]; m != nil {
		return m.store
	}
	return nil
}

// FSM returns the state machine of a running node, or nil.
func (c *Cluster) FSM(id raft.NodeID) *kv.FSM {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m := c.members[id]; m != nil {
		return m.fsm
	}
	return nil
}

// Leader returns the leader with the highest term among running nodes.
func (c *Cluster) Leader() (raft.NodeID, bool) {
	var (
		best raft.NodeID
		term uint64
	)
	for _, id := range c.ids {
		n := c.Node(id)
		if n == nil {
			continue
		}
		st := n.Status()
		if st.Role == raft.Leader.String() && st.Term >= term {
			best, term = id, st.Term
		}
	}
	return best, best != ""
}

// WaitLeader blocks until some running node is leader and has committed an
// entry in its term, so it can serve reads and writes immediately.
func (c *Cluster) WaitLeader(ctx context.Context) (raft.NodeID, error) {
	for {
		if id, ok := c.Leader(); ok {
			if n := c.Node(id); n != nil {
				rctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				_, err := n.ReadIndex(rctx)
				cancel()
				if err == nil {
					return id, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("cluster: no leader: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// WaitConverged blocks until every running node has applied the leader's
// commit index.
func (c *Cluster) WaitConverged(ctx context.Context) error {
	for {
		id, ok := c.Leader()
		if ok {
			target := c.Node(id).Status().CommitIndex
			done := true
			for _, other := range c.ids {
				if n := c.Node(other); n != nil && n.Status().LastApplied < target {
					done = false
				}
			}
			if done {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cluster: not converged: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Close stops every node and server.
func (c *Cluster) Close() {
	for _, id := range c.ids {
		c.Crash(id)
	}
	for _, m := range c.members {
		if m.srv != nil {
			m.srv.Close()
		}
	}
}
