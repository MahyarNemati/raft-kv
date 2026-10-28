// Package inmem is an in-process raft.Transport with fault injection:
// partitions, crashed nodes, random message loss, and random delays (which
// also reorder messages). It exists for tests and benchmarks.
package inmem

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/raft"
)

// ErrUnreachable is returned when a message cannot be delivered.
var ErrUnreachable = errors.New("inmem: peer unreachable")

// Faults describes probabilistic message faults applied to every link.
type Faults struct {
	DropRate float64       // probability a request or response is lost
	MinDelay time.Duration // per-message one-way latency lower bound
	MaxDelay time.Duration // upper bound; delays are uniform in [Min, Max]
}

// Network connects in-memory transports.
type Network struct {
	mu       sync.RWMutex
	handlers map[raft.NodeID]raft.Handler
	group    map[raft.NodeID]int // partition group; nodes talk only within a group
	faults   Faults
	rng      *rand.Rand
	rngMu    sync.Mutex
}

// NewNetwork creates an empty network. seed makes fault decisions reproducible
// for a given message order.
func NewNetwork(seed uint64) *Network {
	return &Network{
		handlers: make(map[raft.NodeID]raft.Handler),
		group:    make(map[raft.NodeID]int),
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
	}
}

// Transport returns the outbound transport for node id.
func (n *Network) Transport(id raft.NodeID) raft.Transport {
	return &transport{net: n, from: id}
}

// Register attaches the inbound handler for id. Re-registering replaces the
// previous handler, which models a restart.
func (n *Network) Register(id raft.NodeID, h raft.Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Disconnect removes id's handler; messages to it fail as if it had crashed.
func (n *Network) Disconnect(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

// Partition splits the network. Each argument is one side; nodes not listed
// end up isolated in a group of their own.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = make(map[raft.NodeID]int)
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i + 1
		}
	}
}

// Heal removes every partition.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = make(map[raft.NodeID]int)
}

// SetFaults replaces the probabilistic fault configuration.
func (n *Network) SetFaults(f Faults) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.faults = f
}

func (n *Network) connected(a, b raft.NodeID) bool {
	if len(n.group) == 0 {
		return true
	}
	ga, okA := n.group[a]
	gb, okB := n.group[b]
	return okA && okB && ga == gb
}

func (n *Network) roll() (drop bool, delay time.Duration) {
	n.mu.RLock()
	f := n.faults
	n.mu.RUnlock()
	if f.DropRate == 0 && f.MaxDelay == 0 {
		return false, 0
	}
	n.rngMu.Lock()
	defer n.rngMu.Unlock()
	drop = f.DropRate > 0 && n.rng.Float64() < f.DropRate
	delay = f.MinDelay
	if span := f.MaxDelay - f.MinDelay; span > 0 {
		delay += time.Duration(n.rng.Int64N(int64(span)))
	}
	return drop, delay
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// deliver runs one request/response exchange from -> to through the fault
// model. A lost message looks like a timeout to the caller, as it would on a
// real network; the handler may still have run if only the reply was lost.
func deliver[Req, Resp any](ctx context.Context, n *Network, from, to raft.NodeID, req Req,
	call func(raft.Handler, Req) (Resp, error)) (Resp, error) {
	var zero Resp

	lookup := func() (raft.Handler, bool) {
		n.mu.RLock()
		defer n.mu.RUnlock()
		h, ok := n.handlers[to]
		if !ok || !n.connected(from, to) {
			return nil, false
		}
		if _, ok := n.handlers[from]; !ok {
			return nil, false
		}
		return h, true
	}

	drop, delay := n.roll()
	if err := sleepCtx(ctx, delay); err != nil {
		return zero, err
	}
	h, ok := lookup()
	if !ok {
		return zero, fmt.Errorf("%w: %s -> %s", ErrUnreachable, from, to)
	}
	if drop {
		<-ctx.Done()
		return zero, ctx.Err()
	}
	resp, err := call(h, req)
	if err != nil {
		return zero, err
	}

	drop, delay = n.roll()
	if err := sleepCtx(ctx, delay); err != nil {
		return zero, err
	}
	if _, ok := lookup(); !ok || drop {
		<-ctx.Done()
		return zero, ctx.Err()
	}
	return resp, nil
}

type transport struct {
	net  *Network
	from raft.NodeID
}

func (t *transport) RequestVote(ctx context.Context, to raft.NodeID, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	c := *req
	return deliver(ctx, t.net, t.from, to, &c, raft.Handler.HandleRequestVote)
}

func (t *transport) AppendEntries(ctx context.Context, to raft.NodeID, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	c := *req
	c.Entries = make([]raft.Entry, len(req.Entries))
	for i, e := range req.Entries {
		e.Data = append([]byte(nil), e.Data...)
		c.Entries[i] = e
	}
	return deliver(ctx, t.net, t.from, to, &c, raft.Handler.HandleAppendEntries)
}

func (t *transport) InstallSnapshot(ctx context.Context, to raft.NodeID, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	c := *req
	c.Snapshot.Data = append([]byte(nil), req.Snapshot.Data...)
	return deliver(ctx, t.net, t.from, to, &c, raft.Handler.HandleInstallSnapshot)
}
