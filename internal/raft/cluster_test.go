package raft_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/raft"
	"github.com/MahyarNemati/raft-kv/internal/storage"
	"github.com/MahyarNemati/raft-kv/internal/transport/inmem"
)

// recorder is a state machine that remembers every command by index, which
// lets tests assert the State Machine Safety property directly.
type recorder struct {
	mu      sync.Mutex
	applied map[uint64]string
	last    uint64
}

func newRecorder() *recorder { return &recorder{applied: map[uint64]string{}} }

func (r *recorder) Apply(index uint64, data []byte) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index <= r.last {
		panic(fmt.Sprintf("apply out of order: %d after %d", index, r.last))
	}
	r.applied[index] = string(data)
	r.last = index
	return string(data)
}

func (r *recorder) Snapshot() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return json.Marshal(r.applied)
}

func (r *recorder) Restore(b []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := map[uint64]string{}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	r.applied = m
	r.last = 0
	for i := range m {
		r.last = max(r.last, i)
	}
	return nil
}

func (r *recorder) values() map[uint64]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[uint64]string, len(r.applied))
	for k, v := range r.applied {
		out[k] = v
	}
	return out
}

type testCluster struct {
	t       *testing.T
	ids     []raft.NodeID
	net     *inmem.Network
	nodes   map[raft.NodeID]*raft.Node
	fsms    map[raft.NodeID]*recorder
	stores  map[raft.NodeID]raft.Storage
	cfgHook func(*raft.Config)
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testConfig(id raft.NodeID, peers []raft.NodeID) raft.Config {
	c := raft.DefaultConfig(id, peers)
	c.ElectionTimeout = 150 * time.Millisecond
	c.HeartbeatInterval = 30 * time.Millisecond
	c.RPCTimeout = 100 * time.Millisecond
	c.Logger = quietLogger()
	return c
}

func newTestCluster(t *testing.T, n int, hook func(*raft.Config), stores func(raft.NodeID) raft.Storage) *testCluster {
	t.Helper()
	c := &testCluster{
		t:       t,
		net:     inmem.NewNetwork(uint64(n)),
		nodes:   map[raft.NodeID]*raft.Node{},
		fsms:    map[raft.NodeID]*recorder{},
		stores:  map[raft.NodeID]raft.Storage{},
		cfgHook: hook,
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, raft.NodeID(fmt.Sprintf("n%d", i)))
	}
	for _, id := range c.ids {
		if stores != nil {
			c.stores[id] = stores(id)
		} else {
			c.stores[id] = storage.NewMemory()
		}
		c.start(id)
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *testCluster) start(id raft.NodeID) {
	c.t.Helper()
	cfg := testConfig(id, c.ids)
	if c.cfgHook != nil {
		c.cfgHook(&cfg)
	}
	fsm := newRecorder()
	node, err := raft.NewNode(cfg, fsm, c.stores[id], c.net.Transport(id))
	if err != nil {
		c.t.Fatalf("new node %s: %v", id, err)
	}
	c.net.Register(id, node)
	c.nodes[id] = node
	c.fsms[id] = fsm
	node.Start()
}

func (c *testCluster) crash(id raft.NodeID) {
	c.net.Disconnect(id)
	if n := c.nodes[id]; n != nil {
		n.Stop()
	}
	delete(c.nodes, id)
}

func (c *testCluster) stopAll() {
	for id := range c.nodes {
		c.crash(id)
	}
}

func (c *testCluster) waitLeader(among ...raft.NodeID) raft.NodeID {
	c.t.Helper()
	if len(among) == 0 {
		for id := range c.nodes {
			among = append(among, id)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leaders := map[uint64][]raft.NodeID{}
		var best uint64
		for _, id := range among {
			n, ok := c.nodes[id]
			if !ok {
				continue
			}
			st := n.Status()
			if st.Role == raft.Leader.String() {
				leaders[st.Term] = append(leaders[st.Term], id)
				best = max(best, st.Term)
			}
		}
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
		}
		if ls := leaders[best]; len(ls) == 1 {
			return ls[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return ""
}

func (c *testCluster) propose(data string) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			v, err := n.Propose(ctx, []byte(data))
			cancel()
			if err == nil {
				if v.(string) != data {
					c.t.Fatalf("propose returned %v, want %s", v, data)
				}
				return n.Status().CommitIndex
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("could not commit %q", data)
	return 0
}

// waitApplied waits until every running node has applied index.
func (c *testCluster) waitApplied(index uint64) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, n := range c.nodes {
			if n.Status().LastApplied < index {
				done = false
			}
		}
		if done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for id, n := range c.nodes {
		c.t.Logf("%s: %+v", id, n.Status())
	}
	c.t.Fatalf("index %d not applied everywhere", index)
}

// checkSafety asserts that no two nodes applied different commands at the
// same index.
func (c *testCluster) checkSafety() {
	c.t.Helper()
	seen := map[uint64]string{}
	for id, f := range c.fsms {
		for idx, v := range f.values() {
			if prev, ok := seen[idx]; ok && prev != v {
				c.t.Fatalf("state machine safety violated at %d: %q vs %q (%s)", idx, prev, v, id)
			}
			seen[idx] = v
		}
	}
}

func TestElectionSingleLeader(t *testing.T) {
	c := newTestCluster(t, 3, nil, nil)
	leader := c.waitLeader()
	term := c.nodes[leader].Status().Term

	// With a healthy network the leader should stay put.
	time.Sleep(600 * time.Millisecond)
	if got := c.waitLeader(); got != leader {
		t.Fatalf("leader changed from %s to %s without faults", leader, got)
	}
	if st := c.nodes[leader].Status(); st.Term != term {
		t.Fatalf("term moved from %d to %d without faults", term, st.Term)
	}
}

func TestReelectionAfterLeaderCrash(t *testing.T) {
	c := newTestCluster(t, 5, nil, nil)
	old := c.waitLeader()
	idx := c.propose("before")
	c.crash(old)

	newLeader := c.waitLeader()
	if newLeader == old {
		t.Fatal("crashed node still leader")
	}
	idx2 := c.propose("after")
	if idx2 <= idx {
		t.Fatalf("commit index did not advance: %d -> %d", idx, idx2)
	}

	c.start(old)
	c.waitApplied(idx2)
	c.checkSafety()
}

func TestReplicationAllNodesApply(t *testing.T) {
	c := newTestCluster(t, 3, nil, nil)
	c.waitLeader()
	var last uint64
	for i := 0; i < 200; i++ {
		last = c.propose(fmt.Sprintf("cmd-%d", i))
	}
	c.waitApplied(last)
	c.checkSafety()
	for id, f := range c.fsms {
		if n := len(f.values()); n != 200 {
			t.Fatalf("%s applied %d commands, want 200", id, n)
		}
	}
}

func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newTestCluster(t, 5, nil, nil)
	old := c.waitLeader()
	c.propose("base")

	var majority []raft.NodeID
	for _, id := range c.ids {
		if id != old {
			majority = append(majority, id)
		}
	}
	c.net.Partition([]raft.NodeID{old}, majority)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	_, err := c.nodes[old].Propose(ctx, []byte("lost"))
	cancel()
	if err == nil {
		t.Fatal("isolated leader committed a proposal")
	}

	ctx, cancel = context.WithTimeout(context.Background(), 400*time.Millisecond)
	_, err = c.nodes[old].ReadIndex(ctx)
	cancel()
	if err == nil {
		t.Fatal("isolated leader served a linearizable read")
	}

	newLeader := c.waitLeader(majority...)
	idx := c.propose("winner")
	c.net.Heal()
	c.waitApplied(idx)
	c.checkSafety()

	for id, f := range c.fsms {
		for _, v := range f.values() {
			if v == "lost" {
				t.Fatalf("%s applied the partitioned leader's uncommitted entry", id)
			}
		}
	}
	_ = newLeader
}

func TestFollowerCatchesUpViaSnapshot(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *raft.Config) { cfg.SnapshotThreshold = 20 }, nil)
	leader := c.waitLeader()
	var lagger raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			lagger = id
			break
		}
	}
	c.crash(lagger)

	var last uint64
	for i := 0; i < 120; i++ {
		last = c.propose(fmt.Sprintf("v%d", i))
	}
	if st := c.nodes[leader].Status(); st.SnapshotIndex == 0 {
		t.Fatalf("leader did not compact its log: %+v", st)
	}

	c.start(lagger)
	c.waitApplied(last)
	c.checkSafety()
	if st := c.nodes[lagger].Status(); st.SnapshotIndex == 0 {
		t.Fatalf("lagging follower was not caught up by snapshot: %+v", st)
	}
	if n := len(c.fsms[lagger].values()); n != 120 {
		t.Fatalf("lagging follower has %d values, want 120", n)
	}
}

func TestRestartFromWAL(t *testing.T) {
	dirs := map[raft.NodeID]string{}
	open := func(id raft.NodeID) raft.Storage {
		if _, ok := dirs[id]; !ok {
			dirs[id] = t.TempDir()
		}
		w, err := storage.OpenWAL(dirs[id], storage.WALOptions{NoSync: true, Logger: quietLogger()})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	c := newTestCluster(t, 3, func(cfg *raft.Config) { cfg.SnapshotThreshold = 25 }, open)
	c.waitLeader()
	var last uint64
	for i := 0; i < 60; i++ {
		last = c.propose(fmt.Sprintf("w%d", i))
	}
	c.waitApplied(last)
	c.stopAll()
	for id, s := range c.stores {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		c.stores[id] = open(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	c.waitLeader()
	last = c.propose("after-restart")
	c.waitApplied(last)
	c.checkSafety()
	for id, f := range c.fsms {
		if n := len(f.values()); n != 61 {
			t.Fatalf("%s has %d values after restart, want 61", id, n)
		}
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := newTestCluster(t, 1, nil, nil)
	leader := c.waitLeader()
	idx := c.propose("solo")
	if _, err := c.nodes[leader].ReadIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(idx)
}

func TestProposeOnFollowerReturnsLeaderHint(t *testing.T) {
	c := newTestCluster(t, 3, nil, nil)
	leader := c.waitLeader()
	c.propose("x") // ensures followers have heard from the leader
	for id, n := range c.nodes {
		if id == leader {
			continue
		}
		_, err := n.Propose(context.Background(), []byte("y"))
		hint, ok := raft.IsNotLeader(err)
		if !ok {
			t.Fatalf("want NotLeaderError, got %v", err)
		}
		if hint != leader {
			t.Fatalf("hint = %q, want %q", hint, leader)
		}
	}
}

func TestStoppedNodeRejectsWork(t *testing.T) {
	c := newTestCluster(t, 1, nil, nil)
	id := c.waitLeader()
	n := c.nodes[id]
	c.crash(id)
	if _, err := n.Propose(context.Background(), []byte("z")); !errors.Is(err, raft.ErrStopped) {
		t.Fatalf("want ErrStopped, got %v", err)
	}
}
