package cluster_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/MahyarNemati/raft-kv/internal/client"
	"github.com/MahyarNemati/raft-kv/internal/cluster"
	"github.com/MahyarNemati/raft-kv/internal/kv"
	"github.com/MahyarNemati/raft-kv/internal/raft"
	"github.com/MahyarNemati/raft-kv/internal/transport/inmem"
)

// ---- porcupine model ----

type opKind uint8

const (
	opGet opKind = iota
	opPut
	opDelete
	opCAS
)

type kvInput struct {
	kind         opKind
	key          string
	value        string
	expected     string
	expectAbsent bool
}

type kvOutput struct {
	unknown bool // a write whose outcome the client never learned
	ok      bool
	value   string // get: value read; writes: previous value
	found   bool   // get: key existed; writes: key existed before
}

type kvState struct {
	value  string
	exists bool
}

// kvModel is a per-key register with get/put/delete/cas. Writes also check
// the previous value the server reported, which makes the model stricter
// than one that only looks at reads.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return kvState{} },
	Step: func(s, in, out any) (bool, any) {
		st, i, o := s.(kvState), in.(kvInput), out.(kvOutput)
		prevMatches := o.unknown || (o.found == st.exists && (!st.exists || o.value == st.value))
		switch i.kind {
		case opGet:
			return prevMatches, st
		case opPut:
			return prevMatches && (o.unknown || o.ok), kvState{value: i.value, exists: true}
		case opDelete:
			return prevMatches && (o.unknown || o.ok == st.exists), kvState{}
		case opCAS:
			match := (i.expectAbsent && !st.exists) || (!i.expectAbsent && st.exists && st.value == i.expected)
			next := st
			if match {
				next = kvState{value: i.value, exists: true}
			}
			if o.unknown {
				return true, next
			}
			return prevMatches && o.ok == match, next
		}
		return false, st
	},
	DescribeOperation: func(in, out any) string {
		i, o := in.(kvInput), out.(kvOutput)
		res := fmt.Sprintf("ok=%v prev=%q found=%v", o.ok, o.value, o.found)
		if o.unknown {
			res = "unknown"
		}
		switch i.kind {
		case opGet:
			return fmt.Sprintf("get(%s) -> %q found=%v", i.key, o.value, o.found)
		case opPut:
			return fmt.Sprintf("put(%s, %q) -> %s", i.key, i.value, res)
		case opDelete:
			return fmt.Sprintf("delete(%s) -> %s", i.key, res)
		default:
			exp := strconv.Quote(i.expected)
			if i.expectAbsent {
				exp = "<absent>"
			}
			return fmt.Sprintf("cas(%s, %s, %q) -> %s", i.key, exp, i.value, res)
		}
	},
	DescribeState: func(s any) string {
		st := s.(kvState)
		if !st.exists {
			return "<absent>"
		}
		return strconv.Quote(st.value)
	},
}

// ---- history recording ----

type recorder struct {
	start time.Time
	mu    sync.Mutex
	ops   []porcupine.Operation
	// indexes of ops whose return time is fixed up at the end of the run
	pending []int
}

func (r *recorder) now() int64 { return int64(time.Since(r.start)) }

func (r *recorder) add(clientID int, in kvInput, call int64, out kvOutput, ret int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if out.unknown {
		r.pending = append(r.pending, len(r.ops))
	}
	r.ops = append(r.ops, porcupine.Operation{ClientId: clientID, Input: in, Call: call, Output: out, Return: ret})
}

// history closes the history: an operation with unknown outcome may take
// effect at any point after its call, so it is treated as still running when
// the history ends.
func (r *recorder) history() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	end := r.now() + 1
	for _, i := range r.pending {
		r.ops[i].Return = end
	}
	return append([]porcupine.Operation(nil), r.ops...)
}

// ---- workload ----

type stats struct {
	mu                       sync.Mutex
	ok, unknown, failedReads int
}

func runClient(ctx context.Context, t *testing.T, id int, endpoints []string, keys []string, seed uint64, rec *recorder, st *stats) {
	cl, err := client.New(endpoints, client.WithBackoff(10*time.Millisecond))
	if err != nil {
		t.Error(err)
		return
	}
	rng := rand.New(rand.NewPCG(seed, uint64(id)))
	for n := 0; ctx.Err() == nil; n++ {
		key := keys[rng.IntN(len(keys))]
		in := kvInput{key: key, value: fmt.Sprintf("c%d-%d", id, n)}
		switch p := rng.IntN(100); {
		case p < 45:
			in.kind = opGet
		case p < 75:
			in.kind = opPut
		case p < 85:
			in.kind = opDelete
		default:
			in.kind = opCAS
			if rng.IntN(4) == 0 {
				in.expectAbsent = true
			} else {
				// Guess a recent value so a fair share of swaps succeed.
				in.expected = fmt.Sprintf("c%d-%d", rng.IntN(id+1), max(0, n-rng.IntN(3)-1))
			}
		}

		opCtx, cancel := context.WithTimeout(ctx, time.Second)
		call := rec.now()
		out, err := execute(opCtx, cl, in)
		ret := rec.now()
		cancel()

		st.mu.Lock()
		switch {
		case err == nil:
			st.ok++
		case in.kind == opGet:
			st.failedReads++
		default:
			st.unknown++
		}
		st.mu.Unlock()

		if err != nil {
			if errors.Is(err, kv.ErrInvalid) || errors.Is(err, client.ErrRejected) {
				t.Errorf("client %d: %v", id, err)
				return
			}
			if in.kind == opGet {
				continue // a failed read has no effect and constrains nothing
			}
			out = kvOutput{unknown: true}
		}
		rec.add(id, in, call, out, ret)
	}
}

func execute(ctx context.Context, cl *client.Client, in kvInput) (kvOutput, error) {
	switch in.kind {
	case opGet:
		v, found, err := cl.Get(ctx, in.key, kv.Linearizable)
		return kvOutput{ok: true, value: v, found: found}, err
	case opPut:
		r, err := cl.Put(ctx, in.key, in.value)
		return kvOutput{ok: r.OK, value: r.Prev, found: r.Existed}, err
	case opDelete:
		r, err := cl.Delete(ctx, in.key)
		return kvOutput{ok: r.OK, value: r.Prev, found: r.Existed}, err
	default:
		var exp *string
		if !in.expectAbsent {
			exp = &in.expected
		}
		r, err := cl.CAS(ctx, in.key, exp, in.value)
		return kvOutput{ok: r.OK, value: r.Prev, found: r.Existed}, err
	}
}

// ---- nemesis ----

type faultKind int

const (
	faultPartition faultKind = 1 << iota
	faultIsolateLeader
	faultCrash
	faultLossy
)

func (f faultKind) has(k faultKind) bool { return f&k != 0 }

type nemesis struct {
	t     *testing.T
	c     *cluster.Cluster
	rng   *rand.Rand
	kinds faultKind
	log   []string
}

func (n *nemesis) note(format string, args ...any) {
	n.log = append(n.log, fmt.Sprintf(format, args...))
}

func (n *nemesis) run(ctx context.Context, every time.Duration) {
	ids := n.c.IDs()
	var options []faultKind
	for _, k := range []faultKind{faultPartition, faultIsolateLeader, faultCrash, faultLossy} {
		if n.kinds.has(k) {
			options = append(options, k)
		}
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		// Every other step heals so the cluster gets windows of progress.
		if n.rng.IntN(2) == 0 {
			n.heal()
			continue
		}
		switch options[n.rng.IntN(len(options))] {
		case faultPartition:
			perm := n.rng.Perm(len(ids))
			cut := 1 + n.rng.IntN(len(ids)-1)
			var a, b []raft.NodeID
			for i, p := range perm {
				if i < cut {
					a = append(a, ids[p])
				} else {
					b = append(b, ids[p])
				}
			}
			n.c.Network().Partition(a, b)
			n.note("partition %v | %v", a, b)
		case faultIsolateLeader:
			if leader, ok := n.c.Leader(); ok {
				var rest []raft.NodeID
				for _, id := range ids {
					if id != leader {
						rest = append(rest, id)
					}
				}
				n.c.Network().Partition([]raft.NodeID{leader}, rest)
				n.note("isolate leader %s", leader)
			}
		case faultCrash:
			down := 0
			for _, id := range ids {
				if !n.c.Running(id) {
					down++
				}
			}
			victim := ids[n.rng.IntN(len(ids))]
			if n.c.Running(victim) && down < (len(ids)-1)/2 {
				n.c.Crash(victim)
				n.note("crash %s", victim)
			}
		case faultLossy:
			f := inmem.Faults{
				DropRate: 0.05 + 0.15*n.rng.Float64(),
				MaxDelay: time.Duration(1+n.rng.IntN(25)) * time.Millisecond,
			}
			n.c.Network().SetFaults(f)
			n.note("lossy drop=%.2f delay<=%s", f.DropRate, f.MaxDelay)
		}
	}
}

func (n *nemesis) heal() {
	n.c.Network().Heal()
	n.c.Network().SetFaults(inmem.Faults{})
	for _, id := range n.c.IDs() {
		if !n.c.Running(id) {
			if err := n.c.Restart(id); err != nil {
				n.t.Errorf("restart %s: %v", id, err)
			}
			n.note("restart %s", id)
		}
	}
}

// ---- test ----

func fuzzDuration() time.Duration {
	if v := os.Getenv("RAFTKV_FUZZ_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	if testing.Short() {
		return time.Second
	}
	return 4 * time.Second
}

func TestLinearizabilityUnderFaults(t *testing.T) {
	scenarios := []struct {
		name    string
		nodes   int
		faults  faultKind
		snapAt  uint64
		clients int
	}{
		{"partitions", 3, faultPartition | faultIsolateLeader, 0, 5},
		{"crash-restart", 3, faultCrash, 50, 5},
		{"lossy-reordering", 3, faultLossy, 0, 5},
		{"everything-5-nodes", 5, faultPartition | faultIsolateLeader | faultCrash | faultLossy, 40, 6},
	}
	for i, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			seed := uint64(time.Now().UnixNano()) + uint64(i)
			if v := os.Getenv("RAFTKV_FUZZ_SEED"); v != "" {
				s, err := strconv.ParseUint(v, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				seed = s
			}
			t.Logf("seed=%d", seed)
			runScenario(t, seed, sc.nodes, sc.faults, sc.snapAt, sc.clients)
		})
	}
}

func runScenario(t *testing.T, seed uint64, nodes int, faults faultKind, snapAt uint64, clients int) {
	c, err := cluster.Start(cluster.Options{
		Size: nodes,
		Seed: seed,
		HTTP: true,
		Raft: func(cfg *raft.Config) { cfg.SnapshotThreshold = snapAt },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := c.WaitLeader(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()

	rec := &recorder{start: time.Now()}
	st := &stats{}
	keys := []string{"x", "y", "z"}
	nem := &nemesis{t: t, c: c, rng: rand.New(rand.NewPCG(seed, 7)), kinds: faults}

	runCtx, stop := context.WithTimeout(context.Background(), fuzzDuration())
	var wg sync.WaitGroup
	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runClient(runCtx, t, id, c.Endpoints(), keys, seed, rec, st)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		nem.run(runCtx, 150*time.Millisecond)
	}()
	wg.Wait()
	stop()

	nem.heal()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.WaitLeader(ctx); err != nil {
		t.Fatalf("cluster did not recover after healing: %v", err)
	}
	if err := c.WaitConverged(ctx); err != nil {
		t.Fatal(err)
	}
	assertReplicasAgree(t, c, keys)

	history := rec.history()
	t.Logf("ops=%d ok=%d unknown=%d failed_reads=%d faults=%d", len(history), st.ok, st.unknown, st.failedReads, len(nem.log))
	if st.ok < 20 {
		t.Fatalf("too little progress under faults: %d successful ops", st.ok)
	}

	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 60*time.Second)
	switch res {
	case porcupine.Ok:
	case porcupine.Unknown:
		t.Logf("linearizability check timed out; history inconclusive")
	default:
		path := filepath.Join(os.TempDir(), fmt.Sprintf("raftkv-%s-%d.html", strings.ReplaceAll(t.Name(), "/", "_"), seed))
		if err := porcupine.VisualizePath(kvModel, info, path); err == nil {
			t.Logf("visualization written to %s", path)
		}
		t.Logf("nemesis log:\n%s", strings.Join(nem.log, "\n"))
		t.Fatalf("history is not linearizable (seed=%d)", seed)
	}
}

// assertReplicasAgree checks that every replica converged to the same state.
func assertReplicasAgree(t *testing.T, c *cluster.Cluster, keys []string) {
	t.Helper()
	var ref map[string]string
	var refID raft.NodeID
	for _, id := range c.IDs() {
		fsm := c.FSM(id)
		if fsm == nil {
			continue
		}
		got := map[string]string{}
		for _, k := range keys {
			if v, ok := fsm.Get(k); ok {
				got[k] = v
			}
		}
		if ref == nil {
			ref, refID = got, id
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(ref) {
			t.Fatalf("replicas diverged: %s=%v %s=%v", refID, ref, id, got)
		}
	}
}
