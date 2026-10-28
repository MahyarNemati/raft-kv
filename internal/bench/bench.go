// Package bench measures throughput and latency of an in-process cluster.
package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/client"
	"github.com/MahyarNemati/raft-kv/internal/cluster"
	"github.com/MahyarNemati/raft-kv/internal/kv"
	"github.com/MahyarNemati/raft-kv/internal/raft"
	"github.com/MahyarNemati/raft-kv/internal/storage"
)

// Workload selects the operation mix.
type Workload string

const (
	Write Workload = "write" // 100% put
	Read  Workload = "read"  // 100% linearizable get
	Mixed Workload = "mixed" // 50% put, 50% linearizable get
)

// Path selects how requests reach the leader.
type Path string

const (
	// Direct calls the leader's kv.Store in process: consensus cost only.
	Direct Path = "direct"
	// HTTP goes through the client library and the JSON API on loopback.
	HTTP Path = "http"
)

// Storage selects the Raft log backend.
type Storage string

const (
	Memory   Storage = "memory"
	WAL      Storage = "wal"       // write-ahead log without fsync
	WALFsync Storage = "wal-fsync" // write-ahead log, fsync on every append
)

// Options describes one benchmark run.
type Options struct {
	Nodes     int
	Clients   int
	Duration  time.Duration
	Warmup    time.Duration
	Workload  Workload
	Path      Path
	Storage   Storage
	ValueSize int
	Keys      int
	// Dir holds WAL directories; a temporary directory is used when empty.
	Dir string
	// Endpoints targets an external cluster over HTTP instead of booting an
	// in-process one. Nodes, Path and Storage are then informational only.
	Endpoints []string
}

// Defaults fills unset fields.
func (o *Options) Defaults() {
	if o.Nodes == 0 {
		o.Nodes = 3
	}
	if o.Clients == 0 {
		o.Clients = 16
	}
	if o.Duration == 0 {
		o.Duration = 5 * time.Second
	}
	if o.Workload == "" {
		o.Workload = Write
	}
	if o.Path == "" {
		o.Path = Direct
	}
	if o.Storage == "" {
		o.Storage = Memory
	}
	if o.ValueSize == 0 {
		o.ValueSize = 64
	}
	if o.Keys == 0 {
		o.Keys = 1000
	}
}

func (o *Options) validate() error {
	switch o.Workload {
	case Write, Read, Mixed:
	default:
		return fmt.Errorf("bench: unknown workload %q", o.Workload)
	}
	if len(o.Endpoints) == 0 {
		switch o.Path {
		case Direct, HTTP:
		default:
			return fmt.Errorf("bench: unknown path %q", o.Path)
		}
		switch o.Storage {
		case Memory, WAL, WALFsync:
		default:
			return fmt.Errorf("bench: unknown storage %q", o.Storage)
		}
	}
	if o.Nodes < 1 || o.Clients < 1 || o.ValueSize < 0 || o.Keys < 1 || o.Duration <= 0 {
		return errors.New("bench: nodes, clients, keys and duration must be positive")
	}
	return nil
}

// Report is the outcome of a run.
type Report struct {
	Options    Options
	Ops        int
	Errors     int
	Elapsed    time.Duration
	Throughput float64 // successful ops per second
	Mean       time.Duration
	P50        time.Duration
	P90        time.Duration
	P99        time.Duration
	Max        time.Duration
}

// String renders the report as one line.
func (r Report) String() string {
	return fmt.Sprintf("%s/%s/%s nodes=%d clients=%d ops=%d errors=%d throughput=%.0f ops/s p50=%s p90=%s p99=%s max=%s",
		r.Options.Workload, r.Options.Path, r.Options.Storage, r.Options.Nodes, r.Options.Clients,
		r.Ops, r.Errors, r.Throughput, round(r.P50), round(r.P90), round(r.P99), round(r.Max))
}

func round(d time.Duration) time.Duration {
	switch {
	case d > time.Millisecond:
		return d.Round(10 * time.Microsecond)
	default:
		return d.Round(time.Microsecond)
	}
}

// Run boots a cluster, drives the workload, and tears the cluster down.
func Run(ctx context.Context, opts Options) (Report, error) {
	opts.Defaults()
	if err := opts.validate(); err != nil {
		return Report{}, err
	}
	if len(opts.Endpoints) > 0 {
		return runExternal(ctx, opts)
	}
	copts := cluster.Options{
		Size: opts.Nodes,
		Seed: 1,
		HTTP: opts.Path == HTTP,
		// Production timeouts: the test-tuned ones cause spurious elections
		// when the benchmark saturates the CPU.
		Raft: func(c *raft.Config) {
			d := raft.DefaultConfig(c.ID, c.Peers)
			c.ElectionTimeout, c.HeartbeatInterval, c.RPCTimeout = d.ElectionTimeout, d.HeartbeatInterval, d.RPCTimeout
		},
	}
	if opts.Storage != Memory {
		dir := opts.Dir
		if dir == "" {
			tmp, err := os.MkdirTemp("", "raftkv-bench-*")
			if err != nil {
				return Report{}, err
			}
			defer os.RemoveAll(tmp)
			dir = tmp
		}
		noSync := opts.Storage == WAL
		copts.Storage = func(id raft.NodeID) (raft.Storage, error) {
			return storage.OpenWAL(filepath.Join(dir, string(id)), storage.WALOptions{NoSync: noSync})
		}
	}
	c, err := cluster.Start(copts)
	if err != nil {
		return Report{}, err
	}
	defer c.Close()
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := c.WaitLeader(wctx); err != nil {
		return Report{}, err
	}

	workers := make([]worker, opts.Clients)
	for i := range workers {
		w, err := newWorker(c, opts, i)
		if err != nil {
			return Report{}, err
		}
		workers[i] = w
	}
	return measure(ctx, workers, opts)
}

func runExternal(ctx context.Context, opts Options) (Report, error) {
	workers := make([]worker, opts.Clients)
	for i := range workers {
		cl, err := client.New(opts.Endpoints, client.WithClientID(fmt.Sprintf("bench-%d-%d", time.Now().UnixNano(), i)))
		if err != nil {
			return Report{}, err
		}
		workers[i] = &httpWorker{cl: cl}
	}
	return measure(ctx, workers, opts)
}

func measure(ctx context.Context, workers []worker, opts Options) (Report, error) {
	// Seed the keyspace so reads hit existing keys.
	if opts.Workload != Write {
		if err := workers[0].preload(ctx, opts.Keys); err != nil {
			return Report{}, fmt.Errorf("bench: preload: %w", err)
		}
	}

	if opts.Warmup > 0 {
		if _, err := drive(ctx, workers, opts, opts.Warmup); err != nil {
			return Report{}, err
		}
	}
	return drive(ctx, workers, opts, opts.Duration)
}

func drive(ctx context.Context, workers []worker, opts Options, d time.Duration) (Report, error) {
	runCtx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	value := strings.Repeat("v", opts.ValueSize)

	type result struct {
		lat  []time.Duration
		errs int
	}
	results := make([]result, len(workers))
	var wg sync.WaitGroup
	start := time.Now()
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i), 99))
			lat := make([]time.Duration, 0, 4096)
			errs := 0
			for runCtx.Err() == nil {
				key := fmt.Sprintf("key-%d", rng.IntN(opts.Keys))
				write := opts.Workload == Write || (opts.Workload == Mixed && rng.IntN(2) == 0)
				opCtx, opCancel := context.WithTimeout(runCtx, 5*time.Second)
				t0 := time.Now()
				var err error
				if write {
					err = workers[i].put(opCtx, key, value)
				} else {
					err = workers[i].get(opCtx, key)
				}
				el := time.Since(t0)
				opCancel()
				if err != nil {
					if runCtx.Err() == nil {
						errs++
					}
					continue
				}
				lat = append(lat, el)
			}
			results[i] = result{lat: lat, errs: errs}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	rep := Report{Options: opts, Elapsed: elapsed}
	for _, r := range results {
		all = append(all, r.lat...)
		rep.Errors += r.errs
	}
	rep.Ops = len(all)
	if rep.Ops == 0 {
		return rep, fmt.Errorf("bench: no operations completed (%d errors)", rep.Errors)
	}
	slices.Sort(all)
	var sum time.Duration
	for _, l := range all {
		sum += l
	}
	pct := func(p float64) time.Duration { return all[min(len(all)-1, int(float64(len(all))*p))] }
	rep.Throughput = float64(rep.Ops) / elapsed.Seconds()
	rep.Mean = sum / time.Duration(len(all))
	rep.P50, rep.P90, rep.P99, rep.Max = pct(0.50), pct(0.90), pct(0.99), all[len(all)-1]
	return rep, nil
}

type worker interface {
	put(ctx context.Context, key, value string) error
	get(ctx context.Context, key string) error
	preload(ctx context.Context, keys int) error
}

func newWorker(c *cluster.Cluster, opts Options, i int) (worker, error) {
	if opts.Path == HTTP {
		cl, err := client.New(c.Endpoints(), client.WithClientID(fmt.Sprintf("bench-%d", i)))
		if err != nil {
			return nil, err
		}
		return &httpWorker{cl: cl}, nil
	}
	return &directWorker{c: c, id: fmt.Sprintf("bench-%d", i)}, nil
}

type directWorker struct {
	c   *cluster.Cluster
	id  string
	seq uint64
}

func (w *directWorker) store() (*kv.Store, error) {
	id, ok := w.c.Leader()
	if !ok {
		return nil, errors.New("no leader")
	}
	s := w.c.Store(id)
	if s == nil {
		return nil, errors.New("leader not running")
	}
	return s, nil
}

func (w *directWorker) put(ctx context.Context, key, value string) error {
	s, err := w.store()
	if err != nil {
		return err
	}
	w.seq++
	_, err = s.Execute(ctx, kv.Command{Op: kv.OpPut, Key: key, Value: value, ClientID: w.id, Seq: w.seq})
	return err
}

func (w *directWorker) get(ctx context.Context, key string) error {
	s, err := w.store()
	if err != nil {
		return err
	}
	_, _, err = s.Get(ctx, key, kv.Linearizable)
	return err
}

func (w *directWorker) preload(ctx context.Context, keys int) error {
	for i := 0; i < keys; i++ {
		if err := w.put(ctx, fmt.Sprintf("key-%d", i), "seed"); err != nil {
			return err
		}
	}
	return nil
}

type httpWorker struct{ cl *client.Client }

func (w *httpWorker) put(ctx context.Context, key, value string) error {
	_, err := w.cl.Put(ctx, key, value)
	return err
}

func (w *httpWorker) get(ctx context.Context, key string) error {
	_, _, err := w.cl.Get(ctx, key, kv.Linearizable)
	return err
}

func (w *httpWorker) preload(ctx context.Context, keys int) error {
	for i := 0; i < keys; i++ {
		if _, err := w.cl.Put(ctx, fmt.Sprintf("key-%d", i), "seed"); err != nil {
			return err
		}
	}
	return nil
}
