# raft-kv

[![CI](https://github.com/MahyarNemati/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/MahyarNemati/raft-kv/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/MahyarNemati/raft-kv)](https://goreportcard.com/report/github.com/MahyarNemati/raft-kv)

A replicated key-value store built on a from-scratch implementation of the
[Raft consensus algorithm](https://raft.github.io/raft.pdf) — leader election,
log replication, snapshotting and membership — with a crash-safe write-ahead
log, a fault-injecting network for testing partitions, and linearizability
checking of real execution histories.

No `hashicorp/raft`. The only non-stdlib dependency is
[porcupine](https://github.com/anishathalye/porcupine), used in tests.

```bash
make build && make cluster              # 3 nodes on 127.0.0.1:7001-7003
./bin/raftkvctl put hello world
./bin/raftkvctl get hello               # world
./bin/raftkvctl status                  # who is leader, commit index, term
```

## Quickstart

### Local processes

```bash
make build
./scripts/local-cluster.sh start        # or: make cluster
./scripts/local-cluster.sh status
./scripts/local-cluster.sh stop
```

### Docker

```bash
docker compose up --build

docker compose exec n1 raftkvctl -endpoints http://n1:7000 put hello world
docker compose stop n1                  # kill the leader
docker compose exec n2 raftkvctl -endpoints http://n2:7000 get hello
```

Stopping the leader is the demo worth running: a new leader is elected within
an election timeout and the previously written key is still there, because it
was committed to a majority before the write was acknowledged.

## Why this exists

Everyone has read the Raft paper. The paper is also where the interesting work
*starts*: it specifies the protocol but leaves the engineering — durable log
storage, torn writes, snapshot installation, how you convince yourself any of
it is correct — as an exercise.

This repository is that exercise, done properly. The headline is not "I
implemented Raft", it is **how it is tested**.

## Architecture

```
       raftkvctl ──HTTP──┐
                         ▼
                   ┌──────────┐   GET/PUT/DELETE/CAS /v1/kv/{key}
                   │  server  │   GET /v1/status, /healthz, /readyz
                   └────┬─────┘
                        │ proposals
                        ▼
                   ┌──────────┐        ┌───────────────┐
                   │  raft    │◀──────▶│   transport   │  httpjson (real)
                   │  node    │  RPC   │               │  inmem (fault-inject)
                   │          │        └───────────────┘
                   │ election │
                   │ replicate│───┐
                   └────┬─────┘   │ apply committed entries
                        │         ▼
                        │    ┌─────────┐
                        │    │ kv FSM  │  deterministic state machine
                        │    └─────────┘
                        ▼
                   ┌──────────┐
                   │ storage  │  memory | WAL (CRC32 + snapshot compaction)
                   └──────────┘
```

| Package | Responsibility |
|---|---|
| `internal/raft` | the consensus core: node, log, terms, elections, replication |
| `internal/storage` | durable log: CRC-checked WAL, snapshots, compaction |
| `internal/kv` | the replicated state machine, its commands and store |
| `internal/transport/httpjson` | real peer RPC over HTTP/JSON |
| `internal/transport/inmem` | in-process network that drops, delays and partitions |
| `internal/server` | the client-facing HTTP API |
| `internal/client` | Go client with leader discovery and retry |
| `internal/cluster` | harness + linearizability tests |
| `internal/bench` | throughput/latency measurement |

## How it is tested — the part that matters

### Linearizability, not assertions

Most consensus tests assert specific outcomes: "after this sequence, the value
is X". That only tests the sequences you imagined.

Instead, operations are recorded as a **history** of concurrent
invoke/return events, and [porcupine](https://github.com/anishathalye/porcupine)
checks whether that history is *linearizable at all* — whether there exists some
sequential ordering of the operations, consistent with real time, that a correct
single-node register could have produced. If no such ordering exists, the
history is proof of a real bug, even one nobody predicted.

CI runs this suite with `-count=5` because consensus bugs are probabilistic.
Passing once is not evidence.

### Faults are induced, not hoped for

`internal/transport/inmem` deliberately drops messages, delays them, reorders
them and partitions the cluster. Consensus code that has never been partitioned
is untested code — the whole point of Raft is behaviour under partition, so a
test suite that only runs on a healthy network tests nothing interesting.

### The WAL is tested for crashes, not just writes

Anyone can append records to a file. The cases that matter are the ugly ones,
and `internal/storage/wal_test.go` covers them:

- **Torn tails.** The process dies mid-write, leaving a partial record. On
  replay, that record must be discarded — and everything before it kept. Each
  record carries a CRC32 so a truncated or corrupted tail is *detectable*
  rather than silently loaded as data.
- **Replay.** A restarted node reconstructs its log and resumes at the right
  index and term.
- **Compaction.** Snapshots let the log be truncated without losing state, and
  replay after compaction has to splice snapshot plus suffix correctly.

`RAFTKV_FSYNC=true` (the default) fsyncs before acknowledging. Turning it off is
faster and **unsafe** — the benchmark measures both so the cost of durability is
a number, not a feeling.

## HTTP API

| Method | Path | Notes |
|---|---|---|
| `GET` | `/v1/kv/{key}` | reads go through the leader for linearizability |
| `PUT` | `/v1/kv/{key}` | body is the value |
| `DELETE` | `/v1/kv/{key}` | |
| `POST` | `/v1/cas/{key}` | compare-and-swap; the primitive that makes this useful for locks and leader election |
| `GET` | `/v1/status` | leader, term, commit index, applied index |
| `GET` | `/healthz` | process is up |
| `GET` | `/readyz` | node is part of a quorum and caught up |

Followers redirect writes to the leader; the Go client and `raftkvctl` follow
that automatically.

### CLI

```bash
raftkvctl [-endpoints URL,...] [-timeout 5s] <command>

  get [-stale] <key>              print the value; exit 2 if absent
  put <key> <value>               set key
  delete <key>                    remove key (aliases: del, rm)
  cas <key> <expected> <value>    swap if key equals expected; exit 3 if not
  cas -absent <key> <value>       set only if key does not exist
  status                          every node's Raft status
```

`get -stale` reads from whichever node answers instead of routing to the
leader: faster, and explicitly **not** linearizable. Having to pass a flag to
get the weaker guarantee is the right default — stale reads should be a
decision, not an accident.

`cas -absent` is insert-if-not-exists, which together with `cas` is what makes
this usable as a distributed lock or leader-election primitive.

Distinct exit codes mean `raftkvctl` composes in shell scripts without parsing
output: a missing key and a failed swap are different conditions and should not
both be exit 1.

## Configuration

Configured entirely by environment variable, which is what makes the container
and the local-process scripts identical.

| Variable | Default | Meaning |
|---|---|---|
| `RAFTKV_ID` | *(required)* | this node's id; must appear in `RAFTKV_PEERS` |
| `RAFTKV_PEERS` | *(required)* | `n1=http://host:port,n2=...` |
| `RAFTKV_LISTEN` | `:7000` | listen address |
| `RAFTKV_DATA_DIR` | `data` | WAL and snapshot directory |
| `RAFTKV_ELECTION_TIMEOUT` | `300ms` | randomised internally to avoid split votes |
| `RAFTKV_HEARTBEAT_INTERVAL` | `50ms` | must be shorter than the election timeout |
| `RAFTKV_RPC_TIMEOUT` | `200ms` | per-peer RPC deadline |
| `RAFTKV_REQUEST_TIMEOUT` | `5s` | client request deadline |
| `RAFTKV_SNAPSHOT_THRESHOLD` | `10000` | entries before a snapshot is taken |
| `RAFTKV_FSYNC` | `true` | fsync before acknowledging a write |
| `RAFTKV_LOG_LEVEL` | `info` | debug, info, warn, error |
| `RAFTKV_LOG_FORMAT` | `json` | json or text |

Validation is strict and happens at startup: a missing `RAFTKV_ID`, a peer set
that omits this node, or a heartbeat interval longer than the election timeout
all refuse to start. That last one would otherwise produce a cluster that
elects leaders forever and never makes progress — a miserable thing to debug at
runtime, and trivial to reject at boot.

## Benchmarks

```bash
make bench
# or
go run ./cmd/raftkv-bench -workload write,read,mixed \
    -storage memory,wal,wal-fsync -clients 16 -duration 5s -markdown
```

Measures an in-process cluster (or an external one with `-endpoints`) across
workloads, request paths and storage modes.

### Measured, on one machine

Treat these as *shape*, not as a score — they are from an M-series laptop with
`GOMAXPROCS=8` and a 3-node in-process cluster, so they measure the algorithm
and the storage path, not a production deployment. Run it on your own hardware.

<!-- darwin/arm64, GOMAXPROCS=8, go1.27.3, 3 nodes, 16 clients, 3s per run -->

| workload | storage | ops/s | p50 | p99 |
|---|---|---:|---:|---:|
| write | memory | 295,709 | 0.04 ms | 0.24 ms |
| write | wal | 102,460 | 0.13 ms | 0.53 ms |
| write | **wal-fsync** | **117** | **132.57 ms** | **201.81 ms** |
| read | memory | 615,904 | 0.02 ms | 0.11 ms |
| read | wal | 600,584 | 0.02 ms | 0.12 ms |
| read | wal-fsync | 656,943 | 0.02 ms | 0.11 ms |
| mixed | memory | 406,161 | 0.03 ms | 0.16 ms |
| mixed | wal | 153,990 | 0.09 ms | 0.37 ms |
| mixed | wal-fsync | 230 | 69.23 ms | 125.49 ms |

**The one row that matters is `write / wal-fsync`.** Durability costs a factor
of roughly **875x** on write throughput here — 102,460 ops/s becomes 117. That
is not a bug; it is the physics of waiting for a storage device to confirm a
flush, three times, before acknowledging a write.

This is the trade-off every database makes and usually hides. Postgres
`synchronous_commit`, MySQL `innodb_flush_log_at_trx_commit`, Kafka `acks=all`,
etcd's `--unsafe-no-fsync` — all the same dial. Having the number in front of
you changes the conversation from "should we enable fsync" to "is 117 writes
per second enough, and if not, do we batch, use faster storage, or accept
bounded data loss".

Reads are flat across all three storage modes, which is the expected result and
a useful sanity check: reads never touch the log, so the storage path should not
appear in their latency at all. If it did, something would be wrong.

## Development

```bash
make test      # unit + cluster + linearizability, race detector on
make lint      # gofmt + vet
make bench
```

Everything is pure Go, so `CGO_ENABLED=0` is set in the Makefile and CI.

## Limitations, stated plainly

- **Dynamic membership is not implemented.** The peer set is fixed at startup.
  Joint-consensus reconfiguration is the natural next step.
- **No TLS or authentication** between peers or from clients. Run it on a
  trusted network, or front it with a gateway.
- **Snapshots are full, not incremental**, so a large state machine makes
  snapshotting expensive.
- **Reads go to the leader.** No lease-based or follower reads, so read
  throughput does not scale with cluster size.

## License

MIT — see [LICENSE](LICENSE).
