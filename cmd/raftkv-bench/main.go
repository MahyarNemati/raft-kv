// Command raftkv-bench measures throughput and latency of an in-process
// raftkv cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"

	"github.com/MahyarNemati/raft-kv/internal/bench"
)

func main() {
	var o bench.Options
	var workloads, paths, storages string
	flag.IntVar(&o.Nodes, "nodes", 3, "cluster size")
	flag.IntVar(&o.Clients, "clients", 16, "concurrent clients")
	flag.DurationVar(&o.Duration, "duration", 5e9, "measured duration per run")
	flag.DurationVar(&o.Warmup, "warmup", 1e9, "unmeasured warmup per run")
	flag.IntVar(&o.ValueSize, "value-size", 64, "value size in bytes")
	flag.IntVar(&o.Keys, "keys", 1000, "keyspace size")
	flag.StringVar(&workloads, "workload", "write", "comma-separated: write, read, mixed")
	flag.StringVar(&paths, "path", "direct", "comma-separated: direct, http")
	flag.StringVar(&storages, "storage", "memory", "comma-separated: memory, wal, wal-fsync")
	endpoints := flag.String("endpoints", "", "comma-separated URLs of an external cluster to load instead of an in-process one")
	markdown := flag.Bool("markdown", false, "print a markdown table")
	flag.Parse()

	o.Endpoints = split(*endpoints)
	if len(o.Endpoints) > 0 {
		paths, storages = "external", "external"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *markdown {
		fmt.Printf("<!-- %s/%s, GOMAXPROCS=%d, %s, loadavg at start: %s -->\n",
			runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), runtime.Version(), loadavg())
		fmt.Println("| workload | path | storage | clients | ops/s | p50 | p90 | p99 | max |")
		fmt.Println("|---|---|---|---:|---:|---:|---:|---:|---:|")
	}
	failed := false
	for _, w := range split(workloads) {
		for _, p := range split(paths) {
			for _, s := range split(storages) {
				run := o
				run.Workload, run.Path, run.Storage = bench.Workload(w), bench.Path(p), bench.Storage(s)
				rep, err := bench.Run(ctx, run)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s/%s/%s: %v\n", w, p, s, err)
					failed = true
					continue
				}
				if *markdown {
					fmt.Printf("| %s | %s | %s | %d | %.0f | %s | %s | %s | %s |\n",
						w, p, s, run.Clients, rep.Throughput, ms(rep.P50), ms(rep.P90), ms(rep.P99), ms(rep.Max))
				} else {
					fmt.Println(rep)
				}
			}
		}
	}
	if failed {
		os.Exit(1)
	}
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// loadavg returns the 1-minute load average on Linux, or "n/a".
func loadavg() string {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "n/a"
	}
	if f := strings.Fields(string(b)); len(f) > 0 {
		return f[0]
	}
	return "n/a"
}

func ms(d interface{ Seconds() float64 }) string {
	return fmt.Sprintf("%.2f ms", d.Seconds()*1000)
}
