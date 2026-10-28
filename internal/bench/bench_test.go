package bench

import (
	"context"
	"testing"
	"time"
)

func TestRunSmoke(t *testing.T) {
	for _, o := range []Options{
		{Workload: Mixed, Path: Direct, Storage: Memory},
		{Workload: Write, Path: HTTP, Storage: WAL},
	} {
		o.Clients, o.Duration, o.Keys = 4, 300*time.Millisecond, 20
		rep, err := Run(context.Background(), o)
		if err != nil {
			t.Fatalf("%+v: %v", o, err)
		}
		if rep.Ops == 0 || rep.Throughput <= 0 || rep.P50 > rep.P99 || rep.P99 > rep.Max {
			t.Fatalf("implausible report: %s", rep)
		}
	}
}

func TestValidate(t *testing.T) {
	for _, o := range []Options{
		{Workload: "scan"},
		{Path: "grpc"},
		{Storage: "rocksdb"},
		{Clients: -1},
	} {
		o.Defaults()
		if err := o.validate(); err == nil {
			t.Errorf("%+v: expected error", o)
		}
	}
}

func benchmarkRaft(b *testing.B, w Workload) {
	o := Options{Workload: w, Path: Direct, Storage: Memory, Clients: 8, Duration: time.Second, Keys: 1000}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rep, err := Run(context.Background(), o)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(rep.Throughput, "ops/s")
		b.ReportMetric(float64(rep.P50.Microseconds()), "p50-µs")
		b.ReportMetric(float64(rep.P99.Microseconds()), "p99-µs")
	}
}

func BenchmarkClusterWrite(b *testing.B) { benchmarkRaft(b, Write) }
func BenchmarkClusterRead(b *testing.B)  { benchmarkRaft(b, Read) }
