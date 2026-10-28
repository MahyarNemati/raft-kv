package client_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/client"
	"github.com/MahyarNemati/raft-kv/internal/cluster"
	"github.com/MahyarNemati/raft-kv/internal/kv"
)

func startCluster(t *testing.T, size int) *cluster.Cluster {
	t.Helper()
	c, err := cluster.Start(cluster.Options{Size: size, Seed: 1, HTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.WaitLeader(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientEndToEnd(t *testing.T) {
	c := startCluster(t, 3)
	cl, err := client.New(c.Endpoints())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, found, err := cl.Get(ctx, "users/1", kv.Linearizable); err != nil || found {
		t.Fatalf("get missing: %v %v", found, err)
	}
	if _, err := cl.Put(ctx, "users/1", "ada"); err != nil {
		t.Fatal(err)
	}
	if v, found, err := cl.Get(ctx, "users/1", kv.Linearizable); err != nil || !found || v != "ada" {
		t.Fatalf("get = %q %v %v", v, found, err)
	}
	old := "ada"
	if r, err := cl.CAS(ctx, "users/1", &old, "grace"); err != nil || !r.OK {
		t.Fatalf("cas = %+v %v", r, err)
	}
	if r, err := cl.CAS(ctx, "users/1", &old, "nope"); err != nil || r.OK || r.Prev != "grace" {
		t.Fatalf("stale cas = %+v %v", r, err)
	}
	if r, err := cl.CAS(ctx, "users/2", nil, "new"); err != nil || !r.OK {
		t.Fatalf("create-if-absent = %+v %v", r, err)
	}
	if r, err := cl.Delete(ctx, "users/1"); err != nil || !r.OK {
		t.Fatalf("delete = %+v %v", r, err)
	}
	if _, err := cl.Put(ctx, "", "x"); !errors.Is(err, kv.ErrInvalid) {
		t.Fatalf("empty key err = %v", err)
	}

	// A key with characters that need escaping round-trips intact.
	weird := "a b/c?d#e%f"
	if _, err := cl.Put(ctx, weird, "ok"); err != nil {
		t.Fatal(err)
	}
	if v, _, err := cl.Get(ctx, weird, kv.Stale); err != nil {
		t.Fatal(err)
	} else if v != "ok" {
		// Stale reads may lag; a linearizable read must see it.
		if v, _, _ = cl.Get(ctx, weird, kv.Linearizable); v != "ok" {
			t.Fatalf("weird key = %q", v)
		}
	}
}

func TestClientFollowsLeaderAndSurvivesFailover(t *testing.T) {
	c := startCluster(t, 3)
	leader, _ := c.Leader()

	// Point the client at followers first so the first write must be redirected.
	var eps []string
	for i, id := range c.IDs() {
		if id != leader {
			eps = append(eps, c.Endpoints()[i])
		}
	}
	eps = append(eps, c.Endpoints()...)
	cl, err := client.New(eps, client.WithBackoff(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if _, err := cl.Put(ctx, "k", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	c.Crash(leader)
	if _, err := cl.Put(ctx, "k", "after-failover"); err != nil {
		t.Fatalf("write after failover: %v", err)
	}
	if v, _, err := cl.Get(ctx, "k", kv.Linearizable); err != nil || v != "after-failover" {
		t.Fatalf("read after failover = %q %v", v, err)
	}
}

func TestClientDeadlineWithoutQuorum(t *testing.T) {
	c := startCluster(t, 3)
	cl, _ := client.New(c.Endpoints(), client.WithBackoff(10*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := cl.Put(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	cancel()

	ids := c.IDs()
	c.Crash(ids[0])
	c.Crash(ids[1])
	ctx, cancel = context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	_, err := cl.Put(ctx, "k", "w")
	if err == nil {
		t.Fatal("write succeeded without a quorum")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestNewValidatesEndpoints(t *testing.T) {
	if _, err := client.New(nil); err == nil {
		t.Fatal("expected error for no endpoints")
	}
	if _, err := client.New([]string{"::"}); err == nil {
		t.Fatal("expected error for bad endpoint")
	}
	cl, err := client.New([]string{"http://a:1/"}, client.WithClientID("fixed"))
	if err != nil || cl.ID() != "fixed" || cl.Endpoints()[0] != "http://a:1" {
		t.Fatalf("client = %v %v", cl, err)
	}
}
