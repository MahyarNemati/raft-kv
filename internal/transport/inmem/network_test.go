package inmem

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/raft"
)

type countingHandler struct{ calls atomic.Int64 }

func (h *countingHandler) HandleRequestVote(req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	h.calls.Add(1)
	return &raft.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (h *countingHandler) HandleAppendEntries(req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	h.calls.Add(1)
	// Mutating the request must not affect the sender's copy.
	if len(req.Entries) > 0 {
		req.Entries[0].Data[0] = 'X'
	}
	return &raft.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (h *countingHandler) HandleInstallSnapshot(req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	h.calls.Add(1)
	return &raft.InstallSnapshotResponse{Term: req.Term}, nil
}

func setup() (*Network, map[raft.NodeID]*countingHandler) {
	n := NewNetwork(1)
	hs := map[raft.NodeID]*countingHandler{}
	for _, id := range []raft.NodeID{"a", "b", "c"} {
		hs[id] = &countingHandler{}
		n.Register(id, hs[id])
	}
	return n, hs
}

func vote(n *Network, from, to raft.NodeID, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := n.Transport(from).RequestVote(ctx, to, &raft.RequestVoteRequest{Term: 1})
	return err
}

func TestDeliveryAndIsolation(t *testing.T) {
	n, _ := setup()
	if err := vote(n, "a", "b", time.Second); err != nil {
		t.Fatal(err)
	}

	data := []byte("hello")
	req := &raft.AppendEntriesRequest{Entries: []raft.Entry{{Index: 1, Data: data}}}
	if _, err := n.Transport("a").AppendEntries(context.Background(), "b", req); err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatal("receiver mutated the sender's buffer")
	}
}

func TestPartitionAndHeal(t *testing.T) {
	n, _ := setup()
	n.Partition([]raft.NodeID{"a"}, []raft.NodeID{"b", "c"})
	if err := vote(n, "a", "b", time.Second); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("across partition: %v", err)
	}
	if err := vote(n, "b", "c", time.Second); err != nil {
		t.Fatalf("within partition: %v", err)
	}
	n.Heal()
	if err := vote(n, "a", "b", time.Second); err != nil {
		t.Fatalf("after heal: %v", err)
	}
}

func TestDisconnectedSenderCannotSend(t *testing.T) {
	n, hs := setup()
	n.Disconnect("a")
	if err := vote(n, "a", "b", time.Second); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("crashed sender: %v", err)
	}
	if err := vote(n, "b", "a", time.Second); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("crashed receiver: %v", err)
	}
	if hs["b"].calls.Load() != 0 {
		t.Fatal("message from crashed node was delivered")
	}
}

func TestDropsLookLikeTimeouts(t *testing.T) {
	n, _ := setup()
	n.SetFaults(Faults{DropRate: 1})
	if err := vote(n, "a", "b", 50*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dropped message err = %v", err)
	}
}

func TestDelays(t *testing.T) {
	n, _ := setup()
	n.SetFaults(Faults{MinDelay: 20 * time.Millisecond, MaxDelay: 30 * time.Millisecond})
	start := time.Now()
	if err := vote(n, "a", "b", time.Second); err != nil {
		t.Fatal(err)
	}
	// Request and response are each delayed.
	if d := time.Since(start); d < 40*time.Millisecond {
		t.Fatalf("round trip took %s, want >= 40ms", d)
	}
	if err := vote(n, "a", "b", 10*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delay beyond deadline: %v", err)
	}
}
