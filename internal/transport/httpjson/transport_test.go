package httpjson

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/raft"
)

type echoHandler struct {
	err  error
	last *raft.AppendEntriesRequest
}

func (h *echoHandler) HandleRequestVote(req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	if h.err != nil {
		return nil, h.err
	}
	return &raft.RequestVoteResponse{Term: req.Term, VoteGranted: req.CandidateID == "n1"}, nil
}

func (h *echoHandler) HandleAppendEntries(req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	if h.err != nil {
		return nil, h.err
	}
	h.last = req
	return &raft.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (h *echoHandler) HandleInstallSnapshot(req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	if h.err != nil {
		return nil, h.err
	}
	return &raft.InstallSnapshotResponse{Term: req.Term + uint64(len(req.Snapshot.Data))}, nil
}

func TestRoundTrip(t *testing.T) {
	h := &echoHandler{}
	srv := httptest.NewServer(Handler(h))
	defer srv.Close()
	c := NewClient(map[raft.NodeID]string{"n2": srv.URL + "/"})
	ctx := context.Background()

	vr, err := c.RequestVote(ctx, "n2", &raft.RequestVoteRequest{Term: 4, CandidateID: "n1"})
	if err != nil || !vr.VoteGranted || vr.Term != 4 {
		t.Fatalf("vote = %+v %v", vr, err)
	}

	ae := &raft.AppendEntriesRequest{
		Term: 5, LeaderID: "n1", PrevLogIndex: 9, PrevLogTerm: 4, LeaderCommit: 8,
		Entries: []raft.Entry{{Index: 10, Term: 5, Type: raft.EntryCommand, Data: []byte{0, 1, 255}}},
	}
	ar, err := c.AppendEntries(ctx, "n2", ae)
	if err != nil || !ar.Success {
		t.Fatalf("append = %+v %v", ar, err)
	}
	got := h.last.Entries[0]
	if got.Index != 10 || string(got.Data) != string([]byte{0, 1, 255}) {
		t.Fatalf("entry did not survive the wire: %+v", got)
	}

	sr, err := c.InstallSnapshot(ctx, "n2", &raft.InstallSnapshotRequest{Term: 1, Snapshot: raft.Snapshot{Data: []byte("abc")}})
	if err != nil || sr.Term != 4 {
		t.Fatalf("snapshot = %+v %v", sr, err)
	}
}

func TestErrors(t *testing.T) {
	h := &echoHandler{err: raft.ErrStopped}
	srv := httptest.NewServer(Handler(h))
	defer srv.Close()
	c := NewClient(map[raft.NodeID]string{"n2": srv.URL})
	ctx := context.Background()

	if _, err := c.RequestVote(ctx, "n2", &raft.RequestVoteRequest{}); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("stopped peer err = %v", err)
	}
	if _, err := c.RequestVote(ctx, "n9", &raft.RequestVoteRequest{}); err == nil {
		t.Fatal("unknown peer must fail")
	}

	h.err = errors.New("boom")
	if _, err := c.AppendEntries(ctx, "n2", &raft.AppendEntriesRequest{}); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("handler error = %v", err)
	}

	srv.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := c.AppendEntries(ctx, "n2", &raft.AppendEntriesRequest{}); err == nil {
		t.Fatal("closed server must fail")
	}
}
