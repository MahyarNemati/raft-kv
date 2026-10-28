package kv

import (
	"context"
	"errors"
	"testing"
)

func apply(t *testing.T, f *FSM, idx uint64, c Command) Result {
	t.Helper()
	b, err := c.encode()
	if err != nil {
		t.Fatal(err)
	}
	res, ok := f.Apply(idx, b).(Result)
	if !ok {
		t.Fatal("Apply did not return a Result")
	}
	return res
}

func TestFSMOperations(t *testing.T) {
	f := NewFSM()
	if r := apply(t, f, 1, Command{Op: OpPut, Key: "a", Value: "1"}); !r.OK || r.Existed {
		t.Fatalf("put = %+v", r)
	}
	if r := apply(t, f, 2, Command{Op: OpPut, Key: "a", Value: "2"}); !r.OK || !r.Existed || r.Prev != "1" {
		t.Fatalf("overwrite = %+v", r)
	}
	if r := apply(t, f, 3, Command{Op: OpCAS, Key: "a", Expected: "1", Value: "x"}); r.OK || r.Prev != "2" {
		t.Fatalf("failed cas = %+v", r)
	}
	if r := apply(t, f, 4, Command{Op: OpCAS, Key: "a", Expected: "2", Value: "3"}); !r.OK {
		t.Fatalf("cas = %+v", r)
	}
	if v, _ := f.Get("a"); v != "3" {
		t.Fatalf("a = %q", v)
	}
	if r := apply(t, f, 5, Command{Op: OpCAS, Key: "b", ExpectAbsent: true, Value: "new"}); !r.OK {
		t.Fatalf("cas absent = %+v", r)
	}
	if r := apply(t, f, 6, Command{Op: OpCAS, Key: "b", ExpectAbsent: true, Value: "again"}); r.OK {
		t.Fatalf("cas absent on existing key = %+v", r)
	}
	if r := apply(t, f, 7, Command{Op: OpDelete, Key: "b"}); !r.OK || r.Prev != "new" {
		t.Fatalf("delete = %+v", r)
	}
	if r := apply(t, f, 8, Command{Op: OpDelete, Key: "b"}); r.OK {
		t.Fatalf("delete missing = %+v", r)
	}
	if _, ok := f.Get("b"); ok {
		t.Fatal("b should be gone")
	}
	// Empty-string values are real values, distinct from absence.
	apply(t, f, 9, Command{Op: OpPut, Key: "e", Value: ""})
	if v, ok := f.Get("e"); !ok || v != "" {
		t.Fatalf("empty value: %q %v", v, ok)
	}
}

func TestFSMDeduplicatesRetries(t *testing.T) {
	f := NewFSM()
	first := apply(t, f, 1, Command{Op: OpCAS, Key: "k", ExpectAbsent: true, Value: "v", ClientID: "c1", Seq: 1})
	if !first.OK {
		t.Fatalf("first = %+v", first)
	}
	// The retry lands at a later index but must return the original outcome
	// rather than re-executing (which would now fail the CAS).
	retry := apply(t, f, 2, Command{Op: OpCAS, Key: "k", ExpectAbsent: true, Value: "v", ClientID: "c1", Seq: 1})
	if retry != first {
		t.Fatalf("retry = %+v, want %+v", retry, first)
	}
	apply(t, f, 3, Command{Op: OpPut, Key: "k", Value: "w", ClientID: "c1", Seq: 2})
	if r := apply(t, f, 4, Command{Op: OpPut, Key: "k", Value: "old", ClientID: "c1", Seq: 1}); r.Err == "" {
		t.Fatalf("stale seq accepted: %+v", r)
	}
	if v, _ := f.Get("k"); v != "w" {
		t.Fatalf("k = %q, want w", v)
	}
	// Another client is independent.
	if r := apply(t, f, 5, Command{Op: OpPut, Key: "k", Value: "z", ClientID: "c2", Seq: 1}); !r.OK {
		t.Fatalf("c2 = %+v", r)
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	f := NewFSM()
	apply(t, f, 1, Command{Op: OpPut, Key: "a", Value: "1", ClientID: "c", Seq: 7})
	apply(t, f, 2, Command{Op: OpPut, Key: "b", Value: "2"})
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := NewFSM()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if g.Len() != 2 {
		t.Fatalf("len = %d", g.Len())
	}
	// Sessions survive the snapshot, so a retry after restore is still deduped.
	r := apply(t, g, 3, Command{Op: OpPut, Key: "a", Value: "1", ClientID: "c", Seq: 7})
	if r.Index != 1 {
		t.Fatalf("retry after restore re-executed: %+v", r)
	}
	if err := g.Restore([]byte("{")); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestFSMGarbageEntry(t *testing.T) {
	f := NewFSM()
	r, _ := f.Apply(1, []byte("not json")).(Result)
	if r.Err == "" {
		t.Fatal("garbage must produce an error result")
	}
}

func TestCommandValidate(t *testing.T) {
	long := make([]byte, MaxKeyLen+1)
	for i := range long {
		long[i] = 'k'
	}
	bad := []Command{
		{Op: 0, Key: "a"},
		{Op: OpPut},
		{Op: OpPut, Key: string(long)},
		{Op: OpPut, Key: "a", ClientID: "c"},
		{Op: OpCAS, Key: "a", Expected: "x", ExpectAbsent: true},
	}
	for i, c := range bad {
		if err := c.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
	ok := Command{Op: OpCAS, Key: "a", Expected: "x", Value: "y", ClientID: "c", Seq: 1}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
}

type fakeConsensus struct {
	fsm      *FSM
	idx      uint64
	readErr  error
	reads    int
	proposed int
}

func (f *fakeConsensus) Propose(_ context.Context, b []byte) (any, error) {
	f.idx++
	f.proposed++
	return f.fsm.Apply(f.idx, b), nil
}

func (f *fakeConsensus) ReadIndex(context.Context) (uint64, error) {
	f.reads++
	return f.idx, f.readErr
}

func TestStore(t *testing.T) {
	fsm := NewFSM()
	fc := &fakeConsensus{fsm: fsm}
	s := NewStore(fc, fsm)
	ctx := context.Background()

	if _, err := s.Execute(ctx, Command{Op: OpPut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Execute(ctx, Command{Op: OpPut}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid command reached consensus: %v", err)
	}
	if fc.proposed != 1 {
		t.Fatalf("proposed = %d", fc.proposed)
	}

	v, ok, err := s.Get(ctx, "a", Linearizable)
	if err != nil || !ok || v != "1" || fc.reads != 1 {
		t.Fatalf("get = %q %v %v reads=%d", v, ok, err, fc.reads)
	}
	if _, _, err := s.Get(ctx, "a", Stale); err != nil || fc.reads != 1 {
		t.Fatal("stale read must not touch consensus")
	}

	fc.readErr = errors.New("not leader")
	if _, _, err := s.Get(ctx, "a", Linearizable); err == nil {
		t.Fatal("expected read index error")
	}

	_, _ = s.Execute(ctx, Command{Op: OpPut, Key: "a", Value: "2", ClientID: "c", Seq: 5})
	if _, err := s.Execute(ctx, Command{Op: OpPut, Key: "a", Value: "3", ClientID: "c", Seq: 4}); !errors.Is(err, ErrRejected) {
		t.Fatalf("stale seq err = %v", err)
	}
}

func TestParseConsistency(t *testing.T) {
	for in, want := range map[string]Consistency{"": Linearizable, "linearizable": Linearizable, "stale": Stale} {
		got, err := ParseConsistency(in)
		if err != nil || got != want {
			t.Errorf("%q -> %v %v", in, got, err)
		}
	}
	if _, err := ParseConsistency("eventual"); err == nil {
		t.Fatal("expected error")
	}
}
