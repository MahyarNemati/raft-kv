package storage

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/MahyarNemati/raft-kv/internal/raft"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func openLoaded(t *testing.T, dir string) (*WAL, raft.PersistentState) {
	t.Helper()
	w, err := OpenWAL(dir, WALOptions{Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	st, err := w.Load()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, st
}

func entries(start uint64, terms ...uint64) []raft.Entry {
	out := make([]raft.Entry, len(terms))
	for i, tm := range terms {
		idx := start + uint64(i)
		out[i] = raft.Entry{Index: idx, Term: tm, Type: raft.EntryCommand, Data: []byte{byte(idx)}}
	}
	return out
}

func assertIndexes(t *testing.T, got []raft.Entry, first, last uint64) {
	t.Helper()
	if want := int(last - first + 1); len(got) != want {
		t.Fatalf("got %d entries, want %d (%d..%d)", len(got), want, first, last)
	}
	for i, e := range got {
		if e.Index != first+uint64(i) {
			t.Fatalf("entry %d has index %d, want %d", i, e.Index, first+uint64(i))
		}
	}
}

func TestWALRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, st := openLoaded(t, dir)
	if st.Snapshot != nil || len(st.Entries) != 0 || st.Hard.Term != 0 {
		t.Fatalf("fresh wal not empty: %+v", st)
	}
	must(t, w.SetHardState(raft.HardState{Term: 3, VotedFor: "n2"}))
	must(t, w.Append(entries(1, 1, 1, 2)))
	must(t, w.Append(entries(4, 3, 3)))
	must(t, w.Close())

	_, st = openLoaded(t, dir)
	if st.Hard.Term != 3 || st.Hard.VotedFor != "n2" {
		t.Fatalf("hard state = %+v", st.Hard)
	}
	assertIndexes(t, st.Entries, 1, 5)
	if st.Entries[4].Term != 3 || st.Entries[4].Data[0] != 5 {
		t.Fatalf("last entry = %+v", st.Entries[4])
	}
}

func TestWALSuffixReplacement(t *testing.T) {
	dir := t.TempDir()
	w, _ := openLoaded(t, dir)
	must(t, w.Append(entries(1, 1, 1, 1, 1)))
	// A new leader overwrites index 3 onwards.
	must(t, w.Append(entries(3, 2)))
	must(t, w.Close())

	_, st := openLoaded(t, dir)
	assertIndexes(t, st.Entries, 1, 3)
	if st.Entries[2].Term != 2 {
		t.Fatalf("index 3 term = %d, want 2", st.Entries[2].Term)
	}
}

func TestWALRejectsGap(t *testing.T) {
	w, _ := openLoaded(t, t.TempDir())
	must(t, w.Append(entries(1, 1)))
	if err := w.Append(entries(5, 1)); err == nil {
		t.Fatal("expected gap error")
	}
}

func TestWALTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	w, _ := openLoaded(t, dir)
	must(t, w.Append(entries(1, 1, 1, 1)))
	must(t, w.Close())

	path := filepath.Join(dir, walFile)
	info, err := os.Stat(path)
	must(t, err)
	// Chop the last record in half, as a crash mid-write would.
	must(t, os.Truncate(path, info.Size()-5))

	w, st := openLoaded(t, dir)
	assertIndexes(t, st.Entries, 1, 2)
	// The store must be appendable right after the repaired tail.
	must(t, w.Append(entries(3, 2)))
	must(t, w.Close())
	_, st = openLoaded(t, dir)
	assertIndexes(t, st.Entries, 1, 3)
}

func TestWALCorruptRecordStopsReplay(t *testing.T) {
	dir := t.TempDir()
	w, _ := openLoaded(t, dir)
	must(t, w.Append(entries(1, 1, 1, 1)))
	must(t, w.Close())

	path := filepath.Join(dir, walFile)
	b, err := os.ReadFile(path)
	must(t, err)
	b[len(b)-1] ^= 0xff
	must(t, os.WriteFile(path, b, 0o640))

	_, st := openLoaded(t, dir)
	assertIndexes(t, st.Entries, 1, 2)
}

func TestWALSnapshotCompacts(t *testing.T) {
	dir := t.TempDir()
	w, _ := openLoaded(t, dir)
	must(t, w.SetHardState(raft.HardState{Term: 2, VotedFor: "n1"}))
	must(t, w.Append(entries(1, 1, 1, 2, 2, 2)))

	snap := raft.Snapshot{Meta: raft.SnapshotMeta{Index: 3, Term: 2}, Data: []byte("state@3")}
	must(t, w.SaveSnapshot(snap, entries(4, 2, 2)))
	must(t, w.Append(entries(6, 2)))
	must(t, w.Close())

	_, st := openLoaded(t, dir)
	if st.Snapshot == nil || st.Snapshot.Meta != snap.Meta || string(st.Snapshot.Data) != "state@3" {
		t.Fatalf("snapshot = %+v", st.Snapshot)
	}
	if st.Hard.Term != 2 || st.Hard.VotedFor != "n1" {
		t.Fatalf("hard state lost across compaction: %+v", st.Hard)
	}
	assertIndexes(t, st.Entries, 4, 6)
}

func TestWALCrashBetweenSnapshotAndRewrite(t *testing.T) {
	dir := t.TempDir()
	w, _ := openLoaded(t, dir)
	must(t, w.Append(entries(1, 1, 1, 1, 1, 1)))
	must(t, w.Close())

	// Simulate a crash after the snapshot rename but before the WAL rewrite.
	snap := raft.Snapshot{Meta: raft.SnapshotMeta{Index: 3, Term: 1}, Data: []byte("s")}
	must(t, writeFileAtomic(dir, snapshotFile, encodeSnapshot(snap), false))

	_, st := openLoaded(t, dir)
	if st.Snapshot == nil || st.Snapshot.Meta.Index != 3 {
		t.Fatalf("snapshot = %+v", st.Snapshot)
	}
	assertIndexes(t, st.Entries, 4, 5)
}

func TestWALCorruptSnapshotIsAnError(t *testing.T) {
	dir := t.TempDir()
	must(t, os.WriteFile(filepath.Join(dir, snapshotFile), []byte("garbage-garbage-garbage-garbage"), 0o640))
	w, err := OpenWAL(dir, WALOptions{Logger: quiet()})
	must(t, err)
	if _, err := w.Load(); err == nil {
		t.Fatal("expected error for corrupt snapshot")
	}
}

func TestTrimToSnapshot(t *testing.T) {
	snap := &raft.Snapshot{Meta: raft.SnapshotMeta{Index: 3, Term: 2}}
	cases := []struct {
		name        string
		in          []raft.Entry
		first, last uint64
		empty       bool
	}{
		{"overlapping", entries(1, 1, 1, 2, 2), 4, 4, false},
		{"after snapshot", entries(4, 2, 2), 4, 5, false},
		{"term mismatch", entries(1, 1, 1, 1, 1), 0, 0, true},
		{"all covered", entries(1, 1, 1), 0, 0, true},
		{"gap after snapshot", entries(6, 2), 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := trimToSnapshot(tc.in, snap)
			if tc.empty {
				if len(got) != 0 {
					t.Fatalf("want empty, got %d entries", len(got))
				}
				return
			}
			assertIndexes(t, got, tc.first, tc.last)
		})
	}
}

func TestMemoryStore(t *testing.T) {
	m := NewMemory()
	must(t, m.SetHardState(raft.HardState{Term: 1}))
	must(t, m.Append(entries(1, 1, 1, 1)))
	must(t, m.Append(entries(2, 2)))
	st, err := m.Load()
	must(t, err)
	assertIndexes(t, st.Entries, 1, 2)

	// Loaded state must not alias the store.
	st.Entries[0].Data[0] = 42
	st2, _ := m.Load()
	if st2.Entries[0].Data[0] == 42 {
		t.Fatal("Load returned aliased data")
	}

	must(t, m.SaveSnapshot(raft.Snapshot{Meta: raft.SnapshotMeta{Index: 2, Term: 2}}, nil))
	if err := m.Append(entries(2, 3)); err == nil {
		t.Fatal("append below snapshot must fail")
	}
	if err := m.Append(entries(4, 3)); err == nil {
		t.Fatal("append with gap must fail")
	}
	must(t, m.Append(entries(3, 3)))
	st, _ = m.Load()
	assertIndexes(t, st.Entries, 3, 3)
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
