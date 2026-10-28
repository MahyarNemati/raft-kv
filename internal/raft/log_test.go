package raft

import "testing"

func mkEntries(start uint64, terms ...uint64) []Entry {
	es := make([]Entry, len(terms))
	for i, t := range terms {
		es[i] = Entry{Index: start + uint64(i), Term: t}
	}
	return es
}

func TestRaftLogBasics(t *testing.T) {
	l, err := newRaftLog(0, 0, mkEntries(1, 1, 1, 2, 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	if l.lastIndex() != 5 || l.lastTerm() != 3 {
		t.Fatalf("last = %d/%d, want 5/3", l.lastIndex(), l.lastTerm())
	}
	if tm, ok := l.term(3); !ok || tm != 2 {
		t.Fatalf("term(3) = %d,%v", tm, ok)
	}
	if _, ok := l.term(6); ok {
		t.Fatal("term(6) should be out of range")
	}
	if got := l.firstIndexOfTerm(2, 4); got != 3 {
		t.Fatalf("firstIndexOfTerm(2) = %d, want 3", got)
	}
	if got := l.lastIndexOfTerm(2); got != 4 {
		t.Fatalf("lastIndexOfTerm(2) = %d, want 4", got)
	}
	if got := l.lastIndexOfTerm(4); got != 0 {
		t.Fatalf("lastIndexOfTerm(4) = %d, want 0", got)
	}
	s := l.slice(2, 4)
	if len(s) != 2 || s[0].Index != 2 || s[1].Index != 3 {
		t.Fatalf("slice = %+v", s)
	}
	s[0].Term = 99
	if tm, _ := l.term(2); tm != 1 {
		t.Fatal("slice must copy")
	}

	l.truncateFrom(4)
	if l.lastIndex() != 3 {
		t.Fatalf("after truncate last = %d", l.lastIndex())
	}
}

func TestRaftLogCompaction(t *testing.T) {
	l, _ := newRaftLog(0, 0, mkEntries(1, 1, 1, 2, 2, 3))

	l.compactTo(3, 2)
	if l.snapIndex() != 3 || l.snapTerm() != 2 || l.lastIndex() != 5 {
		t.Fatalf("snap=%d/%d last=%d", l.snapIndex(), l.snapTerm(), l.lastIndex())
	}
	if _, ok := l.term(2); ok {
		t.Fatal("compacted index must be unavailable")
	}
	if tm, ok := l.term(3); !ok || tm != 2 {
		t.Fatal("snapshot index must report its term")
	}

	// Mismatched term at the snapshot index discards everything.
	l.compactTo(4, 7)
	if l.snapIndex() != 4 || l.lastIndex() != 4 {
		t.Fatalf("snap=%d last=%d, want 4/4", l.snapIndex(), l.lastIndex())
	}

	// Beyond the end of the log.
	l.compactTo(10, 8)
	if l.snapIndex() != 10 || l.lastIndex() != 10 || l.lastTerm() != 8 {
		t.Fatalf("snap=%d last=%d/%d", l.snapIndex(), l.lastIndex(), l.lastTerm())
	}
}

func TestNewRaftLogRejectsGap(t *testing.T) {
	if _, err := newRaftLog(0, 0, []Entry{{Index: 1, Term: 1}, {Index: 3, Term: 1}}); err == nil {
		t.Fatal("expected gap error")
	}
}

func TestConfigValidate(t *testing.T) {
	c := DefaultConfig("a", []NodeID{"a", "b", "c"})
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	bad := []Config{
		DefaultConfig("", []NodeID{"a"}),
		DefaultConfig("a", []NodeID{"b"}),
		DefaultConfig("a", []NodeID{"a", "a"}),
	}
	c2 := DefaultConfig("a", []NodeID{"a"})
	c2.HeartbeatInterval = c2.ElectionTimeout
	bad = append(bad, c2)
	for i, c := range bad {
		if err := c.validate(); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}
