package raft

import "fmt"

// raftLog is the in-memory view of the log. entries[0] is a sentinel holding
// the index and term of the last compacted entry (0/0 for an empty log), so
// entries[i] has Index == entries[0].Index + i.
type raftLog struct {
	entries []Entry
}

func newRaftLog(snapIndex, snapTerm uint64, entries []Entry) (*raftLog, error) {
	l := &raftLog{entries: make([]Entry, 1, len(entries)+1)}
	l.entries[0] = Entry{Index: snapIndex, Term: snapTerm}
	for _, e := range entries {
		if e.Index <= snapIndex {
			continue
		}
		if e.Index != l.lastIndex()+1 {
			return nil, fmt.Errorf("raft: log gap: have last index %d, got entry %d", l.lastIndex(), e.Index)
		}
		l.entries = append(l.entries, e)
	}
	return l, nil
}

func (l *raftLog) snapIndex() uint64 { return l.entries[0].Index }
func (l *raftLog) snapTerm() uint64  { return l.entries[0].Term }
func (l *raftLog) lastIndex() uint64 { return l.entries[len(l.entries)-1].Index }
func (l *raftLog) lastTerm() uint64  { return l.entries[len(l.entries)-1].Term }

// term returns the term of the entry at index i. ok is false if i is
// compacted away (below the snapshot) or beyond the end of the log.
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i < l.snapIndex() || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.snapIndex()].Term, true
}

func (l *raftLog) entry(i uint64) Entry {
	return l.entries[i-l.snapIndex()]
}

// slice returns a copy of entries in [lo, hi).
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo > hi {
		return nil
	}
	off := l.snapIndex()
	out := make([]Entry, hi-lo)
	copy(out, l.entries[lo-off:hi-off])
	return out
}

// firstIndexOfTerm returns the lowest index > snapIndex holding term t,
// scanning backwards from from.
func (l *raftLog) firstIndexOfTerm(t, from uint64) uint64 {
	i := from
	for i > l.snapIndex()+1 {
		if pt, _ := l.term(i - 1); pt != t {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm returns the highest index holding term t, or 0.
func (l *raftLog) lastIndexOfTerm(t uint64) uint64 {
	for i := len(l.entries) - 1; i > 0; i-- {
		switch et := l.entries[i].Term; {
		case et == t:
			return l.entries[i].Index
		case et < t:
			return 0
		}
	}
	return 0
}

func (l *raftLog) append(es ...Entry) {
	l.entries = append(l.entries, es...)
}

// truncateFrom drops every entry with index >= i.
func (l *raftLog) truncateFrom(i uint64) {
	l.entries = l.entries[:i-l.snapIndex()]
}

// compactTo discards entries up to and including index i, which becomes the
// new sentinel with the given term. If i is beyond the end of the log or the
// term at i disagrees, the whole log is discarded.
func (l *raftLog) compactTo(i, term uint64) {
	if t, ok := l.term(i); ok && t == term {
		rest := l.entries[i-l.snapIndex()+1:]
		next := make([]Entry, 1, len(rest)+1)
		next[0] = Entry{Index: i, Term: term}
		l.entries = append(next, rest...)
		return
	}
	l.entries = []Entry{{Index: i, Term: term}}
}
