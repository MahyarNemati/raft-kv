// Package storage provides raft.Storage implementations: an in-memory store
// for tests and benchmarks, and a write-ahead log with snapshot files for
// real deployments.
package storage

import (
	"fmt"
	"sync"

	"github.com/MahyarNemati/raft-kv/internal/raft"
)

// Memory keeps Raft state in memory. It survives a simulated crash as long as
// the same instance is handed to the restarted node, which is how the test
// harness models a durable disk.
type Memory struct {
	mu      sync.Mutex
	hard    raft.HardState
	snap    *raft.Snapshot
	entries []raft.Entry
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{} }

func (m *Memory) Load() (raft.PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := raft.PersistentState{Hard: m.hard, Entries: cloneEntries(m.entries)}
	if m.snap != nil {
		s := cloneSnapshot(*m.snap)
		st.Snapshot = &s
	}
	return st, nil
}

func (m *Memory) SetHardState(hs raft.HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hard = hs
	return nil
}

func (m *Memory) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	merged, err := spliceEntries(m.entries, m.snapIndex(), entries)
	if err != nil {
		return err
	}
	m.entries = merged
	return nil
}

func (m *Memory) SaveSnapshot(snap raft.Snapshot, entries []raft.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := cloneSnapshot(snap)
	m.snap = &s
	m.entries = cloneEntries(entries)
	return nil
}

func (m *Memory) Close() error { return nil }

func (m *Memory) snapIndex() uint64 {
	if m.snap == nil {
		return 0
	}
	return m.snap.Meta.Index
}

// spliceEntries appends entries to log, replacing any suffix that starts at or
// after the first new index. base is the index preceding log[0].
func spliceEntries(log []raft.Entry, base uint64, entries []raft.Entry) ([]raft.Entry, error) {
	first := entries[0].Index
	last := base + uint64(len(log))
	if first > last+1 {
		return nil, fmt.Errorf("storage: append at %d leaves gap after %d", first, last)
	}
	if first <= base {
		return nil, fmt.Errorf("storage: append at %d overlaps snapshot at %d", first, base)
	}
	log = log[:first-base-1]
	for _, e := range entries {
		e.Data = append([]byte(nil), e.Data...)
		log = append(log, e)
	}
	return log, nil
}

func cloneEntries(es []raft.Entry) []raft.Entry {
	out := make([]raft.Entry, len(es))
	for i, e := range es {
		e.Data = append([]byte(nil), e.Data...)
		out[i] = e
	}
	return out
}

func cloneSnapshot(s raft.Snapshot) raft.Snapshot {
	s.Data = append([]byte(nil), s.Data...)
	return s
}
