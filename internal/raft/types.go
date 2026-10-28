// Package raft implements the Raft consensus algorithm: leader election,
// log replication, snapshot-based log compaction and ReadIndex reads.
package raft

import (
	"context"
	"errors"
	"fmt"
)

// NodeID identifies a member of the cluster.
type NodeID string

// Role is the current Raft role of a node.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", int(r))
	}
}

// EntryType distinguishes client commands from internal entries.
type EntryType uint8

const (
	// EntryCommand carries an opaque state machine command.
	EntryCommand EntryType = iota
	// EntryNoop is appended by a new leader to commit entries from prior terms.
	EntryNoop
)

// Entry is a single log entry.
type Entry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data,omitempty"`
}

// HardState is the state that must be durable before responding to RPCs.
type HardState struct {
	Term     uint64 `json:"term"`
	VotedFor NodeID `json:"voted_for"`
}

// SnapshotMeta describes the last log position covered by a snapshot.
type SnapshotMeta struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}

// Snapshot is a serialized state machine image plus its log position.
type Snapshot struct {
	Meta SnapshotMeta `json:"meta"`
	Data []byte       `json:"data"`
}

// PersistentState is everything a node recovers from stable storage.
type PersistentState struct {
	Hard     HardState
	Snapshot *Snapshot
	Entries  []Entry
}

// Storage is the durable backing store for Raft state. Implementations must
// make writes durable before returning.
type Storage interface {
	Load() (PersistentState, error)
	SetHardState(HardState) error
	// Append persists entries. The first entry's index may be at or below the
	// last stored index, in which case the stored suffix from that index on is
	// replaced.
	Append(entries []Entry) error
	// SaveSnapshot persists snap and replaces the stored log with entries,
	// which must all have indexes greater than snap.Meta.Index.
	SaveSnapshot(snap Snapshot, entries []Entry) error
	Close() error
}

// StateMachine is the replicated application. Apply is called sequentially
// in log order from a single goroutine.
type StateMachine interface {
	Apply(index uint64, data []byte) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// RequestVoteRequest is sent by candidates to gather votes.
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  NodeID `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse is the reply to RequestVoteRequest.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesRequest replicates entries and doubles as a heartbeat.
type AppendEntriesRequest struct {
	Term         uint64  `json:"term"`
	LeaderID     NodeID  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit"`
}

// AppendEntriesResponse is the reply to AppendEntriesRequest. On failure,
// ConflictTerm/ConflictIndex let the leader skip a whole term per round trip.
type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

// InstallSnapshotRequest ships a full snapshot to a follower that is too far
// behind to be caught up from the log.
type InstallSnapshotRequest struct {
	Term     uint64   `json:"term"`
	LeaderID NodeID   `json:"leader_id"`
	Snapshot Snapshot `json:"snapshot"`
}

// InstallSnapshotResponse is the reply to InstallSnapshotRequest.
type InstallSnapshotResponse struct {
	Term uint64 `json:"term"`
}

// Transport delivers RPCs to peers.
type Transport interface {
	RequestVote(ctx context.Context, to NodeID, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, to NodeID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

// Handler is the receiving side of Transport, implemented by *Node.
type Handler interface {
	HandleRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error)
	HandleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	HandleInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

var (
	// ErrStopped is returned by operations on a stopped node.
	ErrStopped = errors.New("raft: node stopped")
	// ErrLeadershipLost means the proposal was overwritten or its outcome is
	// unknown because leadership changed. Retrying with the same client
	// sequence number is safe.
	ErrLeadershipLost = errors.New("raft: leadership lost before commit")
)

// NotLeaderError is returned when an operation requires the leader.
type NotLeaderError struct {
	Leader NodeID // empty when unknown
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "raft: not leader (leader unknown)"
	}
	return fmt.Sprintf("raft: not leader (leader is %s)", e.Leader)
}

// IsNotLeader reports whether err is a *NotLeaderError and returns the hint.
func IsNotLeader(err error) (NodeID, bool) {
	var nl *NotLeaderError
	if errors.As(err, &nl) {
		return nl.Leader, true
	}
	return "", false
}

// Status is a point-in-time view of a node.
type Status struct {
	ID            NodeID `json:"id"`
	Role          string `json:"role"`
	Term          uint64 `json:"term"`
	Leader        NodeID `json:"leader"`
	CommitIndex   uint64 `json:"commit_index"`
	LastApplied   uint64 `json:"last_applied"`
	LastLogIndex  uint64 `json:"last_log_index"`
	SnapshotIndex uint64 `json:"snapshot_index"`
}
