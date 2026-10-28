package kv

import (
	"context"
	"errors"
	"fmt"
)

// Consensus is the slice of *raft.Node the store depends on.
type Consensus interface {
	Propose(ctx context.Context, data []byte) (any, error)
	ReadIndex(ctx context.Context) (uint64, error)
}

// Consistency selects the read path.
type Consistency int

const (
	// Linearizable reads confirm leadership with a quorum (ReadIndex) and
	// wait for the local state machine to catch up before reading.
	Linearizable Consistency = iota
	// Stale reads return local state on any node without coordination.
	Stale
)

// ParseConsistency maps the API spelling to a Consistency.
func ParseConsistency(s string) (Consistency, error) {
	switch s {
	case "", "linearizable":
		return Linearizable, nil
	case "stale":
		return Stale, nil
	default:
		return 0, fmt.Errorf("%w: unknown consistency %q", ErrInvalid, s)
	}
}

// ErrRejected means the state machine applied the entry but refused the
// command, for example a request with a superseded sequence number.
var ErrRejected = errors.New("kv: command rejected")

// Store executes client operations against the replicated FSM.
type Store struct {
	raft Consensus
	fsm  *FSM
}

// NewStore wires a store to its consensus module and local state machine.
func NewStore(c Consensus, fsm *FSM) *Store { return &Store{raft: c, fsm: fsm} }

// Execute proposes cmd and waits for it to be applied.
func (s *Store) Execute(ctx context.Context, cmd Command) (Result, error) {
	if err := cmd.Validate(); err != nil {
		return Result{}, err
	}
	b, err := cmd.encode()
	if err != nil {
		return Result{}, fmt.Errorf("kv: encode command: %w", err)
	}
	v, err := s.raft.Propose(ctx, b)
	if err != nil {
		return Result{}, err
	}
	res, ok := v.(Result)
	if !ok {
		return Result{}, fmt.Errorf("kv: unexpected apply result %T", v)
	}
	if res.Err != "" {
		return res, fmt.Errorf("%w: %s", ErrRejected, res.Err)
	}
	return res, nil
}

// Get reads key with the requested consistency.
func (s *Store) Get(ctx context.Context, key string, c Consistency) (value string, found bool, err error) {
	if key == "" {
		return "", false, fmt.Errorf("%w: key is required", ErrInvalid)
	}
	if c == Linearizable {
		if _, err := s.raft.ReadIndex(ctx); err != nil {
			return "", false, err
		}
	}
	v, ok := s.fsm.Get(key)
	return v, ok, nil
}
