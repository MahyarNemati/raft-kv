package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Config holds the tunables for a Node.
type Config struct {
	ID    NodeID
	Peers []NodeID // every voting member, including ID

	// ElectionTimeout is the lower bound of the randomized election timeout;
	// each follower waits a uniformly random duration in [T, 2T).
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	RPCTimeout        time.Duration

	// MaxEntriesPerAppend caps the batch size of a single AppendEntries RPC.
	MaxEntriesPerAppend int
	// SnapshotThreshold is the number of applied entries past the last
	// snapshot that triggers a new one. Zero disables automatic snapshots.
	SnapshotThreshold uint64
	// CheckQuorum makes a leader step down when it has not heard from a
	// majority within one election timeout.
	CheckQuorum bool

	Logger *slog.Logger
}

// DefaultConfig returns production-leaning defaults for the given member set.
func DefaultConfig(id NodeID, peers []NodeID) Config {
	return Config{
		ID:                  id,
		Peers:               peers,
		ElectionTimeout:     300 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		RPCTimeout:          200 * time.Millisecond,
		MaxEntriesPerAppend: 512,
		SnapshotThreshold:   10000,
		CheckQuorum:         true,
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.ID == "" {
		errs = append(errs, errors.New("id is required"))
	}
	seen := make(map[NodeID]bool, len(c.Peers))
	for _, p := range c.Peers {
		if p == "" {
			errs = append(errs, errors.New("peer id must not be empty"))
		}
		if seen[p] {
			errs = append(errs, fmt.Errorf("duplicate peer %q", p))
		}
		seen[p] = true
	}
	if !seen[c.ID] {
		errs = append(errs, fmt.Errorf("peers must include self (%q)", c.ID))
	}
	if c.ElectionTimeout <= 0 || c.HeartbeatInterval <= 0 || c.RPCTimeout <= 0 {
		errs = append(errs, errors.New("timeouts must be positive"))
	} else if c.HeartbeatInterval >= c.ElectionTimeout {
		errs = append(errs, errors.New("heartbeat interval must be shorter than election timeout"))
	}
	if c.MaxEntriesPerAppend <= 0 {
		errs = append(errs, errors.New("max entries per append must be positive"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("raft: invalid config: %w", errors.Join(errs...))
	}
	return nil
}

func (c *Config) tickInterval() time.Duration {
	t := c.ElectionTimeout / 10
	if t < time.Millisecond {
		t = time.Millisecond
	}
	return t
}
