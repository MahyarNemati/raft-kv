// Package kv is the replicated key-value state machine and the service layer
// that turns client operations into Raft proposals and linearizable reads.
package kv

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Op is a mutating operation carried in the Raft log.
type Op uint8

const (
	OpPut Op = iota + 1
	OpDelete
	OpCAS
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpCAS:
		return "cas"
	default:
		return fmt.Sprintf("op(%d)", uint8(o))
	}
}

// Command is the log payload. ClientID and Seq identify the request for
// at-most-once execution; an empty ClientID opts out of deduplication.
type Command struct {
	Op       Op     `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	Expected string `json:"expected,omitempty"`
	// ExpectAbsent makes a CAS succeed only if the key does not exist.
	ExpectAbsent bool   `json:"expect_absent,omitempty"`
	ClientID     string `json:"client_id,omitempty"`
	Seq          uint64 `json:"seq,omitempty"`
}

// MaxKeyLen bounds key size so a single entry cannot bloat the log.
const MaxKeyLen = 1024

// MaxValueLen bounds value size.
const MaxValueLen = 1 << 20

// ErrInvalid wraps validation failures.
var ErrInvalid = errors.New("kv: invalid request")

// Validate checks a command before it is proposed.
func (c *Command) Validate() error {
	switch c.Op {
	case OpPut, OpDelete, OpCAS:
	default:
		return fmt.Errorf("%w: unknown op %d", ErrInvalid, c.Op)
	}
	if c.Key == "" {
		return fmt.Errorf("%w: key is required", ErrInvalid)
	}
	if len(c.Key) > MaxKeyLen {
		return fmt.Errorf("%w: key longer than %d bytes", ErrInvalid, MaxKeyLen)
	}
	if len(c.Value) > MaxValueLen || len(c.Expected) > MaxValueLen {
		return fmt.Errorf("%w: value longer than %d bytes", ErrInvalid, MaxValueLen)
	}
	if c.ClientID != "" && c.Seq == 0 {
		return fmt.Errorf("%w: seq must be positive when client_id is set", ErrInvalid)
	}
	if c.Op == OpCAS && c.ExpectAbsent && c.Expected != "" {
		return fmt.Errorf("%w: expected and expect_absent are mutually exclusive", ErrInvalid)
	}
	return nil
}

func (c *Command) encode() ([]byte, error) { return json.Marshal(c) }

func decodeCommand(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

// Result is the outcome of an applied command.
type Result struct {
	// Index is the log index at which the command was applied.
	Index uint64 `json:"index"`
	// OK is true for every put, for a delete that removed a key, and for a
	// CAS that swapped.
	OK bool `json:"ok"`
	// Prev is the value before the command ran; Existed reports whether there
	// was one. For a failed CAS this is the current value.
	Prev    string `json:"prev,omitempty"`
	Existed bool   `json:"existed"`
	// Err is set when the state machine refused the command.
	Err string `json:"error,omitempty"`
}
