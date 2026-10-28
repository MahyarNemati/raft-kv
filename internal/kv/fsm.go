package kv

import (
	"encoding/json"
	"fmt"
	"sync"
)

// session remembers the last request executed for a client, which is enough
// for clients that keep at most one write outstanding.
type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

// FSM is the replicated map. Apply runs on the Raft applier goroutine; reads
// may run concurrently from request handlers.
type FSM struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[string]session
	applied  uint64
}

// NewFSM returns an empty state machine.
func NewFSM() *FSM {
	return &FSM{data: make(map[string]string), sessions: make(map[string]session)}
}

// Apply implements raft.StateMachine. It always returns a Result.
func (f *FSM) Apply(index uint64, b []byte) any {
	cmd, err := decodeCommand(b)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = index
	if err != nil {
		return Result{Index: index, Err: fmt.Sprintf("decode command: %v", err)}
	}

	if cmd.ClientID != "" {
		if s, ok := f.sessions[cmd.ClientID]; ok {
			switch {
			case cmd.Seq == s.Seq:
				return s.Result
			case cmd.Seq < s.Seq:
				return Result{Index: index, Err: fmt.Sprintf("stale request: seq %d already superseded by %d", cmd.Seq, s.Seq)}
			}
		}
	}

	res := f.execLocked(index, &cmd)
	if cmd.ClientID != "" {
		f.sessions[cmd.ClientID] = session{Seq: cmd.Seq, Result: res}
	}
	return res
}

func (f *FSM) execLocked(index uint64, cmd *Command) Result {
	prev, existed := f.data[cmd.Key]
	res := Result{Index: index, Prev: prev, Existed: existed}
	switch cmd.Op {
	case OpPut:
		f.data[cmd.Key] = cmd.Value
		res.OK = true
	case OpDelete:
		delete(f.data, cmd.Key)
		res.OK = existed
	case OpCAS:
		match := (cmd.ExpectAbsent && !existed) || (!cmd.ExpectAbsent && existed && prev == cmd.Expected)
		if match {
			f.data[cmd.Key] = cmd.Value
			res.OK = true
		}
	default:
		res.Err = fmt.Sprintf("unknown op %d", cmd.Op)
	}
	return res
}

// Get reads a key from local state. Callers wanting linearizability must
// first establish a read index; see Store.Get.
func (f *FSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[key]
	return v, ok
}

// Len returns the number of keys.
func (f *FSM) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.data)
}

type fsmImage struct {
	Applied  uint64             `json:"applied"`
	Data     map[string]string  `json:"data"`
	Sessions map[string]session `json:"sessions"`
}

// Snapshot implements raft.StateMachine.
func (f *FSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return json.Marshal(fsmImage{Applied: f.applied, Data: f.data, Sessions: f.sessions})
}

// Restore implements raft.StateMachine.
func (f *FSM) Restore(b []byte) error {
	var img fsmImage
	if err := json.Unmarshal(b, &img); err != nil {
		return fmt.Errorf("kv: decode snapshot: %w", err)
	}
	if img.Data == nil {
		img.Data = make(map[string]string)
	}
	if img.Sessions == nil {
		img.Sessions = make(map[string]session)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data, f.sessions, f.applied = img.Data, img.Sessions, img.Applied
	return nil
}
