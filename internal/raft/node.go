package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

const maxApplyBatch = 256

type proposalResult struct {
	value any
	err   error
}

type waiter struct {
	term uint64
	ch   chan proposalResult
}

// Node is a single Raft peer. All exported methods are safe for concurrent use.
type Node struct {
	cfg       Config
	logger    *slog.Logger
	fsm       StateMachine
	storage   Storage
	transport Transport
	others    []NodeID

	ctx    context.Context
	cancel context.CancelFunc
	stopCh chan struct{}
	wg     sync.WaitGroup

	mu       sync.Mutex
	stopped  bool
	fatalErr error
	rng      *rand.Rand

	role     Role
	term     uint64
	votedFor NodeID
	leader   NodeID
	votes    int
	log      *raftLog

	commitIndex    uint64
	lastApplied    uint64
	snapshot       *Snapshot
	pendingRestore *Snapshot

	electionDeadline time.Time

	// Leader-only state, reset on every election win.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	lastAck    map[NodeID]time.Time
	ackSeq     map[NodeID]uint64
	hbSeq      uint64
	triggers   map[NodeID]chan struct{}
	leaderStop chan struct{}

	waiters map[uint64]waiter
	changed chan struct{}
	applyCh chan struct{}
}

// NewNode restores a node from storage. Call Start to begin participating.
func NewNode(cfg Config, fsm StateMachine, storage Storage, transport Transport) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	st, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}

	var snapIndex, snapTerm uint64
	if st.Snapshot != nil {
		snapIndex, snapTerm = st.Snapshot.Meta.Index, st.Snapshot.Meta.Term
		if err := fsm.Restore(st.Snapshot.Data); err != nil {
			return nil, fmt.Errorf("raft: restore snapshot: %w", err)
		}
	}
	rl, err := newRaftLog(snapIndex, snapTerm, st.Entries)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:         cfg,
		logger:      logger.With("node", string(cfg.ID)),
		fsm:         fsm,
		storage:     storage,
		transport:   transport,
		ctx:         ctx,
		cancel:      cancel,
		stopCh:      make(chan struct{}),
		rng:         rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), hashID(cfg.ID))),
		term:        st.Hard.Term,
		votedFor:    st.Hard.VotedFor,
		log:         rl,
		commitIndex: snapIndex,
		lastApplied: snapIndex,
		snapshot:    st.Snapshot,
		waiters:     make(map[uint64]waiter),
		changed:     make(chan struct{}),
		applyCh:     make(chan struct{}, 1),
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.others = append(n.others, p)
		}
	}
	return n, nil
}

func hashID(id NodeID) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= 1099511628211
	}
	return h
}

// Start launches the background goroutines.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimerLocked()
	n.mu.Unlock()
	n.wg.Add(2)
	go n.runTicker()
	go n.runApplier()
	n.logger.Info("raft node started", "term", n.term, "last_index", n.log.lastIndex(), "snapshot_index", n.log.snapIndex())
}

// Stop halts the node and waits for its goroutines. It does not close storage.
func (n *Node) Stop() {
	n.mu.Lock()
	n.shutdownLocked(nil)
	n.mu.Unlock()
	n.wg.Wait()
}

// Err returns the fatal error that halted the node, if any.
func (n *Node) Err() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.fatalErr
}

func (n *Node) shutdownLocked(cause error) {
	if n.stopped {
		return
	}
	n.stopped = true
	n.fatalErr = cause
	if cause != nil {
		n.logger.Error("raft node halted", "err", cause)
	}
	n.cancel()
	close(n.stopCh)
	n.stopReplicatorsLocked()
	for idx, w := range n.waiters {
		w.ch <- proposalResult{err: ErrStopped}
		delete(n.waiters, idx)
	}
	n.notifyLocked()
}

// ID returns this node's identifier.
func (n *Node) ID() NodeID { return n.cfg.ID }

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:            n.cfg.ID,
		Role:          n.role.String(),
		Term:          n.term,
		Leader:        n.leader,
		CommitIndex:   n.commitIndex,
		LastApplied:   n.lastApplied,
		LastLogIndex:  n.log.lastIndex(),
		SnapshotIndex: n.log.snapIndex(),
	}
}

// Leader returns the current leader as known by this node.
func (n *Node) Leader() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) notifyLocked() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

// waitLocked blocks until cond reports done, re-evaluating it whenever node
// state changes. Must be called with n.mu held; returns with it held.
func (n *Node) waitLocked(ctx context.Context, cond func() (bool, error)) error {
	for {
		if n.stopped {
			return ErrStopped
		}
		done, err := cond()
		if err != nil || done {
			return err
		}
		ch := n.changed
		n.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			n.mu.Lock()
			return ctx.Err()
		}
		n.mu.Lock()
	}
}

func (n *Node) resetElectionTimerLocked() {
	base := n.cfg.ElectionTimeout
	n.electionDeadline = time.Now().Add(base + time.Duration(n.rng.Int64N(int64(base))))
}

func (n *Node) persistHardStateLocked() bool {
	if err := n.storage.SetHardState(HardState{Term: n.term, VotedFor: n.votedFor}); err != nil {
		n.shutdownLocked(fmt.Errorf("persist hard state: %w", err))
		return false
	}
	return true
}

func (n *Node) appendLocked(entries ...Entry) bool {
	if err := n.storage.Append(entries); err != nil {
		n.shutdownLocked(fmt.Errorf("persist entries: %w", err))
		return false
	}
	n.log.append(entries...)
	return true
}

// ---- role transitions ----

func (n *Node) becomeFollowerLocked(term uint64, leader NodeID) {
	wasLeader := n.role == Leader
	if term > n.term {
		n.term = term
		n.votedFor = ""
		if !n.persistHardStateLocked() {
			return
		}
	}
	if n.role != Follower {
		n.logger.Info("stepping down", "term", n.term, "from", n.role.String())
	}
	n.role = Follower
	n.leader = leader
	if wasLeader {
		n.stopReplicatorsLocked()
		n.resetElectionTimerLocked()
	}
	n.notifyLocked()
}

func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leader = ""
	n.votes = 1
	if !n.persistHardStateLocked() {
		return
	}
	n.resetElectionTimerLocked()
	n.notifyLocked()
	n.logger.Debug("starting election", "term", n.term)

	if n.votes >= n.quorum() {
		n.becomeLeaderLocked()
		return
	}
	req := &RequestVoteRequest{
		Term:         n.term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.log.lastIndex(),
		LastLogTerm:  n.log.lastTerm(),
	}
	for _, p := range n.others {
		n.wg.Add(1)
		go n.requestVote(p, req)
	}
}

func (n *Node) requestVote(peer NodeID, req *RequestVoteRequest) {
	defer n.wg.Done()
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	resp, err := n.transport.RequestVote(ctx, peer, req)
	cancel()
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	if resp.Term > n.term {
		n.becomeFollowerLocked(resp.Term, "")
		return
	}
	if n.role != Candidate || n.term != req.Term || !resp.VoteGranted {
		return
	}
	n.votes++
	if n.votes >= n.quorum() {
		n.becomeLeaderLocked()
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leader = n.cfg.ID
	now := time.Now()
	next := n.log.lastIndex() + 1
	n.nextIndex = make(map[NodeID]uint64, len(n.others))
	n.matchIndex = make(map[NodeID]uint64, len(n.others))
	n.lastAck = make(map[NodeID]time.Time, len(n.others))
	n.ackSeq = make(map[NodeID]uint64, len(n.others))
	n.triggers = make(map[NodeID]chan struct{}, len(n.others))
	n.leaderStop = make(chan struct{})
	for _, p := range n.others {
		n.nextIndex[p] = next
		n.lastAck[p] = now
		n.triggers[p] = make(chan struct{}, 1)
	}
	n.logger.Info("became leader", "term", n.term, "last_index", n.log.lastIndex())

	// A no-op from the new term lets the leader commit entries from earlier
	// terms and is the precondition for serving ReadIndex.
	if !n.appendLocked(Entry{Index: next, Term: n.term, Type: EntryNoop}) {
		return
	}
	for _, p := range n.others {
		n.wg.Add(1)
		go n.runReplicator(p, n.term, n.leaderStop, n.triggers[p])
	}
	n.advanceCommitLocked()
	n.notifyLocked()
}

func (n *Node) stopReplicatorsLocked() {
	if n.leaderStop != nil {
		close(n.leaderStop)
		n.leaderStop = nil
	}
	n.triggers = nil
}

func (n *Node) triggerReplicationLocked() {
	for _, ch := range n.triggers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// ---- background loops ----

func (n *Node) runTicker() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.tickInterval())
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	now := time.Now()
	if n.role == Leader {
		if n.cfg.CheckQuorum && !n.quorumActiveLocked(now) {
			n.logger.Warn("lost contact with quorum", "term", n.term)
			n.becomeFollowerLocked(n.term, "")
		}
		return
	}
	if now.After(n.electionDeadline) {
		n.startElectionLocked()
	}
}

func (n *Node) quorumActiveLocked(now time.Time) bool {
	active := 1
	for _, p := range n.others {
		if now.Sub(n.lastAck[p]) <= n.cfg.ElectionTimeout {
			active++
		}
	}
	return active >= n.quorum()
}

func (n *Node) runReplicator(peer NodeID, term uint64, stop <-chan struct{}, trigger <-chan struct{}) {
	defer n.wg.Done()
	hb := time.NewTicker(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		for n.replicateOnce(peer, term) {
			select {
			case <-stop:
				return
			default:
			}
		}
		select {
		case <-stop:
			return
		case <-trigger:
		case <-hb.C:
		}
	}
}

// replicateOnce sends one AppendEntries or InstallSnapshot to peer and
// reports whether there is more to send immediately.
func (n *Node) replicateOnce(peer NodeID, term uint64) bool {
	n.mu.Lock()
	if n.stopped || n.role != Leader || n.term != term {
		n.mu.Unlock()
		return false
	}
	next := n.nextIndex[peer]
	if next <= n.log.snapIndex() {
		snap := n.snapshot
		seq := n.hbSeq
		n.mu.Unlock()
		return n.sendSnapshot(peer, term, snap, seq)
	}
	prev := next - 1
	prevTerm, _ := n.log.term(prev)
	hi := min(n.log.lastIndex()+1, next+uint64(n.cfg.MaxEntriesPerAppend))
	req := &AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.slice(next, hi),
		LeaderCommit: n.commitIndex,
	}
	seq := n.hbSeq
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	resp, err := n.transport.AppendEntries(ctx, peer, req)
	cancel()
	if err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.handlePeerTermLocked(peer, term, resp.Term, seq) {
		return false
	}
	if resp.Success {
		match := prev + uint64(len(req.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
			n.advanceCommitLocked()
		}
		if match+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = match + 1
		}
		return n.nextIndex[peer] <= n.log.lastIndex()
	}

	ni := resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		if last := n.log.lastIndexOfTerm(resp.ConflictTerm); last != 0 {
			ni = last + 1
		}
	}
	if ni >= next {
		ni = next - 1
	}
	n.nextIndex[peer] = max(ni, 1)
	return true
}

func (n *Node) sendSnapshot(peer NodeID, term uint64, snap *Snapshot, seq uint64) bool {
	if snap == nil {
		return false
	}
	req := &InstallSnapshotRequest{Term: term, LeaderID: n.cfg.ID, Snapshot: *snap}
	ctx, cancel := context.WithTimeout(n.ctx, 4*n.cfg.RPCTimeout)
	resp, err := n.transport.InstallSnapshot(ctx, peer, req)
	cancel()
	if err != nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.handlePeerTermLocked(peer, term, resp.Term, seq) {
		return false
	}
	idx := snap.Meta.Index
	if idx > n.matchIndex[peer] {
		n.matchIndex[peer] = idx
		n.advanceCommitLocked()
	}
	if idx+1 > n.nextIndex[peer] {
		n.nextIndex[peer] = idx + 1
	}
	n.logger.Info("installed snapshot on peer", "peer", string(peer), "index", idx)
	return n.nextIndex[peer] <= n.log.lastIndex()
}

// handlePeerTermLocked processes the term in a replication response and
// records the acknowledgement. It reports whether the caller should continue
// acting as leader for term.
func (n *Node) handlePeerTermLocked(peer NodeID, term, respTerm, seq uint64) bool {
	if n.stopped {
		return false
	}
	if respTerm > n.term {
		n.becomeFollowerLocked(respTerm, "")
		return false
	}
	if n.role != Leader || n.term != term {
		return false
	}
	n.lastAck[peer] = time.Now()
	if seq > n.ackSeq[peer] {
		n.ackSeq[peer] = seq
		n.notifyLocked()
	}
	return true
}

func (n *Node) advanceCommitLocked() {
	matches := make([]uint64, 0, len(n.cfg.Peers))
	matches = append(matches, n.log.lastIndex())
	for _, p := range n.others {
		matches = append(matches, n.matchIndex[p])
	}
	slices.SortFunc(matches, func(a, b uint64) int {
		switch {
		case a > b:
			return -1
		case a < b:
			return 1
		}
		return 0
	})
	candidate := matches[n.quorum()-1]
	if candidate <= n.commitIndex {
		return
	}
	// Only entries from the current term are committed by counting replicas.
	if t, ok := n.log.term(candidate); !ok || t != n.term {
		return
	}
	n.commitIndex = candidate
	n.signalApply()
	n.notifyLocked()
}

func (n *Node) runApplier() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
		}
		for n.applyBatch() {
		}
	}
}

// applyBatch applies the next batch of committed entries (or a pending
// snapshot) and reports whether more work may remain.
func (n *Node) applyBatch() bool {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return false
	}
	if snap := n.pendingRestore; snap != nil {
		n.pendingRestore = nil
		n.mu.Unlock()
		err := n.fsm.Restore(snap.Data)
		n.mu.Lock()
		defer n.mu.Unlock()
		if err != nil {
			n.shutdownLocked(fmt.Errorf("restore snapshot: %w", err))
			return false
		}
		if snap.Meta.Index > n.lastApplied {
			n.lastApplied = snap.Meta.Index
		}
		for idx, w := range n.waiters {
			if idx <= snap.Meta.Index {
				w.ch <- proposalResult{err: ErrLeadershipLost}
				delete(n.waiters, idx)
			}
		}
		n.notifyLocked()
		return true
	}
	if n.lastApplied >= n.commitIndex {
		n.mu.Unlock()
		return false
	}
	lo := n.lastApplied + 1
	hi := min(n.commitIndex, lo+maxApplyBatch-1)
	entries := n.log.slice(lo, hi+1)
	n.mu.Unlock()

	results := make([]any, len(entries))
	for i, e := range entries {
		if e.Type == EntryCommand {
			results[i] = n.fsm.Apply(e.Index, e.Data)
		}
	}

	n.mu.Lock()
	n.lastApplied = hi
	for i, e := range entries {
		w, ok := n.waiters[e.Index]
		if !ok {
			continue
		}
		delete(n.waiters, e.Index)
		if w.term == e.Term {
			w.ch <- proposalResult{value: results[i]}
		} else {
			w.ch <- proposalResult{err: ErrLeadershipLost}
		}
	}
	n.notifyLocked()
	needSnap := n.cfg.SnapshotThreshold > 0 && n.lastApplied-n.log.snapIndex() >= n.cfg.SnapshotThreshold
	n.mu.Unlock()

	if needSnap {
		n.takeSnapshot()
	}
	return true
}

// takeSnapshot runs on the applier goroutine, so the state machine is
// exactly at lastApplied while it serializes.
func (n *Node) takeSnapshot() {
	n.mu.Lock()
	idx := n.lastApplied
	n.mu.Unlock()

	data, err := n.fsm.Snapshot()
	if err != nil {
		n.logger.Error("state machine snapshot failed", "err", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || idx <= n.log.snapIndex() {
		return
	}
	t, ok := n.log.term(idx)
	if !ok {
		return
	}
	snap := Snapshot{Meta: SnapshotMeta{Index: idx, Term: t}, Data: data}
	rest := n.log.slice(idx+1, n.log.lastIndex()+1)
	if err := n.storage.SaveSnapshot(snap, rest); err != nil {
		n.logger.Error("persist snapshot failed", "err", err, "index", idx)
		return
	}
	n.log.compactTo(idx, t)
	n.snapshot = &snap
	n.logger.Debug("log compacted", "index", idx, "retained", len(rest))
}

// ---- client-facing API ----

// Propose replicates data and blocks until it is applied, returning the
// state machine's result. A context error means the outcome is unknown.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.role != Leader {
		leader := n.leader
		n.mu.Unlock()
		return nil, &NotLeaderError{Leader: leader}
	}
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.term, Type: EntryCommand, Data: data}
	if !n.appendLocked(e) {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	ch := make(chan proposalResult, 1)
	n.waiters[e.Index] = waiter{term: e.Term, ch: ch}
	n.advanceCommitLocked()
	n.triggerReplicationLocked()
	n.mu.Unlock()

	select {
	case r := <-ch:
		return r.value, r.err
	case <-ctx.Done():
		n.mu.Lock()
		if w, ok := n.waiters[e.Index]; ok && w.ch == ch {
			delete(n.waiters, e.Index)
		}
		n.mu.Unlock()
		return nil, ctx.Err()
	}
}

// ReadIndex implements the read path from section 6.4 of the Raft thesis.
// It returns once the local state machine reflects every write committed
// before the call, after confirming leadership with a quorum round trip.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, ErrStopped
	}
	if n.role != Leader {
		return 0, &NotLeaderError{Leader: n.leader}
	}
	term := n.term
	stillLeader := func() error {
		if n.role != Leader || n.term != term {
			return &NotLeaderError{Leader: n.leader}
		}
		return nil
	}

	// The leader's commit index is only known to be current once it has
	// committed an entry from its own term.
	if err := n.waitLocked(ctx, func() (bool, error) {
		if err := stillLeader(); err != nil {
			return false, err
		}
		t, _ := n.log.term(n.commitIndex)
		return t == term, nil
	}); err != nil {
		return 0, err
	}
	readIndex := n.commitIndex

	if len(n.others) > 0 {
		n.hbSeq++
		seq := n.hbSeq
		n.triggerReplicationLocked()
		if err := n.waitLocked(ctx, func() (bool, error) {
			if err := stillLeader(); err != nil {
				return false, err
			}
			acks := 1
			for _, p := range n.others {
				if n.ackSeq[p] >= seq {
					acks++
				}
			}
			return acks >= n.quorum(), nil
		}); err != nil {
			return 0, err
		}
	}

	if err := n.waitLocked(ctx, func() (bool, error) {
		return n.lastApplied >= readIndex, nil
	}); err != nil {
		return 0, err
	}
	return readIndex, nil
}

// ---- RPC handlers ----

// HandleRequestVote implements the RequestVote receiver.
func (n *Node) HandleRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if req.Term > n.term {
		n.becomeFollowerLocked(req.Term, "")
	}
	resp := &RequestVoteResponse{Term: n.term}
	if req.Term < n.term {
		return resp, nil
	}
	upToDate := req.LastLogTerm > n.log.lastTerm() ||
		(req.LastLogTerm == n.log.lastTerm() && req.LastLogIndex >= n.log.lastIndex())
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		if !n.persistHardStateLocked() {
			return nil, ErrStopped
		}
		n.resetElectionTimerLocked()
		resp.VoteGranted = true
	}
	return resp, nil
}

// HandleAppendEntries implements the AppendEntries receiver.
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if req.Term < n.term {
		return &AppendEntriesResponse{Term: n.term}, nil
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
		if n.stopped {
			return nil, ErrStopped
		}
	}
	if n.leader != req.LeaderID {
		n.leader = req.LeaderID
		n.notifyLocked()
	}
	n.resetElectionTimerLocked()
	resp := &AppendEntriesResponse{Term: n.term}

	prev, prevTerm, entries := req.PrevLogIndex, req.PrevLogTerm, req.Entries
	if prev < n.log.snapIndex() {
		// Everything up to the snapshot is committed and therefore matches.
		skip := n.log.snapIndex() - prev
		if skip >= uint64(len(entries)) {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prev, prevTerm = n.log.snapIndex(), n.log.snapTerm()
	}

	if prev > n.log.lastIndex() {
		resp.ConflictIndex = n.log.lastIndex() + 1
		return resp, nil
	}
	if t, _ := n.log.term(prev); t != prevTerm {
		resp.ConflictTerm = t
		resp.ConflictIndex = n.log.firstIndexOfTerm(t, prev)
		return resp, nil
	}

	for i, e := range entries {
		if e.Index <= n.log.lastIndex() {
			if t, _ := n.log.term(e.Index); t == e.Term {
				continue
			}
			n.log.truncateFrom(e.Index)
		}
		if !n.appendLocked(entries[i:]...) {
			return nil, ErrStopped
		}
		break
	}

	lastNew := prev + uint64(len(entries))
	if req.LeaderCommit > n.commitIndex {
		if c := min(req.LeaderCommit, lastNew); c > n.commitIndex {
			n.commitIndex = c
			n.signalApply()
			n.notifyLocked()
		}
	}
	resp.Success = true
	return resp, nil
}

// HandleInstallSnapshot implements the InstallSnapshot receiver. Snapshots
// are sent whole; chunking is unnecessary at this project's state sizes.
func (n *Node) HandleInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if req.Term < n.term {
		return &InstallSnapshotResponse{Term: n.term}, nil
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
		if n.stopped {
			return nil, ErrStopped
		}
	}
	n.leader = req.LeaderID
	n.resetElectionTimerLocked()
	resp := &InstallSnapshotResponse{Term: n.term}

	meta := req.Snapshot.Meta
	if meta.Index <= n.commitIndex {
		return resp, nil
	}
	snap := Snapshot{Meta: meta, Data: append([]byte(nil), req.Snapshot.Data...)}
	n.log.compactTo(meta.Index, meta.Term)
	rest := n.log.slice(meta.Index+1, n.log.lastIndex()+1)
	if err := n.storage.SaveSnapshot(snap, rest); err != nil {
		n.shutdownLocked(fmt.Errorf("persist snapshot: %w", err))
		return nil, ErrStopped
	}
	n.snapshot = &snap
	n.pendingRestore = &snap
	n.commitIndex = meta.Index
	n.signalApply()
	n.notifyLocked()
	n.logger.Info("installed snapshot from leader", "index", meta.Index, "term", meta.Term)
	return resp, nil
}
