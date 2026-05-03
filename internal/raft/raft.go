package raft

import (
	"encoding/gob" // <--- 1. ADD THIS
	"math/rand"
	"time"
)

// 2. ADD THIS ENTIRE FUNCTION
func init() {
	gob.Register(Command{})
}

// ─── Construction ────────────────────────────────────────────────────────────

func NewRaft(id int, peers []int) *Raft {
	rf := &Raft{
		id:    id,
		state: Follower,
		peers: peers,

		votedFor:    -1,
		currentTerm: 0,
		log:         make([]LogEntry, 0),

		commitIndex: -1,
		lastApplied: -1,

		applyCh:    make(chan CommitEntry, 100),
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),

		stopCh: make(chan struct{}), // buffered is not needed; close() is the signal

		pendingReplication: false,
	}

	rf.mu.Lock()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	return rf
}

// ─── Election timer ───────────────────────────────────────────────────────────

// resetElectionTimer restarts the countdown that triggers a new election.
// Must be called while holding rf.mu, except in NewRaft (no contention yet).
func (rf *Raft) resetElectionTimer() {
	// 150–300 ms matches the range recommended in the Raft paper.
	// Randomisation is what makes simultaneous split-votes unlikely.
	timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond

	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	rf.electionTimer = time.AfterFunc(timeout, rf.startElection)
}

// ─── Leader election ──────────────────────────────────────────────────────────

func (rf *Raft) startElection() {
	rf.mu.Lock()

	// If Stop() was already called, do not start a new election.
	select {
	case <-rf.stopCh:
		rf.mu.Unlock()
		return
	default:
	}

	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id

	// Snapshot everything the goroutines will need *before* releasing the
	// lock. This avoids holding the lock across RPC calls (which block)
	// and makes each goroutine self-contained.
	term := rf.currentTerm
	id := rf.id
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.getLastLogTerm()
	peers := append([]int{}, rf.peers...) // defensive copy

	rf.resetElectionTimer()
	rf.mu.Unlock()

	logDebug("[Node %d] Starting election for term %d", id, term)

	// votes is a shared counter, but it is always accessed while holding
	// rf.mu, so no atomic operations are needed.
	votes := 1
	clusterSize := len(peers) + 1
	majority := clusterSize/2 + 1

	for _, peer := range peers {
		go func(p int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			if !rf.callRequestVote(p, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Seeing a higher term means someone else has moved on — step down.
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.resetElectionTimer()
				return
			}

			// Stale reply: we've already moved to a different term or state
			// (e.g. another candidate beat us, or we already became leader).
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				votes++
				// Guard against calling becomeLeader() more than once if
				// several replies arrive in quick succession (both see votes
				// cross the threshold before state flips to Leader).
				if votes >= majority && rf.state == Candidate {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeLeader transitions the node to the Leader state.
// Must be called while holding rf.mu.
func (rf *Raft) becomeLeader() {
	// ── BUG 3 FIX ─────────────────────────────────────────────────────────
	// Without this, the election timer fires again while we are leader,
	// bumps the term, and tears down the very election we just won.
	rf.electionTimer.Stop()

	rf.state = Leader
	logDebug("[Node %d] Became leader for term %d", rf.id, rf.currentTerm)

	for _, peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = -1
	}

	// Launch the heartbeat goroutine tagged with the current term so that
	// it exits automatically if this node steps down into a later term.
	go rf.heartbeatLoop(rf.currentTerm)
}

// ─── Heartbeats ───────────────────────────────────────────────────────────────

// heartbeatLoop sends empty AppendEntries to all peers every 50 ms for as
// long as this node remains leader in the given term.
//
// ── BUG 1 FIX ──────────────────────────────────────────────────────────────
// The original sendHeartbeats() was an empty stub. Without heartbeats,
// followers never hear from the leader, their election timers fire, and they
// start a new election — producing multiple simultaneous leaders.
func (rf *Raft) heartbeatLoop(term int) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rf.stopCh:
			return
		case <-ticker.C:
			rf.mu.Lock()
			if rf.state != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			// Send heartbeats AND replicate log entries
			rf.replicateToAll() // Changed from sendHeartbeat
		}
	}
}

// sendHeartbeat sends a single empty AppendEntries RPC to one peer and handles
// the reply. If the peer reports a higher term, this node steps back down to
// follower.
func (rf *Raft) sendHeartbeat(peer, term, leaderID int) {
	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: leaderID,
	}
	reply := AppendEntriesReply{}

	if !rf.callAppendEntries(peer, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetElectionTimer()
	}

}

// ─── RPC handlers ─────────────────────────────────────────────────────────────

// AppendEntries handles incoming AppendEntries RPCs (heartbeats and, later,
// real log entries). Currently implements the heartbeat path only; full log
// replication logic belongs in Phase 2.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// Reject if leader's term is stale
	if args.Term < rf.currentTerm {
		return nil
	}

	// Valid leader - update term if necessary and step down
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = Follower
	rf.resetElectionTimer()

	// === LOG CONSISTENCY CHECK (NEW) ===

	// Check if our log matches at prevLogIndex
	if args.PrevLogIndex >= 0 {
		// Our log is too short
		if args.PrevLogIndex >= len(rf.log) {
			logDebug("[Node %d] Log too short: prevIndex=%d, logLen=%d",
				rf.id, args.PrevLogIndex, len(rf.log))
			return nil
		}

		// Term mismatch at prevLogIndex
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			logDebug("[Node %d] Term mismatch at index %d: want %d, got %d",
				rf.id, args.PrevLogIndex, args.PrevLogTerm,
				rf.log[args.PrevLogIndex].Term)

			// Delete conflicting entry and everything after it
			rf.log = rf.log[:args.PrevLogIndex]
			return nil
		}
	}

	// Log matches! Append new entries
	logIndex := args.PrevLogIndex + 1

	for i, entry := range args.Entries {
		if logIndex+i < len(rf.log) {
			// Entry exists - check if it matches
			if rf.log[logIndex+i].Term != entry.Term {
				// Conflict - delete this and all following entries
				rf.log = rf.log[:logIndex+i]
				rf.log = append(rf.log, entry)
			}
			// Else: entry already exists and matches, skip it
			//idempotent
		} else {
			// Append new entry
			rf.log = append(rf.log, entry)
		}
	}

	if len(args.Entries) > 0 {
		logDebug("[Node %d] Appended %d entries, log now has %d entries",
			rf.id, len(args.Entries), len(rf.log))
	}

	// Update commit index

	if args.LeaderCommit > rf.commitIndex {
		// Calculate the new commit first
		newCommit := min(args.LeaderCommit, len(rf.log)-1)

		// Explicit guard: NEVER go backwards
		if newCommit > rf.commitIndex {
			oldCommit := rf.commitIndex
			rf.commitIndex = newCommit

			logDebug("[Node %d] Advanced commitIndex from %d to %d",
				rf.id, oldCommit, rf.commitIndex)

			rf.applyCommittedEntries()
		}
	}

	reply.Success = true
	return nil
}

// Helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// RequestVote handles incoming RequestVote RPCs.
// Go's net/rpc requires the handler signature: (args *T, reply *U) error.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logDebug("[Node %d] Received RequestVote from %d for term %d",
		rf.id, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Candidate is behind us — reject.
	if args.Term < rf.currentTerm {
		return nil
	}

	// Candidate is ahead — update our term and become a fresh follower.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// Grant the vote only if:
	//   1. We haven't voted yet (or already voted for this candidate), AND
	//   2. The candidate's log is at least as up-to-date as ours.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()

		logDebug("[Node %d] Granted vote to %d for term %d",
			rf.id, args.CandidateId, args.Term)
	}

	return nil
}

// isLogUpToDate returns true if the candidate's log is at least as
// up-to-date as ours, using the Raft paper's definition (§5.4.1):
//   - higher last term wins; ties broken by longer log.
//
// Callers must hold rf.mu.
func (rf *Raft) isLogUpToDate(candidateIndex, candidateTerm int) bool {
	lastIndex := len(rf.log) - 1
	lastTerm := rf.getLastLogTerm()
	return candidateTerm > lastTerm ||
		(candidateTerm == lastTerm && candidateIndex >= lastIndex)
}

// ─── Test hooks ───────────────────────────────────────────────────────────────

// GetState safely returns the node's current state.
func (rf *Raft) GetState() NodeState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

// Stop shuts this node down cleanly:
//   - resets state to Follower so a stopped node never appears as a leader
//     in the test harness's final count
//   - signals all background goroutines to exit via stopCh
//   - kills the election timer
//   - closes the network listener
func (rf *Raft) Stop() {
	rf.mu.Lock()
	defer rf.mu.Unlock() // Keep the door locked until the entire shutdown is finished

	// 1. Force state to Follower
	rf.state = Follower

	// 2. Kill the timer WHILE the door is locked (Fixes the zombie bug)
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}

	// 3. Trigger the kill switch for background threads
	rf.stopOnce.Do(func() {
		close(rf.stopCh)
	})

	// 4. Cut the network
	if rf.listener != nil {
		rf.listener.Close()
	}
}

// replicateToAll sends the latest log entries to all peers.
// Called when a new command is proposed, or periodically by the heartbeat loop.
func (rf *Raft) replicateToAll() {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	leaderID := rf.id
	leaderCommit := rf.commitIndex
	peers := append([]int{}, rf.peers...)

	rf.mu.Unlock()

	for _, peer := range peers {
		go rf.replicateToPeer(peer, term, leaderID, leaderCommit)
	}
}

// replicateToPeer sends log entries to a single follower.
func (rf *Raft) replicateToPeer(peer, term, leaderID, leaderCommit int) {
	rf.mu.Lock()

	// Double-check we're still the leader
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	// Get the next index to send to this peer
	nextIdx := rf.nextIndex[peer]

	// Build the AppendEntries args
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	// Entries to send: everything from nextIdx onward
	entries := []LogEntry{}
	if nextIdx < len(rf.log) {
		entries = append(entries, rf.log[nextIdx:]...)
	}

	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	reply := AppendEntriesReply{}

	if !rf.callAppendEntries(peer, &args, &reply) {
		return // Network failure
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If peer's term is higher, step down
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetElectionTimer()
		return
	}

	// Stale reply (we've moved on)
	if rf.state != Leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		// Peer accepted the entries!
		rf.nextIndex[peer] = nextIdx + len(entries)
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1

		// Check if we can advance commitIndex
		rf.updateCommitIndex()
	} else {
		// Peer's log doesn't match - decrement nextIndex and retry
		if rf.nextIndex[peer] > 0 {
			rf.nextIndex[peer]--
		}
		// Immediately retry with the decremented index
		go rf.replicateToPeer(peer, term, leaderID, rf.commitIndex)
	}
}

// updateCommitIndex checks if a majority of nodes have replicated an entry,
// and if so, advances the commit index.
//
// From the Raft paper (§5.3):
//
//	"If there exists an N such that N > commitIndex, a majority of
//	 matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N"
//
// Must be called while holding rf.mu.
func (rf *Raft) updateCommitIndex() {
	// Try each index from commitIndex+1 to the end of our log
	for n := rf.commitIndex + 1; n < len(rf.log); n++ {
		// Only commit entries from our current term (safety property)
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		// Count how many nodes have this entry
		replicaCount := 1 // Count ourselves
		for peer := range rf.matchIndex {
			if rf.matchIndex[peer] >= n {
				replicaCount++
			}
		}

		// Check if we have a majority
		majority := (len(rf.peers)+1)/2 + 1
		if replicaCount >= majority {
			// We can commit this entry!
			oldCommit := rf.commitIndex
			rf.commitIndex = n

			logDebug("[Node %d] Advanced commitIndex from %d to %d (replicated on %d/%d nodes)",
				rf.id, oldCommit, n, replicaCount, len(rf.peers)+1)

			// Apply newly committed entries
			rf.applyCommittedEntries()
		}
	}
}

// applyCommittedEntries sends all entries between lastApplied and commitIndex
// to the applyCh channel, where the KV store will consume them.
//
// Must be called while holding rf.mu.
func (rf *Raft) applyCommittedEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		entry := rf.log[rf.lastApplied]

		logDebug("[Node %d] Applying entry at index %d: %+v",
			rf.id, rf.lastApplied, entry.Command)

		// Send to KV store (non-blocking send)
		commitMsg := CommitEntry{
			Index: rf.lastApplied,
			Entry: entry,
		}

		select {
		case rf.applyCh <- commitMsg:
			// Sent successfully
		default:
			// Channel full - this should never happen with a buffer of 100
			logWarn("[Node %d] WARNING: applyCh is full!", rf.id)
		}
	}
}

// GetApplyCh returns the channel where committed entries appear.
// The KV store will read from this channel in Phase 3.
func (rf *Raft) GetApplyCh() <-chan CommitEntry {
	return rf.applyCh
}

func (rf *Raft) GetLogLength() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}

// Add to raft.go:
func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}
