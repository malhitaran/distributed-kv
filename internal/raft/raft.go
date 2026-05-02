package raft

import (
	"log"
	"math/rand"
	"time"
)

// ─── Construction ────────────────────────────────────────────────────────────

func NewRaft(id int, peers []int) *Raft {
	rf := &Raft{
		id:    id,
		state: Follower,
		peers: peers,

		votedFor:    -1,
		currentTerm: 0,
		log:         make([]LogEntry, 0),

		commitIndex: 0,
		lastApplied: 0,

		applyCh:    make(chan LogEntry, 100),
		nextIndex:  make(map[int]int),
		matchIndex: make(map[int]int),

		stopCh: make(chan struct{}), // buffered is not needed; close() is the signal
	}

	rf.resetElectionTimer()
	return rf
}

// ─── Election timer ───────────────────────────────────────────────────────────

// resetElectionTimer restarts the countdown that triggers a new election.
// Must be called while holding rf.mu, except in NewRaft (no contention yet).
func (rf *Raft) resetElectionTimer() {
	// 150–300 ms matches the range recommended in the Raft paper.
	// Randomisation is what makes simultaneous split-votes unlikely.
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

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

	log.Printf("[Node %d] Starting election for term %d", id, term)

	// votes is a shared counter, but it is always accessed while holding
	// rf.mu, so no atomic operations are needed.
	votes := 1
	majority := len(peers)/2 + 1 // e.g. 2 for a 3-node cluster

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
	log.Printf("[Node %d] Became leader for term %d", rf.id, rf.currentTerm)

	for _, peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
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
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rf.stopCh:
			return
		case <-ticker.C:
			rf.mu.Lock()
			// Stop if we are no longer the leader, or no longer in this term.
			if rf.state != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			peers := append([]int{}, rf.peers...)
			leaderID := rf.id
			rf.mu.Unlock()

			for _, peer := range peers {
				go rf.sendHeartbeat(peer, term, leaderID)
			}
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

	// Reject messages from stale leaders.
	if args.Term < rf.currentTerm {
		return nil
	}

	// Valid contact from a current or newer leader.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = Follower
	rf.resetElectionTimer() // critical: this is what keeps followers alive
	reply.Success = true
	return nil
}

// RequestVote handles incoming RequestVote RPCs.
// Go's net/rpc requires the handler signature: (args *T, reply *U) error.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[Node %d] Received RequestVote from %d for term %d",
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

		log.Printf("[Node %d] Granted vote to %d for term %d",
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
	// Flip to Follower before anything else so that GetState() called
	// concurrently never sees a stale Leader on a stopped node.
	rf.mu.Lock()
	rf.state = Follower
	rf.mu.Unlock()

	// stopOnce prevents a double-close panic if Stop() is called twice
	// (the test cleanup functions do exactly this).
	rf.stopOnce.Do(func() {
		close(rf.stopCh)
	})

	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	if rf.listener != nil {
		rf.listener.Close()
	}
}
