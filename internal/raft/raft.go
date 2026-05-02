// constructor, allocate a new Raft and initialize fields
// start the timer
// return a ready to use node
package raft

import (
	"log"
	"math/rand"
	"time"
)

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
	}

	// Start election timer
	rf.resetElectionTimer()

	return rf
}

func (rf *Raft) resetElectionTimer() {
	// Random timeout between 150-300ms (as per Raft paper)
	//ensure Go.120 or newer because older versions of Go produced
	//same sequence of random numbers
	//if this happens i need to look into seeding servers
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}

	rf.electionTimer = time.AfterFunc(timeout, func() {
		rf.startElection()
	})
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock() //defer means as soon as this function ends
	//thne we unlock
	//cleaner code and easier to debug, say we had multiple if statements and they all returned
	//we would then need to ensure unlock is in every branch and also we may miss one
	//more lines of code and error prone

	// Convert to candidate
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id

	log.Printf("[Node %d] Starting election for term %d", rf.id, rf.currentTerm)

	// Vote for self
	votesReceived := 1

	// Request votes from all peers
	for _, peer := range rf.peers {
		go rf.sendRequestVote(peer, &votesReceived)
	}

	// Reset election timer
	rf.resetElectionTimer()
}

func (rf *Raft) sendRequestVote(peer int, votesReceived *int) {

	rf.mu.Lock()
	//not a reference yet, keep variables local until used
	//across the network good go practice

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.id,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.getLastLogTerm(),
		//im going to create a helper here incase log is empty
		//if log empty, lastLogIndex will return -1 which is okay
		//if log was empty last log term would try access
		//-1 of the array which will cause a panic error
		//so in need of a helper function
	}
	rf.mu.Unlock()

	reply := RequestVoteReply{} //empty struct to hold the reply from peer

	//if they reply given our arguments and fill in the form
	//then we handle that reply(whether we was granted a vote) with our current votes(calculation)
	if rf.callRequestVote(peer, &args, &reply) {
		rf.handleVoteReply(&reply, votesReceived)
	}

}

func (rf *Raft) handleVoteReply(reply *RequestVoteReply, votesReceived *int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//we know the reciever has a higher term, step down
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return
	}

	//ensure were still in the candidate state
	if rf.state != Candidate {
		return
	}

	//checkk if we have majority then become leader
	if reply.VoteGranted {

		*votesReceived++

		majority := len(rf.peers)/2 + 1
		if *votesReceived >= majority {
			rf.becomeLeader()

		}
	}
}

//we dont lock here
//in go mutexes are non reentrant, meaning if a thread already has access
//to a resource and tries to lock it again, the server instantly freezes

func (rf *Raft) becomeLeader() {

	log.Printf("[Node %d] Became leader for term %d", rf.id, rf.currentTerm)
	rf.state = Leader

	// Initialize leader state
	for _, peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
	// Start sending heartbeats
	rf.sendHeartbeats()
}

// go standard RPC library has a strict unbreakable rule
// A RPC handler can only take exactly two arguments
// a pointer to the data(args)
// a pointer to the response
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[Node %d] Received RequestVote from %d for term %d",
		rf.id, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If candidate's term is less than ours, reject
	if args.Term < rf.currentTerm {
		return
	}

	// If candidate's term is greater, update our term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// Grant vote if:
	// 1. Haven't voted yet OR already voted for this candidate
	// 2. Candidate's log is at least as up-to-date as ours
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer() // Reset timer when granting vote

		log.Printf("[Node %d] Granted vote to %d for term %d",
			rf.id, args.CandidateId, args.Term)
	}
}

func (rf *Raft) isLogUpToDate(candidateIndex, candidateTerm int) bool {
	lastIndex := len(rf.log) - 1
	lastTerm := rf.getLastLogTerm()

	// Candidate's log is more up-to-date if:
	// 1. Last term is higher, OR
	// 2. Same term but longer log
	return candidateTerm > lastTerm ||
		(candidateTerm == lastTerm && candidateIndex >= lastIndex)
}

// stubs to get my test working
// STUB: Returns the term of the last entry in the log
func (rf *Raft) getLastLogTerm() int {
	// For now, just return 0 to satisfy the compiler
	return 0
}

// STUB: Blasts empty AppendEntries to all peers to maintain Leadership
func (rf *Raft) sendHeartbeats() {
	// We will write the heartbeat loop here later
}
