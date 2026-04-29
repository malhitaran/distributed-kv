//this is for creating a new raft node
func NewRaft(id int, peers []int) *Raft {

	rf := &Raft{
		id:        id,
		stateNode: Follower,
		peers:     peers,

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
	defer rf.mu.Unlock()

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


func sendRequestVote





handlevote reply

become leader