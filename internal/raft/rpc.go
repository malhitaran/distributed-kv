package raft

// RequestVoteArgs is sent by a Candidate to gather votes.
type RequestVoteArgs struct {
	Term         int // candidate's current term
	CandidateId  int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term  of candidate's last log entry
}

// RequestVoteReply is the response to a RequestVoteArgs.
type RequestVoteReply struct {
	Term        int  // receiver's currentTerm (so the candidate can update itself)
	VoteGranted bool // true means the candidate received this vote
}

// AppendEntriesArgs is sent by the Leader to replicate log entries and as a
// heartbeat (Entries is empty for heartbeats).
type AppendEntriesArgs struct {
	Term         int        // leader's current term
	LeaderId     int        // so followers can redirect clients
	PrevLogIndex int        // index of the log entry immediately before the new ones
	PrevLogTerm  int        // term of the PrevLogIndex entry
	Entries      []LogEntry // entries to store (empty slice = heartbeat)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply is the response to an AppendEntriesArgs.
type AppendEntriesReply struct {
	Term    int  // receiver's currentTerm (so the leader can update itself)
	Success bool // true if the follower's log matched PrevLogIndex/PrevLogTerm
}
