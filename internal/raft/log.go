package raft

// getLastLogTerm returns the term of the last entry in the log,
// or 0 if the log is empty.
//
// This helper exists because accessing rf.log[-1] when the log is
// empty causes a panic. Callers (e.g. RequestVote, startElection)
// rely on 0 as a safe sentinel — a node with an empty log has never
// seen any entries, so any non-empty log is more up-to-date.
//
// Callers must hold rf.mu.
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}
