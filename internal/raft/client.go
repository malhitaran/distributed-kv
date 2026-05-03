package raft

import "log"

// Command represents a client operation (e.g., PUT/GET/DELETE)
type Command struct {
	Op    string // "PUT", "GET", "DELETE"
	Key   string
	Value interface{} // nil for GET/DELETE
}

// Propose submits a command to be replicated via Raft.
// Returns:
//   - index: where this command will appear in the log (if committed)
//   - term: the current term (client can use this to detect stale reads)
//   - isLeader: false if this node isn't the leader
//
// If isLeader is false, the client should retry on a different node.
func (rf *Raft) Propose(cmd Command) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Only leaders accept client commands
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// Append to our log
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: cmd,
	}
	rf.log = append(rf.log, entry)

	index = len(rf.log) - 1
	term = rf.currentTerm

	log.Printf("[Node %d] Accepted command %+v at index %d, term %d",
		rf.id, cmd, index, term)

	// Trigger immediate replication (don't wait for next heartbeat)
	go rf.replicateToAll()

	return index, term, true
	//we need to return these items for three reasons
	//first the client needs to know if its sending commands to the actual leader
	//hench we return bool for isLeader
	//second we need a way for the client to know which index holds its command
	//so we return them a reciept for their index, so they know
	//e.g. log entry is at pos 5 so they wait for 5 to be commited
	//we return term to know which leadership epoch accepted this command
	//e.g. a client sends a cmd, say index 12 term 7
	//but it crashes, the term then goes to 8
	//client asks did my index commit
	//not enough info alone, the exact same command in the new term
	//could overwrite index 12 and this would be accepted
	//so we need term too
}
