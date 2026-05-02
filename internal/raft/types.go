package raft

import (
	"net"
	"sync"
	"time"
)

type Raft struct {
	mu       sync.Mutex    // guards all fields below
	stopOnce sync.Once     // ensures stopCh is only closed once (prevents panic on double Stop())
	stopCh   chan struct{} // closed when Stop() is called; all goroutines select on this

	listener net.Listener

	id    int
	state NodeState
	peers []int

	// Persistent state (would be flushed to disk in a real implementation)
	votedFor    int
	currentTerm int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders only (re-initialised after each election)
	matchIndex map[int]int
	nextIndex  map[int]int

	// Channel to forward committed entries to the KV layer (Phase 3)
	applyCh chan LogEntry

	electionTimer *time.Timer
	// Note: no heartbeatTimer field — the heartbeat is driven by a
	// time.NewTicker inside a goroutine launched in becomeLeader(),
	// not by a restartable timer. The goroutine exits via stopCh.
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)
