package raft

import (
	"sync"
	"time"
)

type Raft struct {

	//prevent race condition
	mu sync.Mutex //need a lock for individual nodes

	//node specific info
	id    int
	state NodeState
	peers []int

	//persistent storage
	votedFor    int
	currentTerm int
	log         []LogEntry

	//Fast storage
	commitIndex int
	lastApplied int

	//for the leader node still in fast storage
	matchIndex map[int]int //highest index known
	nextIndex  map[int]int //the next guess

	//channel to send commited entries to KV store
	applyCh chan LogEntry

	heartBeatTime *time.Timer
	electionTimer *time.Timer
	//debugged error here, timer is a stopwatch
	//time is just a data value of current time
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{} //interface to allow any type
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)
