package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

var (
	clientPool = make(map[int]*rpc.Client)
	poolMu     sync.Mutex
)

// Serve starts an isolated RPC server for this node on port 8000+id.
//
// ── BUG 2 FIX ────────────────────────────────────────────────────────────────
// The original code called the package-level rpc.Register(rf), which writes
// into a single global registry keyed on the type name "Raft". The second
// and third calls silently fail (rpc.Register returns an error that was
// ignored), so all inbound RPCs — on all three ports — were dispatched to
// whichever Raft struct was registered first (node 0).
//
// This explains the test log lines like:
//
//	[Node 0] Received RequestVote from 2 for term 1   ← appears twice
//
// Node 2's vote request to peer 0 and to peer 1 both landed on node 0's
// handler because peer 1's port (8001) was also using node 0's registered
// handler. It also explains the term contamination between test runs, since
// the stale global registry carried node 0's term=4 from TestElection into
// TestLeaderFailure, causing the immediate jump to term 5.
//
// Fix: use rpc.NewServer() to get a fresh, isolated registry per node.
// ─────────────────────────────────────────────────────────────────────────────
func (rf *Raft) Serve() {
	server := rpc.NewServer()
	if err := server.Register(rf); err != nil {
		log.Fatalf("[Node %d] RPC registration failed: %v", rf.id, err)
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", 8000+rf.id))
	if err != nil {
		log.Fatalf("[Node %d] Failed to listen on port %d: %v", rf.id, 8000+rf.id, err)
	}
	rf.listener = l

	logDebug("[Node %d] RPC server started on port %d", rf.id, 8000+rf.id)

	go func() {
		for {
			conn, err := rf.listener.Accept()
			if err != nil {
				// Any error here means the listener was closed by Stop().
				// The original code used `continue` here, which spins forever
				// on a closed listener — a CPU-burning goroutine leak.
				return
			}
			go server.ServeConn(conn)
		}
	}()
}

func (rf *Raft) getClient(peer int) (*rpc.Client, error) {
	poolMu.Lock()
	defer poolMu.Unlock()

	if client, exists := clientPool[peer]; exists {
		return client, nil
	}

	client, err := rpc.Dial("tcp", fmt.Sprintf("localhost:%d", 8000+peer))
	if err != nil {
		return nil, err
	}

	clientPool[peer] = client
	return client, nil
}

// callRequestVote dials peer and invokes the RequestVote RPC.
// Returns false on any network or RPC error (treated as "no vote").
func (rf *Raft) callRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	client, err := rf.getClient(peer)
	if err != nil {
		return false
	}
	return client.Call("Raft.RequestVote", args, reply) == nil
}

// callAppendEntries dials peer and invokes the AppendEntries RPC.
// Returns false on any network or RPC error.
func (rf *Raft) callAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	client, err := rf.getClient(peer)
	if err != nil {
		return false
	}
	return client.Call("Raft.AppendEntries", args, reply) == nil
}
