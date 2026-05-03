package test

import (
	"testing"
	"time"

	"github.com/malhitaran/distributed-kv/internal/raft"
)

func TestBasicReplication(t *testing.T) {
	// Start 3 nodes
	nodes := make([]*raft.Raft, 3)

	t.Cleanup(func() {
		for _, node := range nodes {
			if node != nil {
				node.Stop()
			}
		}
	})

	for i := 0; i < 3; i++ {
		peers := []int{}
		for j := 0; j < 3; j++ {
			if i != j {
				peers = append(peers, j)
			}
		}
		nodes[i] = raft.NewRaft(i, peers)
		nodes[i].Serve()
	}

	// Wait for leader election
	time.Sleep(1 * time.Second)

	// Find the leader
	var leader *raft.Raft
	for _, node := range nodes {
		if node.GetState() == raft.Leader {
			leader = node
			break
		}
	}

	if leader == nil {
		t.Fatal("No leader elected")
	}

	// Submit a command
	cmd := raft.Command{
		Op:    "PUT",
		Key:   "x",
		Value: "100",
	}

	index, term, isLeader := leader.Propose(cmd)

	if !isLeader {
		t.Fatal("Propose() returned isLeader=false")
	}

	t.Logf("Command proposed at index=%d, term=%d", index, term)

	// Wait for replication
	time.Sleep(500 * time.Millisecond)

	// Check that all nodes have the entry in their log
	for i, node := range nodes {
		logLen := node.GetLogLength()
		if logLen < index+1 {
			t.Fatalf("Node %d has log length %d, expected at least %d",
				i, logLen, index+1)
		}
	}

	t.Log("All nodes replicated the entry")
}

func TestCommitAfterMajority(t *testing.T) {
	// Start 5 nodes (so majority = 3)
	nodes := make([]*raft.Raft, 5)

	t.Cleanup(func() {
		for _, node := range nodes {
			if node != nil {
				node.Stop()
			}
		}
	})

	for i := 0; i < 5; i++ {
		peers := []int{}
		for j := 0; j < 5; j++ {
			if i != j {
				peers = append(peers, j)
			}
		}
		nodes[i] = raft.NewRaft(i, peers)
		nodes[i].Serve()
	}

	time.Sleep(1 * time.Second)

	// Find leader
	var leader *raft.Raft
	var leaderID int
	for i, node := range nodes {
		if node.GetState() == raft.Leader {
			leader = node
			leaderID = i
			break
		}
	}

	if leader == nil {
		t.Fatal("No leader elected")
	}

	// Kill 2 followers (so we still have majority: 3/5)
	killCount := 0
	for i, node := range nodes {
		if i != leaderID && killCount < 2 {
			node.Stop()
			nodes[i] = nil
			killCount++
			t.Logf("Killed node %d", i)
		}
	}

	// Submit command - should still commit (3/5 majority)
	cmd := raft.Command{Op: "PUT", Key: "y", Value: "200"}
	index, _, _ := leader.Propose(cmd)

	// Wait for commit
	time.Sleep(500 * time.Millisecond)

	// Check that leader committed it
	commitIdx := leader.GetCommitIndex()
	if commitIdx < index {
		t.Fatalf("Leader commitIndex=%d, expected at least %d", commitIdx, index)
	}

	t.Log("Command committed with 3/5 nodes alive")
}
