package test

//with go if a import string does not start with a known domain
//e.g. github
//go compiler assumes were asking for a built in standard library package
import (
	"testing"
	"time"

	"github.com/malhitaran/distributed-kv/internal/raft"
)

func TestElection(t *testing.T) {
	//testing is essentially a state tracking struct
	//it has boolean error flags
	//kill swithces called fatal
	//logs
	//cleanup functions that you can hand it
	// Start 3 nodes
	nodes := make([]*raft.Raft, 3)
	//this is a list of memory addresses pointing to the actual
	//structs
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

	// Wait for election
	time.Sleep(1 * time.Second)

	// Check that exactly one leader exists
	leaders := 0
	for _, node := range nodes {
		if node.GetState() == raft.Leader {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf("Expected 1 leader, got %d", leaders)
	}
}

func TestLeaderFailure(t *testing.T) {
	// Start 3 nodes
	nodes := make([]*raft.Raft, 3)
	// ... (setup like above)

	// Wait for initial election
	time.Sleep(1 * time.Second)

	// Find and kill the leader
	for _, node := range nodes {
		if node.GetState() == raft.Leader {
			node.Stop()
			break
		}
	}

	// Wait for new election
	time.Sleep(1 * time.Second)

	// Verify new leader was elected
	leaders := 0
	for _, node := range nodes {
		if node.GetState() == raft.Leader {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf("Expected 1 leader after failure, got %d", leaders)
	}
}
