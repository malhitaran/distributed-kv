// internal/raft/network.go
package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

func (rf *Raft) callRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// Connect to peer
	address := fmt.Sprintf("localhost:%d", 8000+peer)
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return false
	}
	//limited resources, so i have to close
	//limited file descriptors
	//only allowed 1024 ports open on a mac at once
	defer client.Close()

	// Call RPC
	err = client.Call("Raft.RequestVote", args, reply)
	return err == nil
}

//concurrent network listener
//making our nodes active servers on the network

// Start RPC server
func (rf *Raft) serve() {

	//we need to register what a raft is
	//remote procedures is blind by default
	//scans the memory block associated with rf
	// all methods beginning with a capital letter
	// attached to our rf are made avaiable to be
	// triggered by oncoming network traffic
	rpc.Register(rf)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", 8000+rf.id))
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[Node %d] RPC server started on port %d", rf.id, 8000+rf.id)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
}

//we thread here, we use a go function in order to
//run a infinite loop on a different thread so it doesnt
//affect our main program
// we also use a go command to make multiple connection channels
