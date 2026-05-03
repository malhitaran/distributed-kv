package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/malhitaran/distributed-kv/internal/kvstore"
	"github.com/malhitaran/distributed-kv/internal/raft"
)

type Server struct {
	kv   *kvstore.KVStore
	raft *raft.Raft
}

func main() {
	id := flag.Int("id", 0, "Node ID")
	port := flag.Int("port", 8080, "HTTP port")
	peers := flag.String("peers", "", "Comma-separated peer IDs (e.g., '1,2')")
	flag.Parse()

	// Parse peer IDs
	peerIDs := []int{}
	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			pid, err := strconv.Atoi(strings.TrimSpace(p))
			if err != nil {
				log.Fatalf("Invalid peer ID: %s", p)
			}
			peerIDs = append(peerIDs, pid)
		}
	}

	log.Printf("[Node %d] Starting with peers: %v", *id, peerIDs)

	// Start Raft
	rf := raft.NewRaft(*id, peerIDs)
	rf.Serve()

	// Start KV store
	kv := kvstore.NewKVStore(rf)

	server := &Server{
		kv:   kv,
		raft: rf,
	}

	// Setup routes
	http.HandleFunc("/put", server.handlePut)
	http.HandleFunc("/get", server.handleGet)
	http.HandleFunc("/delete", server.handleDelete)
	http.HandleFunc("/status", server.handleStatus)
	http.HandleFunc("/health", server.handleHealth)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("[Node %d] HTTP server starting on %s", *id, addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// handlePut inserts or updates a key-value pair
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Key   string      `json:"key"`
		Value interface{} `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "Key cannot be empty", http.StatusBadRequest)
		return
	}

	// Measure latency
	start := time.Now()

	err := s.kv.Put(req.Key, req.Value)

	latency := time.Since(start)

	if err != nil {
		if err == kvstore.ErrNotLeader {
			http.Error(w, "Not the leader", http.StatusServiceUnavailable)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Latency-Ms", fmt.Sprintf("%.2f", float64(latency.Microseconds())/1000.0))
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ok",
		"latency_ms": float64(latency.Microseconds()) / 1000.0,
	})
}

// handleGet retrieves a value by key
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	start := time.Now()

	val, err := s.kv.Get(key)

	latency := time.Since(start)

	if err != nil {
		if err == kvstore.ErrKeyNotFound {
			http.Error(w, "Key not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Latency-Ms", fmt.Sprintf("%.2f", float64(latency.Microseconds())/1000.0))
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key":        key,
		"value":      val,
		"latency_ms": float64(latency.Microseconds()) / 1000.0,
	})
}

// handleDelete removes a key
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	start := time.Now()

	err := s.kv.Delete(key)

	latency := time.Since(start)

	if err != nil {
		if err == kvstore.ErrNotLeader {
			http.Error(w, "Not the leader", http.StatusServiceUnavailable)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Latency-Ms", fmt.Sprintf("%.2f", float64(latency.Microseconds())/1000.0))
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ok",
		"latency_ms": float64(latency.Microseconds()) / 1000.0,
	})
}

// handleStatus returns cluster status
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	state := s.raft.GetState()

	stateStr := "follower"
	if state == raft.Leader {
		stateStr = "leader"
	} else if state == raft.Candidate {
		stateStr = "candidate"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"state":        stateStr,
		"log_length":   s.raft.GetLogLength(),
		"commit_index": s.raft.GetCommitIndex(),
	})
}

// handleHealth returns 200 if node is healthy
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
