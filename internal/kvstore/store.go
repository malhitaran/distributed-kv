package kvstore

import (
	"errors"
	"log"
	"sync"

	"github.com/malhitaran/distributed-kv/internal/raft"
)

// KVStore is the actual key-value storage, backed by Raft for replication.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]interface{}
	raft *raft.Raft

	// Channels for client responses
	notifyCh map[int]chan Result // index -> result channel
}

type Result struct {
	Value interface{}
	Err   error
}

func NewKVStore(rf *raft.Raft) *KVStore {
	kv := &KVStore{
		data:     make(map[string]interface{}),
		raft:     rf,
		notifyCh: make(map[int]chan Result),
	}

	// Start goroutine to consume committed entries from Raft
	go kv.applyLoop()

	return kv
}

// applyLoop reads committed entries from Raft and applies them to the KV store
func (kv *KVStore) applyLoop() {
	applyCh := kv.raft.GetApplyCh()

	for entry := range applyCh {
		cmd, ok := entry.Command.(raft.Command)
		if !ok {
			log.Printf("Invalid command type: %T", entry.Command)
			continue
		}

		kv.mu.Lock()

		var result Result

		switch cmd.Op {
		case "PUT":
			kv.data[cmd.Key] = cmd.Value
			result.Value = nil
			result.Err = nil
			log.Printf("[KVStore] PUT %s = %v", cmd.Key, cmd.Value)

		case "GET":
			val, exists := kv.data[cmd.Key]
			if exists {
				result.Value = val
				result.Err = nil
			} else {
				result.Value = nil
				result.Err = nil // Or ErrKeyNotFound
			}
			log.Printf("[KVStore] GET %s = %v", cmd.Key, val)

		case "DELETE":
			delete(kv.data, cmd.Key)
			result.Value = nil
			result.Err = nil
			log.Printf("[KVStore] DELETE %s", cmd.Key)
		}

		// Notify waiting client
		if ch, exists := kv.notifyCh[entry.Index]; exists {
			ch <- result
			delete(kv.notifyCh, entry.Index)
		}

		kv.mu.Unlock()
	}
}

// Put inserts or updates a key-value pair
func (kv *KVStore) Put(key string, value interface{}) error {
	cmd := raft.Command{
		Op:    "PUT",
		Key:   key,
		Value: value,
	}

	index, _, isLeader := kv.raft.Propose(cmd)
	if !isLeader {
		return ErrNotLeader
	}

	// Wait for commit
	resultCh := make(chan Result, 1)

	kv.mu.Lock()
	kv.notifyCh[index] = resultCh
	kv.mu.Unlock()

	result := <-resultCh
	return result.Err
}

// Get retrieves a value by key
func (kv *KVStore) Get(key string) (interface{}, error) {
	// For GET, we can read directly (no consensus needed)
	// BUT: this gives us stale reads. For linearizable reads,
	// we'd need to go through Raft.

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, exists := kv.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}
	return val, nil
}

// Delete removes a key
func (kv *KVStore) Delete(key string) error {
	cmd := raft.Command{
		Op:  "DELETE",
		Key: key,
	}

	index, _, isLeader := kv.raft.Propose(cmd)
	if !isLeader {
		return ErrNotLeader
	}

	resultCh := make(chan Result, 1)

	kv.mu.Lock()
	kv.notifyCh[index] = resultCh
	kv.mu.Unlock()

	result := <-resultCh
	return result.Err
}

var (
	ErrNotLeader   = errors.New("not the leader")
	ErrKeyNotFound = errors.New("key not found")
)
