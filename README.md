# Distributed KV Store with Raft

High-performance distributed key-value store implementing Raft consensus.

## Status
- [x] Project setup
- [x] Leader election
- [ ] Log replication
- [ ] KV storage integration
- [ ] Benchmarking

## Architecture
[Will add diagram]

## Running
```bash
# Node 1
go run cmd/server/main.go --id=1 --port=8001 --peers=8002,8003

# Node 2
go run cmd/server/main.go --id=2 --port=8002 --peers=8001,8003

# Node 3
go run cmd/server/main.go --id=3 --port=8003 --peers=8001,8002
```

## Metrics
[Will add benchmarks]
