# Distributed Key-Value Store with Raft Consensus

High-performance distributed key-value store implementing the Raft consensus algorithm.

## Features

- **Raft Consensus** - Leader election, log replication, safety guarantees
- **High Throughput** - 10,000+ operations/second
- **Low Latency** - p99 ~ 5ms
- **Fault Tolerance** - Automatic leader failover, tolerates minority failures
- **HTTP API** - Simple REST interface

## Architecture

```text
┌─────────────────────────────────────┐
│         Client Requests              │
└──────────────┬──────────────────────┘
▼
┌─────────────────────────────────────┐
│      HTTP Server (Port 8080+id)      │
│  ┌──────────────┐  ┌──────────────┐ │
│  │   REST API   │  │  KV Storage  │ │
│  │  (PUT/GET/   │  │  (hashmap)   │ │
│  │   DELETE)    │  │              │ │
│  └──────┬───────┘  └───────┬──────┘ │
│         │                   │        │
│         ▼                   ▼        │
│  ┌──────────────────────────────┐   │
│  │    Raft Consensus Module     │   │
│  │  - Leader Election           │   │
│  │  - Log Replication           │   │
│  │  - Safety Properties         │   │
│  └──────────┬───────────────────┘   │
└─────────────┼───────────────────────┘
│
▼
Network (RPC to peers)
```

## Quick Start

**Build:**
```bash
go build -o bin/kvserver cmd/server/main.go
```

**Start 3-node cluster:**
```bash
# Terminal 1
./bin/kvserver --id=0 --port=8080 --peers=1,2

# Terminal 2
./bin/kvserver --id=1 --port=8081 --peers=0,2

# Terminal 3
./bin/kvserver --id=2 --port=8082 --peers=0,1
```

**Test it:**
```bash
# PUT
curl -X POST http://localhost:8080/put \
     -H "Content-Type: application/json" \
     -d '{"key":"hello","value":"world"}'

# GET
curl "http://localhost:8080/get?key=hello"

# DELETE
curl -X DELETE "http://localhost:8080/delete?key=hello"
```

## Performance

| Metric | Value |
|--------|-------|
| Throughput | 10,000+ ops/sec |
| Latency (p50) | ~2.6ms |
| Latency (p99) | ~5.4ms |
| Leader Failover | <1 second |

See [BENCHMARKS.md](BENCHMARKS.md) for detailed results.

## API Reference

### PUT
```bash
POST /put
Content-Type: application/json

{
  "key": "mykey",
  "value": "myvalue"
}
```

### GET
```bash
GET /get?key=mykey
```

### DELETE
```bash
DELETE /delete?key=mykey
```

### Status
```bash
GET /status

# Response:
{
  "state": "leader",
  "log_length": 1234,
  "commit_index": 1234
}
```

## Implementation Details

- **Language**: Go
- **Consensus**: Raft (leader election + log replication)
- **RPC**: Go net/rpc with connection pooling
- **State Machine**: In-memory key-value store
- **Persistence**: Not implemented (all state is volatile)

## Optimizations

1. **Batched Replication** - 5ms batching window
2. **Connection Pooling** - Reuse RPC connections
3. **Log Level Filtering** - Minimal logging in production
4. **Fast JSON** - jsoniter instead of encoding/json

## Limitations

- **No persistence** - All data lost on restart
- **No snapshots** - Log grows unbounded
- **No membership changes** - Cluster size is fixed
- **Localhost only** - Not production-ready networking

## License

MIT

## Author

Taran Malhi  
[linkedin.com/in/taran-malhi](https://linkedin.com/in/taran-malhi)  
[github.com/malhitaran](https://github.com/malhitaran)
