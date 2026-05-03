#!/bin/bash

# Maximize file descriptor limits for macOS
ulimit -n 10240 || true

# Start 3-node cluster
./bin/kvserver --id=0 --port=8080 --peers=1,2 > /dev/null 2>&1 &
PID1=$!
./bin/kvserver --id=1 --port=8081 --peers=0,2 > /dev/null 2>&1 &
PID2=$!
./bin/kvserver --id=2 --port=8082 --peers=0,1 > /dev/null 2>&1 &
PID3=$!

# Wait for startup
sleep 5

# Find leader
LEADER_PORT=8080
for port in 8080 8081 8082; do
    state=$(curl -s http://localhost:$port/status | jq -r .state)
    if [ "$state" = "leader" ]; then
        LEADER_PORT=$port
        break
    fi
done

echo "Leader is on port $LEADER_PORT"

# Generate JSON targets for vegeta since the CLI doesn't easily support raw text HTTP formatting
jq -ncM 'range(1; 10001) | {method: "POST", url: ("http://localhost:" + "'"$LEADER_PORT"'" + "/put"), body: ({"key": "key\(.)", "value": "value\(.)"} | tostring | @base64), header: {"Content-Type": ["application/json"]}}' > targets.json

echo "Running benchmarks..."

# Warmup
echo "=== Warmup ==="
vegeta attack -format=json -rate=1000 -duration=10s -targets=targets.json > /dev/null

# Actual benchmarks
echo ""
echo "=== 10K req/s for 10s ==="
vegeta attack -format=json -rate=10000 -duration=10s -targets=targets.json | tee results-10k.bin | vegeta report

echo ""
echo "=== 20K req/s for 10s ==="
vegeta attack -format=json -rate=20000 -duration=10s -targets=targets.json | tee results-20k.bin | vegeta report

echo ""
echo "=== 30K req/s for 10s ==="
vegeta attack -format=json -rate=30000 -duration=10s -targets=targets.json | tee results-30k.bin | vegeta report

echo ""
echo "=== 40K req/s for 10s ==="
vegeta attack -format=json -rate=40000 -duration=10s -targets=targets.json | tee results-40k.bin | vegeta report

echo ""
echo "=== 50K req/s for 10s ==="
vegeta attack -format=json -rate=50000 -duration=10s -targets=targets.json | tee results-50k.bin | vegeta report

# Cleanup
kill $PID1 $PID2 $PID3
