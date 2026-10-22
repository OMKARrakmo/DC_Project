#!/usr/bin/env bash
set -e

echo "========================================================"
echo " RUNNING P2 LOAD BALANCING TEST (3 LOCAL NODES)"
echo "========================================================"

URL1="http://127.0.0.1:8080"
URL2="http://127.0.0.1:8081"
URL3="http://127.0.0.1:8082"

if ! curl -s -m 2 "$URL1/api/cluster" >/dev/null || ! curl -s -m 2 "$URL2/api/cluster" >/dev/null || ! curl -s -m 2 "$URL3/api/cluster" >/dev/null; then
  echo "Local nodes not running. Starting Node 1, 2, 3..."
  if [ ! -f target/dms.jar ]; then
    ./mvnw package -DskipTests
  fi
  mkdir -p logs
  nohup java -jar target/dms.jar --node.id=1 --server.port=8080 --rmi.port=1099 --node.host=127.0.0.1 --peers=127.0.0.1:8081:1100,127.0.0.1:8082:1101 > logs/node1.log 2>&1 &
  nohup java -jar target/dms.jar --node.id=2 --server.port=8081 --rmi.port=1100 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8082:1101 > logs/node2.log 2>&1 &
  nohup java -jar target/dms.jar --node.id=3 --server.port=8082 --rmi.port=1101 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8081:1100 > logs/node3.log 2>&1 &

  for i in {1..20}; do
    sleep 1
    if curl -s -m 2 "$URL1/api/cluster" >/dev/null && curl -s -m 2 "$URL2/api/cluster" >/dev/null && curl -s -m 2 "$URL3/api/cluster" >/dev/null; then
      echo "All 3 nodes ready!"
      break
    fi
  done
fi

sleep 3

echo "--- [TEST 1] Testing Round Robin Strategy (12 requests) ---"
RESP_RR=$(curl -s -X POST "$URL1/api/lb/burst" \
  -H "Content-Type: application/json" \
  -d '{"count":12,"strategy":"ROUND_ROBIN","delayMs":100}')

echo "Round Robin response: $RESP_RR"

python3 -c "
import sys, json
data = json.loads('''$RESP_RR''')
dist = data.get('distribution', {})
print('Distribution:', dist)
assert dist.get('1') == 4, f'Expected 4 for node 1, got {dist.get(\"1\")}'
assert dist.get('2') == 4, f'Expected 4 for node 2, got {dist.get(\"2\")}'
assert dist.get('3') == 4, f'Expected 4 for node 3, got {dist.get(\"3\")}'
print('ASSERTION PASSED: Round Robin cycled evenly (4, 4, 4)')
"

echo "--- [TEST 2] Testing Least Connections Strategy with Overlap (12 requests, 300ms delay) ---"
RESP_LC=$(curl -s -X POST "$URL1/api/lb/burst" \
  -H "Content-Type: application/json" \
  -d '{"count":12,"strategy":"LEAST_CONNECTIONS","delayMs":300}')

echo "Least Connections response: $RESP_LC"

python3 -c "
import sys, json
data = json.loads('''$RESP_LC''')
dist = data.get('distribution', {})
print('Distribution:', dist)
assert len(dist) == 3, f'Expected all 3 nodes to receive requests, got {dist}'
assert all(v > 0 for v in dist.values()), f'Expected every node to receive > 0 requests, got {dist}'
print('ASSERTION PASSED: Least Connections spread overlapping requests across nodes')
"

echo "=== ALL P2 LOAD BALANCER TESTS PASSED ==="
