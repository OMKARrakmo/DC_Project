#!/usr/bin/env bash
# test-p3-e2e.sh: End-to-end test script for Phase 3 on Linux / macOS
# Checks:
# (1) Cross-node RMI routing
# (2) Synchronous replication consistency
# (3) Kill primary -> election -> failover -> confirmed pre-kill donation is still present

set -e

echo "================================================================="
echo "   DMS PHASE 3 END-TO-END TEST: BULLY, REPLICATION, FAILOVER   "
echo "================================================================="

kill_port() {
  local port=$1
  local pid=$(lsof -ti :$port 2>/dev/null || true)
  if [ -n "$pid" ]; then
    kill -9 $pid 2>/dev/null || true
  fi
}

cleanup() {
  echo "Cleaning up processes on ports 8080, 8081, 8082..."
  kill_port 8080
  kill_port 8081
  kill_port 8082
}
trap cleanup EXIT

cleanup
sleep 2

rm -rf data/node-*

if [ ! -f "target/dms.jar" ]; then
  echo "Building target/dms.jar..."
  ./mvnw package -DskipTests
fi

echo "Starting Node 1 (8080, RMI 1099, ID 1)..."
java -jar target/dms.jar --node.id=1 --server.port=8080 --rmi.port=1099 --node.host=127.0.0.1 --peers=127.0.0.1:8081:1100,127.0.0.1:8082:1101 > /dev/null 2>&1 &
P1_PID=$!

echo "Starting Node 2 (8081, RMI 1100, ID 2)..."
java -jar target/dms.jar --node.id=2 --server.port=8081 --rmi.port=1100 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8082:1101 > /dev/null 2>&1 &
P2_PID=$!

echo "Starting Node 3 (8082, RMI 1101, ID 3)..."
java -jar target/dms.jar --node.id=3 --server.port=8082 --rmi.port=1101 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8081:1100 > /dev/null 2>&1 &
P3_PID=$!

echo -n "Waiting for nodes to become ready..."
for i in $(seq 1 35); do
  sleep 1
  echo -n "."
  if curl -s http://127.0.0.1:8080/api/cluster >/dev/null 2>&1 && \
     curl -s http://127.0.0.1:8081/api/cluster >/dev/null 2>&1 && \
     curl -s http://127.0.0.1:8082/api/cluster >/dev/null 2>&1; then
    echo " Ready!"
    break
  fi
done

sleep 3

echo "--- Initial Bully Election ---"
curl -s -X POST http://127.0.0.1:8080/api/election/start >/dev/null
sleep 4

PRIMARY_ID=$(curl -s http://127.0.0.1:8080/api/cluster | grep -o '"coordinatorId":[0-9]*' | cut -d':' -f2)
echo "Elected Primary Coordinator: Node $PRIMARY_ID"

echo "--- CHECK 1: Cross-node RMI Routing ---"
REC1=$(curl -s -X POST http://127.0.0.1:8080/api/donate \
  -H "Content-Type: application/json" \
  -d '{"donor":"Alice CrossNode","amount":500,"branch":"Mumbai","targetNodeId":2,"isolated":false}')

echo "Receipt: $REC1"
PROC_NODE=$(echo "$REC1" | grep -o '"processingNodeId":[0-9]*' | cut -d':' -f2)
STATUS=$(echo "$REC1" | grep -o '"status":"[^"]*"' | cut -d':' -f2 | tr -d '"')
PRE_KILL_RECEIPT=$(echo "$REC1" | grep -o '"receiptId":"[^"]*"' | cut -d':' -f2 | tr -d '"')

if [ "$PROC_NODE" != "2" ] || [ "$STATUS" != "CONFIRMED" ]; then
  echo "CHECK 1 FAILED! procNode=$PROC_NODE status=$STATUS"
  exit 1
fi
echo "[CHECK 1 PASSED] Cross-node RMI routing verified"

echo "--- CHECK 2: Synchronous Replication Consistency ---"
curl -s -X POST http://127.0.0.1:8080/api/repl/mode -H "Content-Type: application/json" -d '{"mode":"SYNCHRONOUS"}' >/dev/null
curl -s -X POST http://127.0.0.1:8081/api/repl/mode -H "Content-Type: application/json" -d '{"mode":"SYNCHRONOUS"}' >/dev/null
curl -s -X POST http://127.0.0.1:8082/api/repl/mode -H "Content-Type: application/json" -d '{"mode":"SYNCHRONOUS"}' >/dev/null

curl -s -X POST http://127.0.0.1:8080/api/donate \
  -H "Content-Type: application/json" \
  -d '{"donor":"Bob Sync","amount":1200,"branch":"Pune","targetNodeId":1,"isolated":false}' >/dev/null

H1=$(curl -s http://127.0.0.1:8080/api/repl/hash | grep -o '"hash":"[^"]*"' | cut -d':' -f2 | tr -d '"')
H2=$(curl -s http://127.0.0.1:8081/api/repl/hash | grep -o '"hash":"[^"]*"' | cut -d':' -f2 | tr -d '"')
H3=$(curl -s http://127.0.0.1:8082/api/repl/hash | grep -o '"hash":"[^"]*"' | cut -d':' -f2 | tr -d '"')

echo "Hashes: Node1=$H1 Node2=$H2 Node3=$H3"
if [ "$H1" = "EMPTY" ] || [ "$H1" != "$H2" ] || [ "$H2" != "$H3" ]; then
  echo "CHECK 2 FAILED: Ledger hashes mismatch!"
  exit 1
fi
echo "[CHECK 2 PASSED] Synchronous replication consistency verified"

echo "--- CHECK 3: Primary Process Kill, Bully Failover & Data Persistence ---"
echo "Terminating Primary PID for Node $PRIMARY_ID..."
if [ "$PRIMARY_ID" = "3" ]; then
  kill -9 $P3_PID 2>/dev/null || true
  kill_port 8082
  SURVIVOR="http://127.0.0.1:8080"
else
  kill -9 $P1_PID 2>/dev/null || true
  kill_port 8080
  SURVIVOR="http://127.0.0.1:8082"
fi

echo "Waiting 8s for heartbeat failure detection and Bully election failover..."
sleep 8

NEW_COORD=$(curl -s $SURVIVOR/api/cluster | grep -o '"coordinatorId":[0-9]*' | cut -d':' -f2)
echo "New Coordinator elected: Node $NEW_COORD"
if [ "$NEW_COORD" = "$PRIMARY_ID" ]; then
  echo "CHECK 3 FAILED: Coordinator did not change!"
  exit 1
fi

REC_POST=$(curl -s -X POST $SURVIVOR/api/donate \
  -H "Content-Type: application/json" \
  -d '{"donor":"Charlie Failover","amount":800,"branch":"Chennai","isolated":false}')

POST_STATUS=$(echo "$REC_POST" | grep -o '"status":"[^"]*"' | cut -d':' -f2 | tr -d '"')
if [ "$POST_STATUS" != "CONFIRMED" ]; then
  echo "CHECK 3 FAILED: Post-failover donation failed!"
  exit 1
fi

LEDGER=$(curl -s $SURVIVOR/api/ledger)
if ! echo "$LEDGER" | grep -q "$PRE_KILL_RECEIPT"; then
  echo "CHECK 3 FAILED: Pre-kill receipt $PRE_KILL_RECEIPT was lost!"
  exit 1
fi

echo "[CHECK 3 PASSED] Failover succeeded and pre-kill confirmed donation is intact!"
echo "ALL PHASE 3 CHECKS PASSED!"
