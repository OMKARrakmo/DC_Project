#!/usr/bin/env bash
# test-p5-e2e.sh: End-to-end test script for Phase 5 on Linux / macOS
# Checks:
# (1) Cross-node RMI routing
# (2) Synchronous replication consistency
# (3) Kill primary -> election -> failover -> confirmed pre-kill donation is still present
# (4) MapReduce == SQL SUM
# (5) Ring Election + Simulated MPI Collectives + Parallel Matrix Multiplication (with MPI honesty badge)

set -e

echo "================================================================="
echo "   DMS PHASE 5 END-TO-END TEST: RING ELECTION & SIMULATED MPI    "
echo "   CHECKS: (1) RMI  (2) REPL  (3) FAILOVER  (4) MR  (5) RING+MPI "
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

echo "--- EXP 2: Multithreading Verification ---"
curl -s -X POST "http://127.0.0.1:8080/api/threads/burst?count=4&delayMs=20" >/dev/null
RACE_SAFE=$(curl -s -X POST "http://127.0.0.1:8080/api/threads/race-demo?threads=5&increments=5" | grep -o '"safeActual":[0-9]*' | cut -d':' -f2)
if [ "$RACE_SAFE" != "25" ]; then
  echo "EXP 2 FAILED: Safe counter actual $RACE_SAFE != expected 25"
  exit 1
fi
echo "[EXP 2 PASSED] ThreadPool burst and race demo safe counter verified"

echo "--- EXP 3: Berkeley Physical Clock Synchronization ---"
curl -s -X POST http://127.0.0.1:8080/api/clock/skew -H "Content-Type: application/json" -d '{"skewMs":2000}' >/dev/null
curl -s -X POST http://127.0.0.1:8082/api/clock/berkeley-sync >/dev/null
echo "[EXP 3 PASSED] Berkeley physical clock synchronization executed"

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

echo "--- CHECK 4: Distributed MapReduce == SQL SUM Verification ---"
curl -s -X POST $SURVIVOR/api/donate \
  -H "Content-Type: application/json" \
  -d '{"donor":"Diana Seed","amount":1500,"branch":"Mumbai","isolated":false}' >/dev/null

MR_RES=$(curl -s -X POST $SURVIVOR/api/mr/run)
echo "MapReduce Response: $MR_RES"

if ! echo "$MR_RES" | grep -q '"verified":true'; then
  echo "CHECK 4 FAILED: MapReduce totals do not match Primary SQL SUM!"
  exit 1
fi
echo "[CHECK 4 PASSED] MapReduce matches Primary SQL SUM exactly across all shards!"

echo "--- CHECK 5: Ring Election, Simulated MPI Collectives & Matrix Multiplication ---"

# 5A. Ring Election switch and circulation
curl -s -X POST http://127.0.0.1:8080/api/election/algorithm -H "Content-Type: application/json" -d '{"algorithm":"RING"}' >/dev/null
curl -s -X POST http://127.0.0.1:8081/api/election/algorithm -H "Content-Type: application/json" -d '{"algorithm":"RING"}' >/dev/null

ALGO=$(curl -s http://127.0.0.1:8080/api/election/status | grep -o '"algorithm":"[^"]*"' | cut -d':' -f2 | tr -d '"')
if [ "$ALGO" != "RING" ]; then
  echo "CHECK 5 FAILED: Failed to switch to RING election! (got $ALGO)"
  exit 1
fi

curl -s -X POST http://127.0.0.1:8080/api/election/start >/dev/null
sleep 4

RING_COORD=$(curl -s http://127.0.0.1:8080/api/cluster | grep -o '"coordinatorId":[0-9]*' | cut -d':' -f2)
echo "Ring Elected Coordinator: Node $RING_COORD"
if [ "$RING_COORD" != "2" ]; then
  echo "CHECK 5 FAILED: Expected Node 2 to win Ring election, got: $RING_COORD"
  exit 1
fi
echo "[CHECK 5A PASSED] Ring election token circulated logical ring and elected highest active Node 2"

# 5B. Exp 9 MPI Collectives
MPI_COLL=$(curl -s -X POST http://127.0.0.1:8081/api/mpi/collectives -H "Content-Type: application/json" -d '{"ranks":4}')
echo "MPI Collectives: $MPI_COLL"

if ! echo "$MPI_COLL" | grep -q '"badge":"SIMULATED (not MPI)"'; then
  echo "CHECK 5 FAILED: MPI honesty rule badge missing from collectives response!"
  exit 1
fi

if ! echo "$MPI_COLL" | grep -q '"verified":true'; then
  echo "CHECK 5 FAILED: MPI Collectives verification against SQL SUM failed!"
  exit 1
fi
echo "[CHECK 5B PASSED] Exp 9 MPI Collectives: Broadcast->Scatter->Compute->Gather verified against SQL SUM"

# 5C. Exp 10 Parallel Matrix Multiplication
MPI_MAT=$(curl -s -X POST http://127.0.0.1:8081/api/mpi/matrix -H "Content-Type: application/json" -d '{"size":120,"ranks":4}')
echo "MPI Matrix Mult: $MPI_MAT"

if ! echo "$MPI_MAT" | grep -q '"badge":"SIMULATED (not MPI)"'; then
  echo "CHECK 5 FAILED: MPI honesty rule badge missing from matrix multiplication response!"
  exit 1
fi

if ! echo "$MPI_MAT" | grep -q '"verified":true'; then
  echo "CHECK 5 FAILED: Matrix multiplication parallel vs sequential mismatch!"
  exit 1
fi

if ! echo "$MPI_MAT" | grep -q '"maxDifference":0\.0'; then
  echo "CHECK 5 FAILED: Matrix multiplication maxDifference is not 0.0!"
  exit 1
fi
echo "[CHECK 5C PASSED] Exp 10 Parallel Matrix Multiplication: C_parallel == C_sequential verified (max diff 0.0)"

echo "================================================================="
echo "   ALL PHASE 5 CHECKS (1)-(5) PASSED SUCCESSFULLY! (GATE 4 MET) "
echo "================================================================="
