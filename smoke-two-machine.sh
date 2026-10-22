#!/usr/bin/env bash
set -e

ARG1=${1:---local}
ARG2=${2:-}
PORT_A=${3:-8080}
PORT_B=${4:-8080}

if [ "$ARG1" = "--local" ] || [ "$ARG1" = "-local" ]; then
  echo "========================================================"
  echo " RUNNING SMOKE TEST IN --local MODE"
  echo "========================================================"

  URL_A="http://127.0.0.1:8080"
  URL_B="http://127.0.0.1:8081"

  # Check if nodes are running
  if ! curl -s -m 2 "$URL_A/api/cluster" >/dev/null || ! curl -s -m 2 "$URL_B/api/cluster" >/dev/null; then
    echo "Local nodes not running. Starting Node 1 and Node 2 in background..."
    if [ ! -f target/dms.jar ]; then
      ./mvnw package -DskipTests
    fi
    mkdir -p logs
    nohup java -jar target/dms.jar --node.id=1 --server.port=8080 --rmi.port=1099 --node.host=127.0.0.1 --peers=127.0.0.1:8081:1100 > logs/node1.log 2>&1 &
    nohup java -jar target/dms.jar --node.id=2 --server.port=8081 --rmi.port=1100 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099 > logs/node2.log 2>&1 &
    echo "Waiting for nodes to start..."
    for i in {1..20}; do
      sleep 1
      if curl -s -m 2 "$URL_A/api/cluster" >/dev/null && curl -s -m 2 "$URL_B/api/cluster" >/dev/null; then
        echo "Nodes ready!"
        break
      fi
    done
  fi

  sleep 2

  echo "[Step 1] Submitting donation to Node 1 targeting Node 2 via RMI..."
  RESPONSE=$(curl -s -X POST "$URL_A/api/donate" \
    -H "Content-Type: application/json" \
    -d '{"donor":"SmokeLocalUser","amount":500,"branch":"Mumbai","targetNodeId":2,"isolated":false}')

  echo "Response received: $RESPONSE"

  python3 -c "
import sys, json
data = json.loads('''$RESPONSE''')
print('\n----------------- RECEIPT DETAILS -----------------')
print('  Processing Node ID :', data.get('processingNodeId'))
print('  Host IP            :', data.get('host'))
print('  Operating System   :', data.get('os'))
print('  Thread Name        :', data.get('threadName'))
print('  Lamport Timestamp  :', data.get('lamportTs'))
print('  Primary Node ID    :', data.get('primaryId'))
print('  Receipt Status     :', data.get('status'))
print('  Message            :', data.get('message'))
print('---------------------------------------------------')

assert data.get('processingNodeId') == 2, f'Expected node 2, got {data.get(\"processingNodeId\")}'
assert data.get('status') == 'CONFIRMED', f'Expected CONFIRMED, got {data.get(\"status\")}'
assert data.get('threadName'), 'Thread name must not be empty'
assert data.get('os'), 'OS must not be empty'
assert data.get('lamportTs', 0) > 0, 'Lamport timestamp must be > 0'
print('ASSERTIONS: ALL PASSED')
"

  echo "=== ALL LOCAL SMOKE CHECKS PASSED ==="
  exit 0
else
  # Two-machine mode
  IP_A="$ARG1"
  IP_B="$ARG2"

  if [ -z "$IP_A" ] || [ -z "$IP_B" ]; then
    echo "Usage:"
    echo "  ./smoke-two-machine.sh --local"
    echo "  ./smoke-two-machine.sh <NODE_A_IP> <NODE_B_IP> [PORT_A] [PORT_B]"
    exit 1
  fi

  echo "========================================================"
  echo " RUNNING SMOKE TEST IN TWO-MACHINE MODE"
  echo " Gateway Node A: $IP_A:$PORT_A"
  echo " Expected Node B: $IP_B:$PORT_B"
  echo "========================================================"

  URL_A="http://$IP_A:$PORT_A"

  echo "[Step 1] Submitting donation via Gateway Node A ($URL_A)..."
  RESPONSE=$(curl -s -X POST "$URL_A/api/donate" \
    -H "Content-Type: application/json" \
    -d '{"donor":"SmokeTwoMachineUser","amount":750,"branch":"Mumbai","isolated":false}')

  echo "Response received: $RESPONSE"

  python3 -c "
import sys, json
data = json.loads('''$RESPONSE''')
print('\n----------------- RECEIPT DETAILS -----------------')
print('  Processing Node ID :', data.get('processingNodeId'))
print('  Host IP            :', data.get('host'))
print('  Operating System   :', data.get('os'))
print('  Thread Name        :', data.get('threadName'))
print('  Lamport Timestamp  :', data.get('lamportTs'))
print('  Primary Node ID    :', data.get('primaryId'))
print('  Receipt Status     :', data.get('status'))
print('  Message            :', data.get('message'))
print('---------------------------------------------------')

host = str(data.get('host', ''))
assert host not in ['127.0.0.1', 'localhost', '::1'], f'Host IP must not be loopback, got {host}'
assert host != '$IP_A', f'Processing host IP ({host}) must differ from Gateway A IP ($IP_A)'
assert data.get('threadName'), 'Thread name must not be empty'
assert data.get('os'), 'OS must not be empty'
assert data.get('lamportTs', 0) > 0, 'Lamport timestamp must be > 0'
print('ASSERTIONS: ALL PASSED')
"

  echo "=== TWO-MACHINE SMOKE TEST PASSED ==="
  exit 0
fi
