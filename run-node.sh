#!/usr/bin/env bash
set -e

NODE_ID=${1:-1}
SERVER_PORT=${2:-8080}
RMI_PORT=${3:-1099}
NODE_HOST=${4:-127.0.0.1}
PEERS=${5:-}

echo "[DMS] Launching Node $NODE_ID on HTTP $SERVER_PORT, RMI $RMI_PORT, Host $NODE_HOST"
if [ -n "$PEERS" ]; then
  echo "[DMS] Configured peers: $PEERS"
fi

java -jar target/dms.jar --node.id="$NODE_ID" --server.port="$SERVER_PORT" --rmi.port="$RMI_PORT" --node.host="$NODE_HOST" --peers="$PEERS"
