#!/usr/bin/env bash
echo "Stopping local DMS nodes..."
for pidfile in logs/node*.pid; do
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      echo "Killing PID $pid from $pidfile..."
      kill "$pid" || true
    fi
    rm -f "$pidfile"
  fi
done

# Fallback: kill by port
for port in 8080 8081 8082; do
  pid=$(lsof -t -i:$port 2>/dev/null || true)
  if [ -n "$pid" ]; then
    echo "Killing lingering process on port $port (PID: $pid)..."
    kill -9 $pid 2>/dev/null || true
  fi
done

echo "All demo nodes stopped."
