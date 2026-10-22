#!/usr/bin/env bash
set -e

echo "==================================================================="
echo "Starting 3-node local DMS cluster (Spring Boot + Java RMI)"
echo "==================================================================="

if [ ! -f target/dms.jar ]; then
  echo "target/dms.jar not found! Building first..."
  ./mvnw package -DskipTests
fi

mkdir -p logs

echo "Starting Node 1 (HTTP 8080, RMI 1099)..."
nohup java -jar target/dms.jar --node.id=1 --server.port=8080 --rmi.port=1099 --node.host=127.0.0.1 --peers=127.0.0.1:8081:1100,127.0.0.1:8082:1101 > logs/node1.log 2>&1 &
echo $! > logs/node1.pid

sleep 2

echo "Starting Node 2 (HTTP 8081, RMI 1100)..."
nohup java -jar target/dms.jar --node.id=2 --server.port=8081 --rmi.port=1100 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8082:1101 > logs/node2.log 2>&1 &
echo $! > logs/node2.pid

sleep 2

echo "Starting Node 3 (HTTP 8082, RMI 1101)..."
nohup java -jar target/dms.jar --node.id=3 --server.port=8082 --rmi.port=1101 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8081:1100 > logs/node3.log 2>&1 &
echo $! > logs/node3.pid

echo "==================================================================="
echo "3 nodes started in background!"
echo "Web Dashboards:"
echo "  Node 1: http://localhost:8080"
echo "  Node 2: http://localhost:8081"
echo "  Node 3: http://localhost:8082"
echo "Logs saved in ./logs/"
echo "To stop all nodes, run ./stop-demo-local.sh"
echo "==================================================================="
