@echo off
echo ===================================================================
echo Starting 3-node local DMS cluster (Spring Boot + Java RMI)
echo ===================================================================

if not exist target\dms.jar (
  echo target\dms.jar not found! Building dms.jar first...
  call .\mvnw.cmd package -DskipTests
)

echo Starting Node 1 (HTTP 8080, RMI 1099)...
start "DMS Node 1 [Port 8080]" cmd /k "run-node.bat 1 8080 1099 127.0.0.1 127.0.0.1:8081:1100,127.0.0.1:8082:1101"

timeout /t 2 /nobreak >nul

echo Starting Node 2 (HTTP 8081, RMI 1100)...
start "DMS Node 2 [Port 8081]" cmd /k "run-node.bat 2 8081 1100 127.0.0.1 127.0.0.1:8080:1099,127.0.0.1:8082:1101"

timeout /t 2 /nobreak >nul

echo Starting Node 3 (HTTP 8082, RMI 1101)...
start "DMS Node 3 [Port 8082]" cmd /k "run-node.bat 3 8082 1101 127.0.0.1 127.0.0.1:8080:1099,127.0.0.1:8081:1100"

echo ===================================================================
echo 3 nodes started in separate windows!
echo Web Dashboards:
echo   Node 1: http://localhost:8080
echo   Node 2: http://localhost:8081
echo   Node 3: http://localhost:8082
echo To stop all nodes, run stop-demo-local.bat
echo ===================================================================
