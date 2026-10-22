@echo off
set NODE_ID=%1
if "%NODE_ID%"=="" set NODE_ID=1

set SERVER_PORT=%2
if "%SERVER_PORT%"=="" set SERVER_PORT=8080

set RMI_PORT=%3
if "%RMI_PORT%"=="" set RMI_PORT=1099

set NODE_HOST=%4
if "%NODE_HOST%"=="" set NODE_HOST=127.0.0.1

set PEERS=%5
if "%PEERS%"=="" set PEERS=

echo [DMS] Launching Node %NODE_ID% on HTTP %SERVER_PORT%, RMI %RMI_PORT%, Host %NODE_HOST%
if not "%PEERS%"=="" echo [DMS] Configured peers: %PEERS%

java -jar target\dms.jar --node.id=%NODE_ID% --server.port=%SERVER_PORT% --rmi.port=%RMI_PORT% --node.host=%NODE_HOST% --peers=%PEERS%
