@echo off
echo Stopping local DMS nodes...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8080 :8081 :8082" ^| findstr "LISTENING"') do (
  echo Terminating process PID %%a
  taskkill /F /PID %%a 2>nul
)
echo All demo nodes stopped.
