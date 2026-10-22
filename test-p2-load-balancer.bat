@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0test-p2-load-balancer.ps1" %*
