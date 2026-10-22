@echo off
setlocal
echo ================================================================
echo    LAUNCHING DMS PHASE 4 END-TO-END AUTOMATED TEST (CHECKS 1-4)
echo ================================================================

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0test-p4-e2e.ps1"
if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Phase 4 end-to-end tests failed with exit code %ERRORLEVEL%!
    exit /b %ERRORLEVEL%
)

echo.
echo [SUCCESS] Phase 4 end-to-end tests completed successfully!
exit /b 0
