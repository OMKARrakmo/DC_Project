@echo off
setlocal
echo ================================================================
echo    LAUNCHING DMS PHASE 3 END-TO-END AUTOMATED TEST (CHECKS 1-3)
echo ================================================================

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0test-p3-e2e.ps1"
if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Phase 3 end-to-end tests failed with exit code %ERRORLEVEL%!
    exit /b %ERRORLEVEL%
)

echo.
echo [SUCCESS] Phase 3 end-to-end tests completed successfully!
exit /b 0
