@echo off
setlocal
echo ================================================================
echo    LAUNCHING DMS PHASE 5 END-TO-END AUTOMATED TEST (CHECKS 1-5)
echo ================================================================

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0test-p5-e2e.ps1"
if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Phase 5 end-to-end tests failed with exit code %ERRORLEVEL%!
    exit /b %ERRORLEVEL%
)

echo.
echo [SUCCESS] Phase 5 end-to-end tests completed successfully!
exit /b 0
