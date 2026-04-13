@echo off
setlocal EnableExtensions

echo [INFO] Stopping local backend/frontend windows started by launcher...

taskkill /FI "WINDOWTITLE eq Snowflake Migrator Backend" /T /F >nul 2>nul
set KILL_BACKEND=%ERRORLEVEL%

taskkill /FI "WINDOWTITLE eq Snowflake Migrator Frontend" /T /F >nul 2>nul
set KILL_FRONTEND=%ERRORLEVEL%

if "%KILL_BACKEND%"=="0" (
  echo [OK] Backend window stopped.
) else (
  echo [WARN] Backend window was not running or was already stopped.
)

if "%KILL_FRONTEND%"=="0" (
  echo [OK] Frontend window stopped.
) else (
  echo [WARN] Frontend window was not running or was already stopped.
)

echo [DONE] Native local services stop attempt complete.
exit /b 0
