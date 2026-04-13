@echo off
setlocal EnableExtensions

cd /d "%~dp0"

where docker >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Docker CLI not found. Install Docker Desktop first.
  pause
  exit /b 1
)

echo [INFO] Stopping Docker services for this project...
docker compose down --remove-orphans
set EXIT_CODE=%ERRORLEVEL%

if not "%EXIT_CODE%"=="0" (
  echo.
  echo [ERROR] docker compose down failed with code %EXIT_CODE%.
  pause
)

exit /b %EXIT_CODE%
