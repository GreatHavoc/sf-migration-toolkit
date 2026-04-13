@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

where docker >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Docker CLI not found. Install Docker Desktop first.
  pause
  exit /b 1
)

docker info >nul 2>nul
if errorlevel 1 (
  echo [INFO] Docker engine is not running. Starting Docker Desktop...
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process 'Docker Desktop' -ErrorAction SilentlyContinue; Start-Process 'C:\Program Files\Docker\Docker\Docker Desktop.exe' -ErrorAction SilentlyContinue" >nul 2>nul
  call :wait_for_docker
  if errorlevel 1 (
    echo [ERROR] Docker did not become ready after 180 seconds.
    echo         Start Docker Desktop manually and run this script again.
    pause
    exit /b 1
  )
)

:docker_ready
echo [INFO] Docker is ready.
echo [INFO] Starting stack with docker compose up --build
echo.

docker compose up --build
set EXIT_CODE=%ERRORLEVEL%

if not "%EXIT_CODE%"=="0" (
  echo.
  echo [ERROR] docker compose exited with code %EXIT_CODE%.
  pause
)

exit /b %EXIT_CODE%

:wait_for_docker
set /a ELAPSED=0

:wait_loop
docker info >nul 2>nul
if not errorlevel 1 exit /b 0

if !ELAPSED! GEQ 180 exit /b 1

timeout /t 2 /nobreak >nul
set /a ELAPSED+=2
echo [INFO] Waiting for Docker engine... !ELAPSED!s
goto wait_loop
