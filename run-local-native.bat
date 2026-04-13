@echo off
setlocal EnableExtensions

cd /d "%~dp0"

where uv >nul 2>nul
if errorlevel 1 (
  echo [ERROR] uv is not installed. Install from https://docs.astral.sh/uv/
  pause
  exit /b 1
)

where node >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Node.js is not installed. Install Node.js 20+ first.
  pause
  exit /b 1
)

where corepack >nul 2>nul
if errorlevel 1 (
  echo [ERROR] corepack not found. Install a recent Node.js distribution with Corepack.
  pause
  exit /b 1
)

echo [1/6] Enabling Corepack and activating pnpm...
corepack enable
if errorlevel 1 goto :failed
corepack prepare pnpm@10.33.0 --activate
if errorlevel 1 goto :failed

echo [2/6] Preparing Python virtual environment with uv...
if not exist ".venv\Scripts\python.exe" (
  uv venv .venv
  if errorlevel 1 goto :failed
)

echo [3/6] Installing backend dependencies with uv...
uv pip install --python ".venv\Scripts\python.exe" -r backend/requirements.txt
if errorlevel 1 goto :failed

echo [4/6] Installing frontend dependencies with pnpm...
cd /d "%~dp0frontend"
pnpm install --frozen-lockfile
if errorlevel 1 goto :failed

cd /d "%~dp0"
if not exist "backend\data" mkdir "backend\data"
if not exist "backend\runtime" mkdir "backend\runtime"

echo [5/6] Launching backend API in new terminal...
start "Snowflake Migrator Backend" cmd /k "cd /d \"%~dp0\" && .venv\Scripts\python.exe -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000"

echo [6/6] Launching frontend in new terminal...
start "Snowflake Migrator Frontend" cmd /k "cd /d \"%~dp0frontend\" && set NEXT_PUBLIC_API_BASE_URL=http://localhost:8000/api && pnpm dev --hostname 0.0.0.0 --port 3000"

timeout /t 3 /nobreak >nul
start "" "http://localhost:3000"

echo.
echo [DONE] Local stack started.
echo        Backend:  http://localhost:8000/api/health
echo        Frontend: http://localhost:3000
exit /b 0

:failed
echo.
echo [ERROR] Startup failed. Check messages above and retry.
pause
exit /b 1
