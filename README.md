# Snowflake Migration Utility

Standalone, open-source Snowflake migration toolkit with a new web control plane:

- Backend: FastAPI (`backend/`)
- Frontend: Next.js App Router + TypeScript + Ant Design (`frontend/`)
- Migration engine: existing Python modules in repo root (`migration/`, `dependencies.py`, `discovery.py`, `utils.py`, `connection.py`)

This is a standalone FastAPI + Next.js architecture while keeping migration behavior and checkpoint/resume semantics.

---

## What is included

- **Guided Wizard UI:** Context-driven Next.js step flow (Connect → Setup → Precheck → Run → Monitor)
- **One-Click Infrastructure Setup:** Consolidates Azure integration and external stage validation into a single action
- **Unified Connection Testing:** Test source and target concurrently; MFA updates are frictionless
- Migration orchestration across schemas/phases with checkpoint files (`checkpoint_<run_id>.json`)
- Source/target Snowflake connection validation
- Azure integration + external stage setup and inspection
- Pre-migration analysis (dependency checks + schema ordering)
- Standalone long-running job runtime (in-process workers, no Redis/Celery)
- SQLite persistence for jobs/event history
- SSE event stream for live frontend monitoring

---

## Project structure

```text
backend/                 FastAPI app, job store, SSE APIs
frontend/                Next.js + Ant Design web UI
migration/               Migration phase implementations
connection.py            Snowflake connection helpers
dependencies.py          Dependency analysis + schema ordering
discovery.py             Snowflake object discovery + DDL helpers
utils.py                 Shared migration utility helpers
```

---

## Prerequisites

- Python 3.13+
- Node.js 20+
- `uv` (Python package/env manager)
- Corepack-enabled `pnpm` (frontend package manager)

---

## Setup

### 1) Python dependencies

Install root dependencies (migration engine + API deps) with `uv`:

```bash
uv pip install -r requirements.txt
```

Or install backend-only API dependencies:

```bash
uv pip install -r backend/requirements.txt
```

### 2) Frontend dependencies

```bash
cd frontend
corepack enable
corepack prepare pnpm@10.33.0 --activate
pnpm install
```

---

## Run locally

### One-click startup on Windows

From repo root, double-click or run either script:

- Docker mode: `run-local-docker.bat`
- Native mode (`uv` + `pnpm`): `run-local-native.bat`

Matching stop scripts:

- Docker mode stop: `stop-local-docker.bat`
- Native mode stop: `stop-local-native.bat`

Docker script starts Docker Desktop (if needed) and runs compose.
Native script installs dependencies (if needed), starts backend + frontend in separate terminals, and opens the browser.

### One-command startup (Docker Compose)

From repo root:

```bash
run-local-docker.bat
# or: docker compose up --build
```

Then open:

- Frontend: `http://localhost:3000`
- Backend API: `http://localhost:8000/api/health`

Data/runtime mounts used by compose:

- `./backend/data` -> SQLite job/event store (`app.db`)
- `./backend/runtime` -> runtime artifacts (including checkpoint files)

### Backend API

From repo root:

```bash
.venv\Scripts\python.exe -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
```

Alternative:

```bash
uv run uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000
```

API base URL: `http://localhost:8000/api`

### Frontend UI

From `frontend/`:

```bash
pnpm dev --hostname 0.0.0.0 --port 3000
```

Open `http://localhost:3000`.

If your backend is not on port 8000, set:

```bash
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000/api
```

---

## Key API endpoints (v1)

- `GET /api/health`
- `GET /api/health/defaults`
- `POST /api/connections/test`
- `POST /api/integration/ensure`
- `POST /api/integration/stage/ensure`
- `POST /api/integration/stage/inspect`
- `POST /api/integration/stage/list`
- `POST /api/analysis/precheck`
- `POST /api/analysis/schema-order`
- `POST /api/migrations/start`
- `POST /api/migrations/{job_id}/cancel`
- `POST /api/migrations/{job_id}/resume`
- `GET /api/migrations`
- `GET /api/migrations/{job_id}`
- `GET /api/migrations/{job_id}/events` (SSE)

---

## Notes

- This project is intentionally standalone: no Redis, no Celery, no managed queue.
- Job/event metadata persists locally in `backend/data/app.db`.
- Stage/setup validation is stricter than legacy UI input handling.

---

## License

MIT
