# Snowflake Migration - Issues & Fixes Log

## Overview
List of critical bugs, performance issues, and UX problems encountered during development and how they were resolved.

---

## 🔴 Critical Bugs

### 1. Empty Database Dropdowns in Step 2
**Symptom:** Database dropdowns showing empty after successful connection test in Step 1.

**Root Cause:** Two issues:
1. Race condition in `DashboardContext.tsx` - "Next" button enabled BEFORE `listDatabases` completed
2. Silent failure when user role had no database access

**Fix:**
- Fixed validation in `handleTestBoth` to only enable buttons AFTER both test and list succeed
- Added backend validation in `connections.py` to raise HTTP 400 if no databases found

**Files:** `frontend/src/components/dashboard/DashboardContext.tsx`, `backend/app/api/connections.py`

---

### 2. Table Data Migration Hanging/Timeout
**Symptom:** Migration stuck on TABLE_DATA phase, browser shows "Event stream disconnected"

**Root Causes:**
1. SSE stream used blocking `time.sleep(1)` inside async FastAPI (blocked thread pool)
2. O(N²) performance: `is_iceberg_table()` called inside loop for EVERY table
3. Frontend EventSource not closed on job end (false auto-reconnect warning)

**Fixes:**
- Converted `stream_migration_events` to async with proper `Last-Event-ID` handling
- Refactored `migrate_table_data_ordered` to pre-fetch Iceberg/External table list ONCE
- Added `source.close()` on `stream.ended` in frontend API

**Files:** `backend/app/api/migrations.py`, `migration/tables.py`, `frontend/src/lib/api.ts`

---

### 3. Concurrent Connection Crashes
**Symptom:** MigrationWorker thread crashes mid-migration

**Root Cause:** Background worker shared connection with UI requests. Snowflake connector can't handle concurrent queries on same socket.

**Fix:** 
- Added per-connection `threading.RLock()` in `_CONN_CACHE`
- JobRunner now provisions FRESH isolated connections for migrations

**Files:** `backend/app/services/snowflake_service.py`, `backend/app/services/job_runner.py`

---

### 4. Procedurs Showing 0 migrated
**Symptom:** Procedures phase completes with 0 objects migrated

**Root Causes:**
1. Empty schemas array `[]` bypassed "migrate ALL schemas" fallback
2. Malformed GET_DDL identifier (missing db.schema prefix)

**Fixes:**
- Fixed `resolve_schemas` in `analysis_service.py` to treat empty array like `None`
- Fixed identifier format: `db.schema.name(args)` e.g., `CORTEX_CANVAS.PUBLIC.GET_TABLE_NAMES(VARCHAR, VARCHAR)`

**Files:** `backend/app/services/analysis_service.py`, `discovery.py`

---

## 🟡 Performance Issues

### 5. O(N²) SHOW TABLES in Table Data Phase
**Symptom:** Data migration extremely slow for large schemas

**Root Cause:** `DESC TABLE` and `SELECT COUNT(*)` executed for every table individually

**Fix:** Pre-fetch `INFORMATION_SCHEMA.COLUMNS` and use `rows` from `SHOW TABLES` metadata

**File:** `migration/tables.py`

---

### 6. Excluding System Functions/Procedures
**Symptom:** Inventory showing 1000+ built-in functions/procedures

**Root Cause:** No filter for `is_builtin` column from SHOW commands

**Fix:** Added `is_builtin == "Y"` check in discovery functions

**File:** `discovery.py`

---

## 🟢 UX/UI Fixes

### 7. Deprecated Antd Components
**Symptom:** Console warnings about deprecated antd components

**Fixes:**
- `<Alert message>` → `<Alert title>`
- `<List>` → `<Space>` + `<div>`
- `<Space direction="vertical">` → `<Space orientation="vertical">`

**Files:** `Step1Connections.tsx`, `Step2SetupAndAnalysis.tsx`, `Step4MonitorAndHistory.tsx`

---

### 8. Target DB Input
**Symptom:** User expected to create new DB, but dropdown showed existing databases

**Fix:** Changed Target DB from Select dropdown to Input text field. Auto-fills from Source DB but user can edit.

**File:** `Step2SetupAndAnalysis.tsx`

---

### 9. Live Monitor Overhaul
**Symptom:** Very long scroll page with stacked progress/timeline/history

**Fix:** Added Tabs (Progress vs Timeline), added max-height scroll containers (300px/400px/600px)

**File:** `Step4MonitorAndHistory.tsx`

---

### 10. LocalStorage Caching
**Symptom:** Stage settings lost on page refresh

**Fix:** Added localStorage persistence for:
- Source/Target connections (without passwords)
- Namespace config
- Stage config

**File:** `DashboardContext.tsx`

---

## 📝 Removed Features

### STREAMLITS Migration
**Action:** Removed STREAMLITS phase from orchestrator (no longer needed)

### Added STAGES Migration
**Action:** Created `migration/stages.py` and added STAGES phase between VIEWS and CORTEX_SEARCH

---

## 🔧 Syntax Fixes (Quick Reference)

| File | Issue | Fix |
|------|-------|-----|
| `orchestrator.py` | IndentationError after debug removal | Re-indent `rewrite_db = ...` |
| `discovery.py` | IndentationError in procedure block | Fixed try/except block structure |
| Multiple files | `except:` | Changed to `except Exception as e:` with logging |

---

*Generated: April 2026*