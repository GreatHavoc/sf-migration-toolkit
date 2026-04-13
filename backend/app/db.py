"""SQLite helpers for standalone job/event persistence."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .config import settings


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_data_dir() -> None:
    Path(settings.data_dir).mkdir(parents=True, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    ensure_data_dir()
    conn = sqlite3.connect(settings.database_path)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db_connection():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    ensure_data_dir()
    with db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_jobs (
                job_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                status TEXT NOT NULL,
                source_db TEXT NOT NULL,
                target_db TEXT NOT NULL,
                schemas_json TEXT NOT NULL,
                selected_phases_json TEXT NOT NULL,
                dry_run INTEGER NOT NULL,
                request_json TEXT NOT NULL,
                result_json TEXT,
                error_text TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                message TEXT NOT NULL,
                phase TEXT,
                count_value INTEGER,
                error_text TEXT,
                payload_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES migration_jobs(job_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_migration_events_job_id
            ON migration_events(job_id)
            """
        )


def dumps_json(data: object) -> str:
    return json.dumps(data, separators=(",", ":"), default=str)


def loads_json(value: str | None, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback
