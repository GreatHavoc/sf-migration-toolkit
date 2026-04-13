"""Persistence operations for migration jobs and events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import uuid

from ..constants import TERMINAL_JOB_STATES
from ..db import db_connection, dumps_json, loads_json, utc_now_iso


@dataclass(frozen=True)
class StoredJob:
    job_id: str
    run_id: str
    status: str
    source_db: str
    target_db: str
    schemas: list[str]
    selected_phases: list[str]
    dry_run: bool
    request_data: dict[str, Any]
    result_data: dict[str, Any] | None
    error_text: str | None
    created_at: str
    updated_at: str


def create_job(
    *,
    run_id: str,
    source_db: str,
    target_db: str,
    schemas: list[str],
    selected_phases: list[str],
    dry_run: bool,
    request_data: dict[str, Any],
) -> StoredJob:
    now = utc_now_iso()
    job_id = str(uuid.uuid4())
    with db_connection() as conn:
        conn.execute(
            """
            INSERT INTO migration_jobs (
                job_id,
                run_id,
                status,
                source_db,
                target_db,
                schemas_json,
                selected_phases_json,
                dry_run,
                request_json,
                result_json,
                error_text,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                run_id,
                "queued",
                source_db,
                target_db,
                dumps_json(schemas),
                dumps_json(selected_phases),
                int(dry_run),
                dumps_json(request_data),
                None,
                None,
                now,
                now,
            ),
        )
    return get_job(job_id)


def _row_to_job(row) -> StoredJob:
    return StoredJob(
        job_id=row["job_id"],
        run_id=row["run_id"],
        status=row["status"],
        source_db=row["source_db"],
        target_db=row["target_db"],
        schemas=loads_json(row["schemas_json"], []),
        selected_phases=loads_json(row["selected_phases_json"], []),
        dry_run=bool(row["dry_run"]),
        request_data=loads_json(row["request_json"], {}),
        result_data=loads_json(row["result_json"], None),
        error_text=row["error_text"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def get_job(job_id: str) -> StoredJob:
    with db_connection() as conn:
        row = conn.execute(
            "SELECT * FROM migration_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
    if not row:
        raise KeyError(f"Job not found: {job_id}")
    return _row_to_job(row)


def list_jobs(limit: int = 50) -> list[StoredJob]:
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM migration_jobs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_row_to_job(r) for r in rows]


def update_job_status(job_id: str, status: str, error_text: str | None = None) -> None:
    now = utc_now_iso()
    with db_connection() as conn:
        conn.execute(
            """
            UPDATE migration_jobs
            SET status = ?, error_text = ?, updated_at = ?
            WHERE job_id = ?
            """,
            (status, error_text, now, job_id),
        )


def complete_job(job_id: str, status: str, result_data: dict[str, Any]) -> None:
    if status not in TERMINAL_JOB_STATES:
        raise ValueError("complete_job requires a terminal status")
    now = utc_now_iso()
    with db_connection() as conn:
        conn.execute(
            """
            UPDATE migration_jobs
            SET status = ?, result_json = ?, updated_at = ?
            WHERE job_id = ?
            """,
            (status, dumps_json(result_data), now, job_id),
        )


def add_event(
    *,
    job_id: str,
    run_id: str,
    event_type: str,
    message: str,
    phase: str | None = None,
    count_value: int | None = None,
    error_text: str | None = None,
    payload: dict[str, Any] | None = None,
) -> int:
    now = utc_now_iso()
    payload_json = dumps_json(payload) if payload is not None else None
    with db_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO migration_events (
                job_id,
                run_id,
                event_type,
                message,
                phase,
                count_value,
                error_text,
                payload_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                run_id,
                event_type,
                message,
                phase,
                count_value,
                error_text,
                payload_json,
                now,
            ),
        )
        return int(cur.lastrowid)


def get_events(
    job_id: str, after_event_id: int = 0, limit: int = 200
) -> list[dict[str, Any]]:
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT event_id, job_id, run_id, event_type, message, phase, count_value,
                   error_text, payload_json, created_at
            FROM migration_events
            WHERE job_id = ? AND event_id > ?
            ORDER BY event_id ASC
            LIMIT ?
            """,
            (job_id, after_event_id, limit),
        ).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "event_id": int(row["event_id"]),
                "job_id": row["job_id"],
                "run_id": row["run_id"],
                "event_type": row["event_type"],
                "message": row["message"],
                "phase": row["phase"],
                "count": row["count_value"],
                "error": row["error_text"],
                "payload": loads_json(row["payload_json"], None),
                "created_at": row["created_at"],
            }
        )
    return out
