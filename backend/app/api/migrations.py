"""Migration job endpoints and SSE event stream."""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from ..constants import MIGRATION_PHASES
from ..schemas.jobs import (
    JobActionResponse,
    MigrationEvent,
    MigrationJobDetail,
    MigrationJobSummary,
    MigrationListResponse,
    MigrationRunRequest,
    MigrationRunResponse,
)
from ..services.job_runner import JobRunner
from ..services.job_store import get_events, get_job, list_jobs

router = APIRouter(prefix="/migrations", tags=["migrations"])
job_runner = JobRunner(max_workers=2)


def _to_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _to_summary(job) -> MigrationJobSummary:
    return MigrationJobSummary(
        job_id=job.job_id,
        run_id=job.run_id,
        status=job.status,
        source_db=job.source_db,
        target_db=job.target_db,
        dry_run=job.dry_run,
        created_at=_to_datetime(job.created_at),
        updated_at=_to_datetime(job.updated_at),
    )


@router.post("/start", response_model=MigrationRunResponse)
def start_migration(payload: MigrationRunRequest) -> MigrationRunResponse:
    try:
        request_data = payload.model_dump(mode="python")
        if not request_data.get("run_id"):
            request_data["generated_run_id"] = str(uuid.uuid4())[:8]
            request_data["run_id"] = request_data["generated_run_id"]

        selected_phases = request_data.get("selected_phases") or MIGRATION_PHASES
        request_data["selected_phases"] = selected_phases

        submitted = job_runner.submit(request_data)
        return MigrationRunResponse(
            run_id=submitted["run_id"],
            job_id=submitted["job_id"],
            status=submitted["status"],
            message="Migration job submitted",
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("", response_model=MigrationListResponse)
def list_migration_jobs(
    limit: int = Query(default=50, ge=1, le=200),
) -> MigrationListResponse:
    jobs = list_jobs(limit=limit)
    return MigrationListResponse(jobs=[_to_summary(job) for job in jobs])


@router.get("/{job_id}", response_model=MigrationJobDetail)
def get_migration_job(job_id: str) -> MigrationJobDetail:
    try:
        job = get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return MigrationJobDetail(
        job_id=job.job_id,
        run_id=job.run_id,
        status=job.status,
        source_db=job.source_db,
        target_db=job.target_db,
        dry_run=job.dry_run,
        created_at=_to_datetime(job.created_at),
        updated_at=_to_datetime(job.updated_at),
        schemas=job.schemas,
        selected_phases=job.selected_phases,
        error=job.error_text,
        result=job.result_data,
    )


@router.post("/{job_id}/cancel", response_model=JobActionResponse)
def cancel_migration(job_id: str) -> JobActionResponse:
    try:
        result = job_runner.request_cancel(job_id)
        return JobActionResponse(
            job_id=result["job_id"],
            run_id=result["run_id"],
            status=result["status"],
            message=result["message"],
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{job_id}/resume", response_model=JobActionResponse)
def resume_migration(job_id: str) -> JobActionResponse:
    try:
        job = get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if job.status == "running":
        return JobActionResponse(
            job_id=job.job_id,
            run_id=job.run_id,
            status=job.status,
            message="Job is already running",
        )

    request_data = dict(job.request_data)
    request_data["run_id"] = job.run_id
    try:
        submitted = job_runner.submit(request_data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JobActionResponse(
        job_id=submitted["job_id"],
        run_id=submitted["run_id"],
        status=submitted["status"],
        message="Migration resumed as a new job",
    )


@router.get("/{job_id}/events")
def stream_migration_events(
    job_id: str,
    after_event_id: int = Query(default=0, ge=0),
) -> StreamingResponse:
    try:
        job = get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    def event_generator():
        last_id = after_event_id
        while True:
            events = get_events(job_id, after_event_id=last_id, limit=200)
            for raw in events:
                event = MigrationEvent(
                    event_id=raw["event_id"],
                    job_id=raw["job_id"],
                    run_id=raw["run_id"],
                    event_type=raw["event_type"],
                    message=raw["message"],
                    phase=raw.get("phase"),
                    count=raw.get("count"),
                    error=raw.get("error"),
                    created_at=_to_datetime(raw["created_at"]),
                )
                last_id = event.event_id
                yield f"id: {event.event_id}\n"
                yield f"event: {event.event_type}\n"
                yield f"data: {event.model_dump_json()}\n\n"

            current = get_job(job_id)
            if current.status in {"succeeded", "failed", "cancelled"} and not events:
                terminal_payload = {
                    "job_id": current.job_id,
                    "run_id": current.run_id,
                    "status": current.status,
                }
                yield "event: stream.ended\n"
                yield f"data: {json.dumps(terminal_payload)}\n\n"
                break

            yield "event: stream.ping\n"
            yield f"data: {json.dumps({'job_id': job.job_id, 'last_event_id': last_id})}\n\n"
            time.sleep(1)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(
        event_generator(), media_type="text/event-stream", headers=headers
    )
