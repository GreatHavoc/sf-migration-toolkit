"""In-process migration job runtime with callback-based progress events."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from migration.orchestrator import migrate_all_objects
from utils import build_azure_stage_url

from ..constants import MIGRATION_PHASES, TERMINAL_JOB_STATES
from .analysis_service import resolve_schemas
from .integration_service import ensure_integration, ensure_stage
from .job_store import add_event, complete_job, create_job, get_job, update_job_status
from .snowflake_service import snowflake_connection
from .validation_service import (
    normalize_stage_prefix,
    validate_azure_tenant_id,
    validate_container_name,
    validate_storage_account_name,
)


@dataclass
class JobRuntime:
    future: Future
    cancel_requested: bool = False


class JobRunner:
    def __init__(self, max_workers: int = 2):
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="migration-job"
        )
        self._active_jobs: dict[str, JobRuntime] = {}

    def submit(self, request_data: dict[str, Any]) -> dict[str, str]:
        run_id = (request_data.get("run_id") or "").strip() or request_data.get(
            "generated_run_id"
        )
        if not run_id:
            raise ValueError("run_id is required")

        source_db = request_data["databases"]["source_db"]
        target_db = request_data["databases"]["target_db"]
        selected_phases = request_data.get("selected_phases") or MIGRATION_PHASES
        schemas = request_data.get("schemas") or []
        dry_run = bool(request_data.get("dry_run"))

        job = create_job(
            run_id=run_id,
            source_db=source_db,
            target_db=target_db,
            schemas=schemas,
            selected_phases=selected_phases,
            dry_run=dry_run,
            request_data=request_data,
        )

        add_event(
            job_id=job.job_id,
            run_id=run_id,
            event_type="job.queued",
            message="Migration job queued",
        )

        future = self._executor.submit(self._run_job, job.job_id)
        self._active_jobs[job.job_id] = JobRuntime(future=future)

        return {"job_id": job.job_id, "run_id": run_id, "status": "queued"}

    def request_cancel(self, job_id: str) -> dict[str, str]:
        job = get_job(job_id)
        if job.status in TERMINAL_JOB_STATES:
            return {
                "job_id": job.job_id,
                "run_id": job.run_id,
                "status": job.status,
                "message": "Job already finished",
            }

        runtime = self._active_jobs.get(job_id)
        if runtime:
            runtime.cancel_requested = True

        update_job_status(job_id, "cancel_requested")
        add_event(
            job_id=job.job_id,
            run_id=job.run_id,
            event_type="job.cancel_requested",
            message="Cancel requested",
        )
        return {
            "job_id": job.job_id,
            "run_id": job.run_id,
            "status": "cancel_requested",
            "message": "Cancellation requested",
        }

    def _run_job(self, job_id: str) -> None:
        job = get_job(job_id)
        request = job.request_data

        update_job_status(job_id, "running")
        add_event(
            job_id=job_id,
            run_id=job.run_id,
            event_type="job.started",
            message="Migration job started",
        )

        try:
            stage_cfg = request["stage"]
            namespace = request["namespace"]
            databases = request["databases"]

            storage_account = validate_storage_account_name(
                stage_cfg["storage_account"]
            )
            container = validate_container_name(stage_cfg["container"])
            tenant_id = validate_azure_tenant_id(stage_cfg["azure_tenant_id"])
            prefix = normalize_stage_prefix(stage_cfg.get("prefix") or "exports/")
            stage_url = build_azure_stage_url(storage_account, container, prefix)

            selected_phases = request.get("selected_phases") or MIGRATION_PHASES

            # Strip passcode from connection payloads before worker connects.
            # The UI's earlier authentication persists the MFA token in OS keyring.
            # A fresh connection without passcode will use that cached token.
            src_creds = {
                k: v for k, v in request["source_connection"].items() if k != "passcode"
            }
            tgt_creds = {
                k: v for k, v in request["target_connection"].items() if k != "passcode"
            }

            with (
                snowflake_connection(_Obj(src_creds)) as src_conn,
                snowflake_connection(_Obj(tgt_creds)) as tgt_conn,
            ):
                resolved_schemas = resolve_schemas(
                    src_conn,
                    databases["source_db"],
                    request.get("schemas") or [],
                )

                if not resolved_schemas:
                    raise RuntimeError("No schemas available for migration.")

                ensure_integration(
                    src_conn, request["stage"]["integration_name"], tenant_id, stage_url
                )
                ensure_integration(
                    tgt_conn, request["stage"]["integration_name"], tenant_id, stage_url
                )

                ensure_stage(
                    src_conn,
                    namespace["mig_db"],
                    namespace["mig_schema"],
                    namespace["stage_name"],
                    stage_url,
                    request["stage"]["integration_name"],
                )
                ensure_stage(
                    tgt_conn,
                    namespace["mig_db"],
                    namespace["mig_schema"],
                    namespace["stage_name"],
                    stage_url,
                    request["stage"]["integration_name"],
                )

                add_event(
                    job_id=job_id,
                    run_id=job.run_id,
                    event_type="job.prepared",
                    message="Integration and stage checks complete",
                    payload={"schemas": resolved_schemas},
                )

                stage_ref = f"@{namespace['mig_db']}.{namespace['mig_schema']}.{namespace['stage_name']}"

                result = migrate_all_objects(
                    src_conn=src_conn,
                    tgt_conn=tgt_conn,
                    src_db=databases["source_db"],
                    tgt_db=databases["target_db"],
                    schemas=resolved_schemas,
                    stage_ref=stage_ref,
                    run_id=job.run_id,
                    dry_run=bool(request.get("dry_run")),
                    init_streamlit_live=True,
                    tgt_query_wh=(
                        request["target_connection"].get("warehouse") or None
                    ),
                    nb_int_stage_name=request["namespace"].get("nb_int_stage_name")
                    or "NB_MIG_INT_STAGE",
                    stage_prefix=request["stage"].get("stage_prefix") or "sf_migration",
                    phase_callback=lambda phase, count, error: self._phase_callback(
                        job_id=job_id,
                        run_id=job.run_id,
                        phase=phase,
                        count=count,
                        error=error,
                    ),
                    selected_phases=selected_phases,
                )

            status = "succeeded"
            complete_job(job_id, status, result)
            add_event(
                job_id=job_id,
                run_id=job.run_id,
                event_type="job.completed",
                message="Migration job completed successfully",
                payload={
                    "total_migrated": result.get("total_migrated", 0),
                    "warnings": len(result.get("warnings", [])),
                    "errors": len(result.get("errors", [])),
                },
            )
        except Exception as exc:
            update_job_status(job_id, "failed", error_text=str(exc))
            add_event(
                job_id=job_id,
                run_id=job.run_id,
                event_type="job.failed",
                message="Migration job failed",
                error_text=str(exc),
            )
        finally:
            self._active_jobs.pop(job_id, None)

    def _phase_callback(
        self,
        *,
        job_id: str,
        run_id: str,
        phase: str,
        count: int,
        error: str | None,
    ) -> None:
        event_type = "phase.failed" if error else "phase.completed"
        message = f"{phase} failed" if error else f"{phase} completed"
        add_event(
            job_id=job_id,
            run_id=run_id,
            event_type=event_type,
            message=message,
            phase=phase,
            count_value=count,
            error_text=error,
        )


class _Obj:
    def __init__(self, data: dict[str, Any]):
        self.account = data.get("account")
        self.user = data.get("user")
        self.password = data.get("password")
        self.role = data.get("role")
        self.warehouse = data.get("warehouse")
        self.passcode = data.get("passcode")
