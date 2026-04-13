"""Job and migration run schemas."""

from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field
from .connections import SnowflakeConnectionPayload


class MigrationNamespace(BaseModel):
    mig_db: str = Field(min_length=1)
    mig_schema: str = Field(min_length=1)
    stage_name: str = Field(min_length=1)


class MigrationSourceTarget(BaseModel):
    source_db: str = Field(min_length=1)
    target_db: str = Field(min_length=1)


class MigrationStageConfig(BaseModel):
    integration_name: str = Field(min_length=1)
    azure_tenant_id: str = Field(min_length=1)
    storage_account: str = Field(min_length=1)
    container: str = Field(min_length=1)
    prefix: str = "exports/"
    stage_prefix: str = "sf_migration"


class MigrationRunRequest(BaseModel):
    source_connection: "SnowflakeConnectionPayload"
    target_connection: "SnowflakeConnectionPayload"
    namespace: MigrationNamespace
    databases: MigrationSourceTarget
    stage: MigrationStageConfig
    schemas: list[str] | None = None
    selected_phases: list[str] | None = None
    run_id: str | None = None
    dry_run: bool = False


class MigrationRunResponse(BaseModel):
    run_id: str
    job_id: str
    status: str
    message: str


class MigrationEvent(BaseModel):
    event_id: int
    job_id: str
    run_id: str
    event_type: str
    message: str
    phase: str | None = None
    count: int | None = None
    error: str | None = None
    created_at: datetime


class MigrationJobSummary(BaseModel):
    job_id: str
    run_id: str
    status: str
    source_db: str
    target_db: str
    dry_run: bool
    created_at: datetime
    updated_at: datetime


class MigrationJobDetail(MigrationJobSummary):
    schemas: list[str]
    selected_phases: list[str]
    error: str | None = None
    result: dict | None = None


class MigrationListResponse(BaseModel):
    jobs: list[MigrationJobSummary]


class JobActionResponse(BaseModel):
    job_id: str
    run_id: str
    status: str
    message: str
