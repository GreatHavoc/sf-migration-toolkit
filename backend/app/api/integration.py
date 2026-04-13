"""Integration/stage setup endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..schemas.integration import (
    EnsureIntegrationRequest,
    EnsureIntegrationResponse,
    EnsureStageRequest,
    EnsureStageResponse,
    InspectStageRequest,
    InspectStageResponse,
    ListStageRequest,
    ListStageResponse,
)
from ..services.integration_service import (
    build_validated_stage_url,
    ensure_integration,
    ensure_stage,
    inspect_stage,
    list_stage_entries,
)
from ..services.snowflake_service import snowflake_connection

router = APIRouter(prefix="/integration", tags=["integration"])


@router.post("/ensure", response_model=EnsureIntegrationResponse)
def ensure_integration_route(
    payload: EnsureIntegrationRequest,
) -> EnsureIntegrationResponse:
    try:
        stage_url = build_validated_stage_url(
            payload.stage.storage_account,
            payload.stage.container,
            payload.stage.prefix,
        )
        with snowflake_connection(payload.connection) as conn:
            ensure_integration(
                conn,
                integration_name=payload.integration_name,
                tenant_id=payload.azure_tenant_id,
                stage_url=stage_url,
            )
        return EnsureIntegrationResponse(
            ok=True,
            integration_name=payload.integration_name,
            stage_url=stage_url,
            message="Storage integration ensured",
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stage/ensure", response_model=EnsureStageResponse)
def ensure_stage_route(payload: EnsureStageRequest) -> EnsureStageResponse:
    try:
        stage_url = build_validated_stage_url(
            payload.stage.storage_account,
            payload.stage.container,
            payload.stage.prefix,
        )
        with snowflake_connection(payload.connection) as conn:
            stage_fqn = ensure_stage(
                conn,
                mig_db=payload.namespace.mig_db,
                mig_schema=payload.namespace.mig_schema,
                stage_name=payload.namespace.stage_name,
                stage_url=stage_url,
                integration_name=payload.integration_name,
            )
        return EnsureStageResponse(
            ok=True,
            stage_fqn=stage_fqn,
            stage_url=stage_url,
            integration_name=payload.integration_name,
            message="External stage ensured",
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stage/inspect", response_model=InspectStageResponse)
def inspect_stage_route(payload: InspectStageRequest) -> InspectStageResponse:
    try:
        with snowflake_connection(payload.connection) as conn:
            props = inspect_stage(
                conn,
                mig_db=payload.namespace.mig_db,
                mig_schema=payload.namespace.mig_schema,
                stage_name=payload.namespace.stage_name,
            )
        stage_fqn = (
            f"{payload.namespace.mig_db}."
            f"{payload.namespace.mig_schema}."
            f"{payload.namespace.stage_name}"
        )
        return InspectStageResponse(stage_fqn=stage_fqn, properties=props)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stage/list", response_model=ListStageResponse)
def list_stage_route(payload: ListStageRequest) -> ListStageResponse:
    try:
        with snowflake_connection(payload.connection) as conn:
            rows = list_stage_entries(
                conn,
                mig_db=payload.namespace.mig_db,
                mig_schema=payload.namespace.mig_schema,
                stage_name=payload.namespace.stage_name,
            )
        stage_fqn = (
            f"{payload.namespace.mig_db}."
            f"{payload.namespace.mig_schema}."
            f"{payload.namespace.stage_name}"
        )
        return ListStageResponse(stage_fqn=stage_fqn, rows=rows, count=len(rows))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
