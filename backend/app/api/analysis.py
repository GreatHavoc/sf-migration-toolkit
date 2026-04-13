"""Pre-migration analysis endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..schemas.analysis import (
    AnalysisRequest,
    PrecheckResponse,
    SchemaOrderResponse,
)
from ..services.analysis_service import (
    compute_schema_order,
    resolve_schemas,
    run_precheck,
)
from ..services.snowflake_service import snowflake_connection

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.post("/precheck", response_model=PrecheckResponse)
def precheck_route(payload: AnalysisRequest) -> PrecheckResponse:
    try:
        with snowflake_connection(payload.connection) as conn:
            schemas = resolve_schemas(conn, payload.source_db, payload.schemas)
            result = run_precheck(conn, payload.source_db, schemas)
        return PrecheckResponse(
            source_db=payload.source_db,
            schemas=schemas,
            valid=bool(result.get("valid", False)),
            errors=[str(item) for item in result.get("errors", [])],
            warnings=[str(item) for item in result.get("warnings", [])],
            inventory_summary=result.get("inventory_summary", {}),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/schema-order", response_model=SchemaOrderResponse)
def schema_order_route(payload: AnalysisRequest) -> SchemaOrderResponse:
    try:
        with snowflake_connection(payload.connection) as conn:
            schemas = resolve_schemas(conn, payload.source_db, payload.schemas)
            ordered = compute_schema_order(conn, payload.source_db, schemas)
        return SchemaOrderResponse(
            source_db=payload.source_db,
            schemas=schemas,
            ordered_schemas=ordered,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
