"""Connection test endpoint."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from connection import exec_sql

from ..schemas.connections import (
    ConnectionTestRequest,
    ConnectionTestResponse,
    ListDatabasesRequest,
    ListDatabasesResponse,
    ListSchemasRequest,
    ListSchemasResponse,
)
from ..services.snowflake_service import snowflake_connection
from discovery import get_all_schemas

router = APIRouter(prefix="/connections", tags=["connections"])


@router.post("/test", response_model=ConnectionTestResponse)
def test_connection(payload: ConnectionTestRequest) -> ConnectionTestResponse:
    try:
        with snowflake_connection(payload.connection) as conn:
            exec_sql(conn, "SELECT 1")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return ConnectionTestResponse(
        ok=True,
        account=payload.connection.account,
        user=payload.connection.user,
        role=payload.connection.role,
        warehouse=payload.connection.warehouse,
    )


@router.post("/databases", response_model=ListDatabasesResponse)
def list_databases_route(payload: ListDatabasesRequest) -> ListDatabasesResponse:
    try:
        from connection import exec_sql_with_cols
        from utils import _find_col

        with snowflake_connection(payload.connection) as conn:
            cols, rows = exec_sql_with_cols(conn, "SHOW DATABASES")
            i_name = _find_col(cols, "name")
            if i_name is None:
                # Fallback to index 1 if not found
                db_names = [row[1] for row in rows if len(row) > 1]
            else:
                db_names = [
                    row[i_name] for row in rows if len(row) > i_name and row[i_name]
                ]

            if not db_names:
                raise ValueError(
                    "No databases found or accessible. Check your role privileges."
                )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return ListDatabasesResponse(databases=db_names)


@router.post("/schemas", response_model=ListSchemasResponse)
def list_schemas_route(payload: ListSchemasRequest) -> ListSchemasResponse:
    try:
        with snowflake_connection(payload.connection) as conn:
            schema_names = get_all_schemas(conn, payload.database)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return ListSchemasResponse(schemas=schema_names)
