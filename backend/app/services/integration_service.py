"""Integration and stage orchestration service."""

from __future__ import annotations

from connection import exec_sql
from utils import (
    build_azure_stage_url,
    ensure_external_stage_azure,
    ensure_session_namespace,
    ensure_storage_integration_azure,
    describe_stage,
)

from .validation_service import (
    validate_azure_tenant_id,
    validate_container_name,
    validate_storage_account_name,
    normalize_stage_prefix,
)


def build_validated_stage_url(storage_account: str, container: str, prefix: str) -> str:
    account = validate_storage_account_name(storage_account)
    container_name = validate_container_name(container)
    normalized_prefix = normalize_stage_prefix(prefix)
    return build_azure_stage_url(account, container_name, normalized_prefix)


def ensure_integration(
    conn, integration_name: str, tenant_id: str, stage_url: str
) -> None:
    valid_tenant_id = validate_azure_tenant_id(tenant_id)
    ensure_storage_integration_azure(
        conn,
        integration_name=integration_name,
        tenant_id=valid_tenant_id,
        allowed_locations=[stage_url],
    )


def ensure_stage(
    conn,
    mig_db: str,
    mig_schema: str,
    stage_name: str,
    stage_url: str,
    integration_name: str,
) -> str:
    ensure_external_stage_azure(
        conn,
        mig_db=mig_db,
        mig_schema=mig_schema,
        stage_name=stage_name,
        stage_url=stage_url,
        integration_name=integration_name,
    )
    return f"{mig_db}.{mig_schema}.{stage_name}"


def inspect_stage(
    conn, mig_db: str, mig_schema: str, stage_name: str
) -> dict[str, str]:
    props = describe_stage(conn, mig_db, mig_schema, stage_name)
    out: dict[str, str] = {}
    for key, value in props.items():
        out[str(key)] = "" if value is None else str(value)
    return out


def list_stage_entries(
    conn, mig_db: str, mig_schema: str, stage_name: str
) -> list[list[str]]:
    ensure_session_namespace(conn, mig_db, mig_schema)
    rows = exec_sql(conn, f"LIST @{stage_name}")
    normalized: list[list[str]] = []
    for row in rows:
        normalized.append(["" if cell is None else str(cell) for cell in row])
    return normalized
