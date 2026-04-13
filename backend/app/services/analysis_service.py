"""Pre-migration analysis service wrappers."""

from __future__ import annotations

from dependencies import (
    build_table_dependency_order_from_views,
    validate_cross_db_dependencies,
)
from discovery import get_all_schemas, inventory_all_objects


def resolve_schemas(
    conn, source_db: str, requested_schemas: list[str] | None
) -> list[str]:
    available = get_all_schemas(conn, source_db)
    if requested_schemas:
        requested_upper = {s.upper() for s in requested_schemas}
        filtered = [s for s in available if s.upper() in requested_upper]
        return filtered
    return available


def run_precheck(conn, source_db: str, schemas: list[str]) -> dict:
    result = validate_cross_db_dependencies(conn, source_db, schemas)

    # Collect inventory summary
    inventory_summary = {}
    for schema in schemas:
        inv = inventory_all_objects(conn, source_db, schema)
        summary = inv.get("_summary", {})
        for key, count in summary.items():
            inventory_summary[key] = inventory_summary.get(key, 0) + count

    result["inventory_summary"] = inventory_summary
    return result


def compute_schema_order(conn, source_db: str, schemas: list[str]) -> list[str]:
    return build_table_dependency_order_from_views(conn, source_db, schemas)
