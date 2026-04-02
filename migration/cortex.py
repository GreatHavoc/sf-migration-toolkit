from connection import exec_sql
from discovery import (
    list_cortex_search_services,
    get_cortex_search_service_info,
    build_cortex_search_ddl,
)
from utils import rewrite_db_in_ddl


def migrate_cortex_search_services(
    src_conn,
    tgt_conn,
    src_db: str,
    src_schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
    rewrite_db: bool = False,
) -> dict:
    """Migrate Cortex Search services from source to target.

    GET_DDL does not support CORTEX_SEARCH_SERVICE, so DDL is reconstructed
    from SHOW CORTEX SEARCH SERVICES output columns.

    Services depend on base tables/views existing on target first.
    """
    services = list_cortex_search_services(src_conn, src_db, src_schema)
    if not services:
        return {"migrated": 0, "errors": [], "skipped": []}

    errors = []
    migrated = 0
    skipped = []

    for svc_name in services:
        info = get_cortex_search_service_info(src_conn, src_db, src_schema, svc_name)
        if not info:
            skipped.append(f"{svc_name}: Could not get metadata")
            continue

        ddl = build_cortex_search_ddl(info, tgt_db, tgt_schema)
        if not ddl:
            skipped.append(f"{svc_name}: Could not build DDL")
            continue

        if rewrite_db and tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)

        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{svc_name}: {str(e)}")
        else:
            migrated += 1

    out = {"migrated": migrated, "errors": errors}
    if skipped:
        out["skipped"] = skipped
    return out
