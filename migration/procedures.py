import re
from connection import exec_sql
from discovery import (
    list_functions,
    get_function_ddl,
    has_external_handler,
    list_procedures,
    get_procedure_ddl,
)


def migrate_functions(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate UDFs from source to target. Skips functions with external handlers."""
    funcs = list_functions(src_conn, src_db, schema)
    if not funcs:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0
    skipped = []

    for func in funcs:
        ddl = get_function_ddl(src_conn, src_db, schema, func)
        if not ddl:
            # Could not retrieve DDL (permissions, signature mismatch, or
            # unsupported function type). Skip creation but don't treat as a
            # hard error so migration can continue for other objects.
            skipped.append(f"{func}: Could not get DDL")
            continue

        if has_external_handler(ddl):
            skipped.append(f"{func}: Skipped (external handler)")
            continue

        if tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)

        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{func}: {str(e)}")
        else:
            migrated += 1

    out = {"migrated": migrated, "errors": errors}
    if skipped:
        out["skipped"] = skipped
    return out


def migrate_procedures(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate stored procedures from source to target."""
    procs = list_procedures(src_conn, src_db, schema)
    if not procs:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0
    skipped = []

    for proc in procs:
        meta = get_procedure_ddl(src_conn, src_db, schema, proc)
        # meta is a dict: {'ddl','is_system','signature','error'}
        if not meta or meta.get("is_system"):
            skipped.append(f"{proc}: System or unmanaged procedure (skipped)")
            continue

        ddl = meta.get("ddl")
        if not ddl:
            skipped.append(f"{proc}: Could not get DDL ({meta.get('error')})")
            continue

        if tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)

        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{proc}: {str(e)}")
        else:
            migrated += 1

    out = {"migrated": migrated, "errors": errors}
    if skipped:
        out["skipped"] = skipped
    return out
