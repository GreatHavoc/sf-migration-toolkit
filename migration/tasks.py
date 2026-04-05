import re
from connection import exec_sql
from discovery import (
    list_streams,
    get_stream_ddl,
    list_tasks,
    get_task_ddl,
    list_alerts,
    get_alert_ddl,
    list_dynamic_tables,
    get_dynamic_table_ddl,
    list_pipes,
    get_pipe_ddl,
    get_task_predecessor_map,
)
from utils import rewrite_db_in_ddl


def migrate_streams(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate streams from source to target."""
    streams = list_streams(src_conn, src_db, schema)
    if not streams:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0

    for stream in streams:
        ddl = get_stream_ddl(src_conn, src_db, schema, stream)
        if not ddl:
            errors.append(f"{stream}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{stream}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def _resolve_task_order(tasks: list, get_ddl_func, src_conn, src_db, schema) -> list:
    """Resolve task creation order based on dependencies."""
    if not tasks:
        return tasks

    # Normalize task names for graph logic, keep original for execution order mapping.
    task_map = {t.upper(): t for t in tasks}
    task_keys = list(task_map.keys())

    # Build dependency graph from SHOW TASKS predecessors metadata
    pred_map = get_task_predecessor_map(src_conn, src_db, schema)
    deps = {t: set() for t in task_keys}
    for task_u in task_keys:
        preds = pred_map.get(task_u, set())
        for p in preds:
            p_u = str(p).upper()
            if p_u in task_map:
                deps[task_u].add(p_u)

    # Topological sort (Kahn's algorithm)
    indeg = {t: len(deps[t]) for t in task_keys}
    q = [t for t in task_keys if indeg[t] == 0]
    order = []

    while q:
        curr = q.pop(0)
        order.append(curr)
        for t in task_keys:
            if curr in deps[t]:
                indeg[t] -= 1
                if indeg[t] == 0:
                    q.append(t)

    # If cycle or unresolved, fallback to original order
    if len(order) != len(task_keys):
        return tasks
    return [task_map[k] for k in order]


def migrate_tasks(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate tasks from source to target with dependency ordering."""
    tasks = list_tasks(src_conn, src_db, schema)
    if not tasks:
        return {"migrated": 0, "errors": []}

    # Resolve task order based on dependencies
    ordered_tasks = _resolve_task_order(tasks, get_task_ddl, src_conn, src_db, schema)

    errors = []
    migrated = 0

    for task in ordered_tasks:
        ddl = get_task_ddl(src_conn, src_db, schema, task)
        if not ddl:
            errors.append(f"{task}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{task}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def migrate_alerts(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate alerts from source to target."""
    alerts = list_alerts(src_conn, src_db, schema)
    if not alerts:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0

    for alert in alerts:
        ddl = get_alert_ddl(src_conn, src_db, schema, alert)
        if not ddl:
            errors.append(f"{alert}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{alert}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def _get_table_refs_from_ddl(ddl: str) -> list:
    """Extract referenced tables from DDL."""
    if not ddl:
        return []
    import re

    refs = []
    # Match table references: schema.table or db.schema.table
    matches = re.findall(
        r"FROM\s+([A-Za-z0-9_\.]+)|JOIN\s+([A-Za-z0-9_\.]+)", ddl, re.IGNORECASE
    )
    for m in matches:
        for part in m:
            if part:
                parts = part.split(".")
                refs.append(parts[-1].upper())
    return refs


def _resolve_dynamic_table_order(
    dts: list, get_ddl_func, src_conn, src_db, schema
) -> list:
    """Resolve dynamic table creation order based on table dependencies."""
    if not dts:
        return dts

    # Build dependency graph
    deps = {dt: set() for dt in dts}
    for dt in dts:
        ddl = get_ddl_func(src_conn, src_db, schema, dt) or ""
        refs = _get_table_refs_from_ddl(ddl)
        # If this DT references another DT, add dependency
        for ref in refs:
            if ref in dts and ref != dt:
                deps[dt].add(ref)

    # Topological sort
    indeg = {dt: len(deps[dt]) for dt in dts}
    q = [dt for dt in dts if indeg[dt] == 0]
    order = []

    while q:
        curr = q.pop(0)
        order.append(curr)
        for dt in dts:
            if curr in deps[dt]:
                indeg[dt] -= 1
                if indeg[dt] == 0:
                    q.append(dt)

    return order if len(order) == len(dts) else dts


def migrate_dynamic_tables(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate dynamic tables from source to target with dependency ordering."""
    dts = list_dynamic_tables(src_conn, src_db, schema)
    if not dts:
        return {"migrated": 0, "errors": []}

    # Resolve order based on table dependencies
    ordered_dts = _resolve_dynamic_table_order(
        dts, get_dynamic_table_ddl, src_conn, src_db, schema
    )

    errors = []
    migrated = 0

    for dt in ordered_dts:
        ddl = get_dynamic_table_ddl(src_conn, src_db, schema, dt)
        if not ddl:
            errors.append(f"{dt}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{dt}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def migrate_pipes(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate pipes from source to target."""
    pipes = list_pipes(src_conn, src_db, schema)
    if not pipes:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0

    for pipe in pipes:
        ddl = get_pipe_ddl(src_conn, src_db, schema, pipe)
        if not ddl:
            errors.append(f"{pipe}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{pipe}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}
