import re
from connection import exec_sql, exec_script
from discovery import (
    list_views,
    get_view_ddl,
    list_semantic_views,
    get_semantic_view_ddl,
    list_materialized_views,
    get_materialized_view_ddl,
)
from dependencies import _extract_fqns_from_sql


def _extract_view_refs(ddl: str, default_db: str, default_schema: str) -> set:
    """Return a set of tuples (db, schema, name) for objects referenced in ddl.

    This delegates to the sqlglot-based extractor in dependencies.py for
    robust parsing of identifiers (handles quoting, CTEs, etc.).
    """
    if not ddl:
        return set()
    fqns = _extract_fqns_from_sql(ddl, default_db, default_schema)
    return set(fqns)


def _obj_exists_on_target(tgt_conn, db: str, schema: str, name: str) -> bool:
    """Check via INFORMATION_SCHEMA if a table/view exists on the target.

    Uses TABLES and VIEWS metadata to avoid needing SELECT privileges on the object.
    Returns True if present, False otherwise.
    """
    try:
        # Normalize to upper for comparison
        db_u = db.upper()
        schema_u = schema.upper()
        name_u = name.upper()
        q_tables = f"SELECT 1 FROM {db_u}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema_u}' AND TABLE_NAME = '{name_u}' LIMIT 1"
        rows = exec_sql(tgt_conn, q_tables)
        if rows and len(rows) > 0:
            return True
        q_views = f"SELECT 1 FROM {db_u}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema_u}' AND TABLE_NAME = '{name_u}' LIMIT 1"
        rows = exec_sql(tgt_conn, q_views)
        if rows and len(rows) > 0:
            return True
    except Exception:
        # If the metadata query fails (permissions), treat as not existing
        return False
    return False


def resolve_view_order(
    view_names: list[str], ddls: dict, default_db: str, default_schema: str
) -> list[str]:
    """Topologically sort views by referenced view names where possible.

    Uses the sqlglot-based extraction to find referenced objects and builds a
    dependency graph between views (by name). If a cycle or unknown parse
    prevents a full order, return the original view_names as a safe fallback.
    """
    deps = {v: set() for v in view_names}
    name_set = set(view_names)
    for v in view_names:
        ddl = ddls.get(v) or ""
        refs = _extract_view_refs(ddl, default_db, default_schema)
        for db, sch, name in refs:
            if name in name_set and name != v:
                deps[v].add(name)

    indeg = {v: 0 for v in view_names}
    for v, ds in deps.items():
        for d in ds:
            indeg[d] += 1

    q = [v for v, d in indeg.items() if d == 0]
    order = []
    while q:
        n = q.pop(0)
        order.append(n)
        for m in view_names:
            if n in deps.get(m, set()):
                indeg[m] -= 1
                deps[m].discard(n)
                if indeg[m] == 0:
                    q.append(m)

    if len(order) != len(view_names):
        return view_names
    return order


def migrate_views(
    src_conn,
    tgt_conn,
    src_db: str,
    src_schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
    rewrite_db: bool = False,
) -> dict:
    views = list_views(src_conn, src_db, src_schema)
    if not views:
        return {"migrated": 0, "errors": []}

    errors = []
    ddls = {}
    for v in views:
        ddl = get_view_ddl(src_conn, src_db, src_schema, v) or ""
        if rewrite_db and tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
        ddls[v] = ddl

    order = resolve_view_order(views, ddls, src_db, src_schema)
    migrated = 0

    # We'll attempt to create views in iterative passes. If a view references
    # objects that are not yet present on the target (cross-schema refs), defer
    # creation until they are available. After a full pass with no progress,
    # remaining views are considered failures.
    pending = list(order)
    max_passes = max(3, len(pending))
    for _pass in range(max_passes):
        progress = False
        remaining = []
        for v in pending:
            ddl = ddls.get(v) or ""
            if not ddl:
                # nothing to create
                continue

            # extract referenced objects and verify existence on target
            refs = _extract_view_refs(ddl, src_db, src_schema)
            # set of view names being created in this schema
            name_set = set(views)
            missing = []
            for db_part, sch_part, obj_part in refs:
                # If the referenced object is another view in this schema that
                # we're creating, don't treat it as missing here — it will be
                # created in this iterative process (or is part of a cycle).
                if obj_part in name_set:
                    if db_part is None:
                        # schema.object form referring to same schema
                        if sch_part.upper() == src_schema.upper():
                            continue
                    else:
                        try:
                            if (
                                db_part.upper() == src_db.upper()
                                and sch_part.upper() == src_schema.upper()
                            ):
                                continue
                        except Exception:
                            pass

                if db_part:
                    # fully qualified in source; map DB if rewrite_db
                    tgt_db = None
                    if rewrite_db:
                        # caller supplies rewrite_db flag externally; here we
                        # don't have target DB variable — assume caller will
                        # pass full DDL already rewritten by orchestrator.
                        tgt_db = db_part
                    else:
                        tgt_db = db_part
                    if not _obj_exists_on_target(tgt_conn, tgt_db, sch_part, obj_part):
                        missing.append(f"{tgt_db}.{sch_part}.{obj_part}")
                else:
                    # schema.object form — assume source schema is in DDL and
                    # will be resolved relative to the target schema
                    # For safety, check both target schema and current schema
                    # of the DDL (we only have target schema parameter available)
                    if not _obj_exists_on_target(
                        tgt_conn, tgt_schema, sch_part, obj_part
                    ):
                        missing.append(f"{tgt_schema}.{sch_part}.{obj_part}")

            if missing:
                # still waiting on referenced objects
                remaining.append(v)
                continue

            # All references exist (or we couldn't detect missing); try to create
            if not dry_run:
                try:
                    exec_script(tgt_conn, ddl, remove_comments=True)
                    migrated += 1
                    progress = True
                except Exception as e:
                    # If creation fails due to missing object/authorization, defer
                    # and try in next pass; otherwise record the error.
                    msg = str(e)
                    if "does not exist or not authorized" in msg or "002003" in msg:
                        remaining.append(v)
                    else:
                        errors.append(f"{v}: {msg}")
            else:
                migrated += 1
                progress = True

        pending = remaining
        if not pending or not progress:
            break

    # Any remaining pending views are failures — report missing refs
    if pending:
        for v in pending:
            ddl = ddls.get(v) or ""
            refs = _extract_view_refs(ddl, src_db, src_schema)
            name_set = set(views)
            miss = []
            for db_part, sch_part, obj_part in refs:
                if obj_part in name_set:
                    if db_part is None:
                        if sch_part.upper() == src_schema.upper():
                            continue
                    else:
                        try:
                            if (
                                db_part.upper() == src_db.upper()
                                and sch_part.upper() == src_schema.upper()
                            ):
                                continue
                        except Exception:
                            pass

                if db_part:
                    tgt_db = db_part
                    if not _obj_exists_on_target(tgt_conn, tgt_db, sch_part, obj_part):
                        miss.append(f"{tgt_db}.{sch_part}.{obj_part}")
                else:
                    if not _obj_exists_on_target(
                        tgt_conn, tgt_schema, sch_part, obj_part
                    ):
                        miss.append(f"{tgt_schema}.{sch_part}.{obj_part}")
            if miss:
                errors.append(
                    f"{v}: Missing or unauthorized referenced objects: {', '.join(miss)}"
                )
            else:
                errors.append(f"{v}: Could not create view (unknown error)")

    return {"migrated": migrated, "errors": errors}


def migrate_semantic_views(
    src_conn,
    tgt_conn,
    src_db: str,
    src_schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
    rewrite_db: bool = False,
) -> dict:
    svs = list_semantic_views(src_conn, src_db, src_schema)
    if not svs:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0

    for sv in svs:
        ddl = get_semantic_view_ddl(src_conn, src_db, src_schema, sv)
        if not ddl:
            errors.append(f"{sv}: Could not get DDL")
            continue
        if rewrite_db and tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
        if not dry_run:
            try:
                exec_script(tgt_conn, ddl, remove_comments=True)
                migrated += 1
            except Exception as e:
                errors.append(f"{sv}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def migrate_materialized_views(
    src_conn,
    tgt_conn,
    src_db: str,
    src_schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
    rewrite_db: bool = False,
) -> dict:
    mvs = list_materialized_views(src_conn, src_db, src_schema)
    if not mvs:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0

    for mv in mvs:
        ddl = get_materialized_view_ddl(src_conn, src_db, src_schema, mv)
        if not ddl:
            errors.append(f"{mv}: Could not get DDL")
            continue
        if rewrite_db and tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{mv}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}
