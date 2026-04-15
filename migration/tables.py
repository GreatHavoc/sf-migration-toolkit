import logging

logger = (
    logging.logger(__name__)
    if hasattr(logging, "logger")
    else logging.getLogger(__name__)
)
import re
from connection import exec_sql, exec_sql_with_cols, fetch_one_val
from discovery import get_table_columns, is_iceberg_table
from utils import (
    fq,
    stage_loc_literal,
    sql_string_literal,
    qident,
    rewrite_db_in_ddl,
    _find_col,
)


# --- FK discovery + ordering (copied from mainv2 to restore default FK-aware ordering) ---
def get_foreign_keys(conn, db: str, schema: str) -> dict:
    """
    Query INFORMATION_SCHEMA for foreign keys and return FK graph used for ordering.
    """
    try:
        exec_sql(conn, f"USE DATABASE {qident(db)}")
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    sql = """
        SELECT 
            tc.CONSTRAINT_NAME,
            tc.TABLE_NAME,
            kcu.COLUMN_NAME,
            ccu.TABLE_NAME AS REFERENCED_TABLE,
            ccu.COLUMN_NAME AS REFERENCED_COLUMN,
            ccu.TABLE_SCHEMA AS REFERENCED_SCHEMA,
            ccu.TABLE_DATABASE AS REFERENCED_DATABASE
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu
            ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
          AND tc.TABLE_SCHEMA = %s
    """
    try:
        rows = exec_sql(conn, sql, (schema,))
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        return {}

    fk_graph = {}
    for r in rows:
        child_table = r[1]
        fk_graph.setdefault(child_table, {"referenced_by": [], "references": []})
        fk_graph[child_table]["references"].append(
            {
                "constraint": r[0],
                "column": r[2],
                "referenced_table": r[3],
                "referenced_column": r[4],
                "referenced_schema": r[5],
                "referenced_database": r[6],
            }
        )

        parent = r[3]
        fk_graph.setdefault(parent, {"referenced_by": [], "references": []})
        fk_graph[parent]["referenced_by"].append(
            {
                "constraint": r[0],
                "child_table": child_table,
                "column": r[2],
                "referenced_column": r[4],
            }
        )

    return fk_graph


def resolve_table_data_order(table_names: list[str], fk_graph: dict) -> list[str]:
    """
    Resolve table order for data loading - parent tables before child tables.
    Uses FK graph to determine safe loading order (Kahn's algorithm).
    """
    if not table_names:
        return []

    deps = {t: set() for t in table_names}

    for t in table_names:
        t_info = fk_graph.get(t, {})
        for ref in t_info.get("references", []):
            parent = ref.get("referenced_table", "")
            if parent in table_names and parent != t:
                deps[t].add(parent)

    indeg = {t: 0 for t in table_names}
    for t, ds in deps.items():
        for parent in ds:
            indeg[parent] = indeg.get(parent, 0) + 1

    queue = [t for t, d in indeg.items() if d == 0]
    order = []

    while queue:
        current = queue.pop(0)
        order.append(current)
        for t, ds in deps.items():
            if current in ds:
                indeg[t] -= 1
                ds.discard(current)
                if indeg[t] == 0:
                    queue.append(t)

    if len(order) != len(table_names):
        # cycle detected or missing info — fall back to original list
        return table_names

    return order


def migrate_table_ddls(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate table DDL (structure without data). Skips Iceberg, external, and dynamic tables."""
    # Try SHOW TABLES first
    try:
        tables = exec_sql(src_conn, f"SHOW TABLES IN SCHEMA {fq(src_db, schema)}")
        table_names = [r[1] for r in tables if len(r) > 1]
        if table_names:
            pass  # Got tables successfully
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        table_names = []

    # Fallback to INFORMATION_SCHEMA if SHOW TABLES returned nothing
    if not table_names:
        try:
            rows = exec_sql(
                src_conn,
                f"SELECT TABLE_NAME FROM {src_db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_TYPE = 'BASE TABLE'",
            )
            table_names = [r[0] for r in rows if r[0]]
        except Exception as e:
            logger.warning(f"Ignored exception: {e}")
            table_names = []

    # Exclude dynamic tables - they will be migrated in DYNAMIC_TABLES phase
    try:
        dynamic_tables = exec_sql(
            src_conn, f"SHOW DYNAMIC TABLES IN SCHEMA {fq(src_db, schema)}"
        )
        dynamic_names = {r[1] for r in dynamic_tables if len(r) > 1}
        table_names = [t for t in table_names if t not in dynamic_names]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        pass  # No dynamic tables or error

    if not table_names:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0

    for t in table_names:
        if is_iceberg_table(src_conn, src_db, schema, t):
            errors.append(f"{t}: Skipped (Iceberg table)")
            continue

        ddl = fetch_one_val(
            src_conn, "SELECT GET_DDL('TABLE', %s, TRUE)", (f"{src_db}.{schema}.{t}",)
        )
        if not ddl:
            errors.append(f"{t}: Could not get DDL")
            continue

        if tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)

        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{t}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def migrate_table_data_ordered(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    stage_ref: str,
    run_id: str,
    dry_run: bool = False,
    tgt_warehouse: str = None,
) -> dict:
    """Migrate table data with FK-aware ordering. Skips Dynamic Tables (they have no data)."""
    # Get warehouse from source or target (required for COPY INTO)
    # Try source first, then target
    wh = tgt_warehouse
    if not wh:
        for conn, name in [(src_conn, "source"), (tgt_conn, "target")]:
            try:
                cur = conn.cursor()
                cur.execute("SELECT CURRENT_WAREHOUSE()")
                w = cur.fetchone()
                if w and w[0]:
                    wh = w[0]
                    logger.info(f"Auto-detected {name} warehouse: {wh}")
                    break
            except Exception as e:
                logger.warning(f"Failed to detect {name} warehouse: {e}")

    if wh:
        # Apply to both connections for the unload/load operations
        exec_sql(tgt_conn, f"USE WAREHOUSE {wh}")
        exec_sql(src_conn, f"USE WAREHOUSE {wh}")
    else:
        logger.error("No warehouse available - COPY INTO will fail")
    cols, tables = exec_sql_with_cols(
        src_conn, f"SHOW TABLES IN SCHEMA {fq(src_db, schema)}"
    )

    i_name = _find_col(cols, "name")
    i_is_external = _find_col(cols, "is_external")
    i_rows = _find_col(cols, "rows")

    if i_name is None:
        table_names = [r[1] for r in tables if len(r) > 1]
    else:
        table_names = [r[i_name] for r in tables]

    # Pre-compute iceberg and external tables, and grab row counts
    skip_tables = set()
    src_row_counts = {}
    for r in tables:
        if i_name is None or len(r) <= i_name:
            continue
        name = r[i_name]

        if i_rows is not None and len(r) > i_rows:
            try:
                src_row_counts[name] = int(r[i_rows])
            except:
                src_row_counts[name] = None

        # Check for EXTERNAL
        if i_is_external is not None and len(r) > i_is_external:
            if str(r[i_is_external]).upper() == "Y":
                skip_tables.add(name)
                continue

        # Check for ICEBERG across all columns (since SHOW TABLES structure can vary)
        row_str = " ".join(str(c).upper() for c in r if c is not None)
        if "ICEBERG" in row_str:
            skip_tables.add(name)

    # Exclude dynamic tables - they have no data to copy
    dynamic_tables = exec_sql(
        src_conn, f"SHOW DYNAMIC TABLES IN SCHEMA {fq(src_db, schema)}"
    )
    dynamic_names = {r[1] for r in dynamic_tables if len(r) > 1}
    table_names = [t for t in table_names if t not in dynamic_names]

    if not table_names:
        return {"migrated": 0, "errors": [], "validation": []}

    # Pre-fetch all columns for all tables in the schema (O(1) query instead of O(N) DESC TABLE)
    all_columns: dict[str, list[str]] = {}
    try:
        col_rows = exec_sql(
            src_conn,
            f"SELECT TABLE_NAME, COLUMN_NAME FROM {src_db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema.upper()}' ORDER BY ORDINAL_POSITION",
        )
        for r in col_rows:
            tname, cname = r[0], r[1]
            if tname not in all_columns:
                all_columns[tname] = []
            all_columns[tname].append(cname)
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        pass

    # --- Restore FK-aware ordering (default: always on) ---
    try:
        fk_graph = get_foreign_keys(src_conn, src_db, schema)
        ordered_tables = resolve_table_data_order(table_names, fk_graph)
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        ordered_tables = table_names

    errors = []
    migrated = 0
    validation_results = []

    for t in ordered_tables:
        if t in skip_tables:
            errors.append(f"{t}: Skipped (Iceberg or External table)")
            continue

        # Use pre-fetched columns, or fallback to DESC TABLE if missing
        cols = all_columns.get(t)
        if not cols:
            cols = get_table_columns(src_conn, src_db, schema, t)

        if not cols:
            continue

        # Get source row count before migration from SHOW TABLES metadata
        src_count = src_row_counts.get(t)

        path = f"data/{run_id}/{src_db}/{schema}/{t}/"
        unload_path = f"{stage_ref}/{path}"
        load_path = f"{stage_ref}/{path}"

        unload_sql = build_csv_unload_sql(unload_path, src_db, schema, t, cols)
        load_sql = build_csv_load_sql(load_path, tgt_db, tgt_schema, t, cols)

        if not dry_run:
            try:
                exec_sql(src_conn, unload_sql)
                exec_sql(tgt_conn, load_sql)
                exec_sql(src_conn, f"REMOVE {sql_string_literal(unload_path)}")

                # Validate row count on target
                tgt_count = None
                try:
                    tgt_count = exec_sql(
                        tgt_conn, f"SELECT COUNT(*) FROM {fq(tgt_db, tgt_schema, t)}"
                    )
                    tgt_count = tgt_count[0][0] if tgt_count else None
                except Exception as e:
                    logger.warning(f"Ignored exception: {e}")
                    pass

                if (
                    src_count is not None
                    and tgt_count is not None
                    and src_count != tgt_count
                ):
                    validation_results.append(
                        {
                            "table": t,
                            "src_count": src_count,
                            "tgt_count": tgt_count,
                            "status": "MISMATCH",
                        }
                    )
                elif src_count is not None and tgt_count is not None:
                    validation_results.append(
                        {
                            "table": t,
                            "src_count": src_count,
                            "tgt_count": tgt_count,
                            "status": "OK",
                        }
                    )

                migrated += 1
            except Exception as e:
                errors.append(f"{t}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors, "validation": validation_results}


def build_csv_unload_sql(
    unload_path: str, src_db: str, src_schema: str, table: str, columns: list[str]
) -> str:
    from utils import qident

    col_list = ", ".join(f"{qident(c)}" for c in columns)
    unload_loc = stage_loc_literal(unload_path)

    return f"""
COPY INTO {unload_loc}
FROM (SELECT {col_list} FROM {fq(src_db, src_schema, table)})
FILE_FORMAT = (
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = GZIP
)
OVERWRITE = TRUE;
""".strip()


def build_csv_load_sql(
    load_path: str, tgt_db: str, tgt_schema: str, table: str, columns: list[str]
) -> str:
    from utils import qident

    tgt_table = fq(tgt_db, tgt_schema, table)
    tgt_col_list = ", ".join(qident(c) for c in columns)
    sel_cols = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    from_loc = stage_loc_literal(load_path)

    return f"""
COPY INTO {tgt_table} ({tgt_col_list})
FROM (SELECT {sel_cols} FROM {from_loc})
FILE_FORMAT = (
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  COMPRESSION = GZIP
)
ON_ERROR = 'ABORT_STATEMENT';
""".strip()
