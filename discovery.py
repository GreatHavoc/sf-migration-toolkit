import logging
import sys

logger = (
    logging.logger(__name__)
    if hasattr(logging, "logger")
    else logging.getLogger(__name__)
)


def log_debug(msg):
    """Log to both logger and stdout for debugging"""
    print(f"[DEBUG] {msg}", file=sys.stderr)
    logger.info(msg)


from typing import Optional
import json
import ast
from connection import exec_sql, exec_sql_with_cols, fetch_one_val
from utils import fq, _find_col


def get_all_schemas(conn, db: str) -> list[str]:
    """Get all schemas in a database, excluding system schemas."""
    from config import SKIP_SCHEMAS

    rows = exec_sql(conn, f"SHOW SCHEMAS IN DATABASE {db}")
    schemas = [r[1] for r in rows if len(r) > 1]
    return [s for s in schemas if s and s.upper() not in SKIP_SCHEMAS]


def list_tables(conn, db: str, schema: str) -> list[str]:
    """List all tables in a schema."""
    rows = exec_sql(conn, f"SHOW TABLES IN SCHEMA {fq(db, schema)}")
    return [r[1] for r in rows if len(r) > 1 and r[1]]


def inventory_all_objects(conn, db: str, schema: str) -> dict:
    """Discover all objects in a schema. Returns comprehensive inventory."""
    inventory = {
        "tables": [],
        "views": [],
        "materialized_views": [],
        "semantic_views": [],
        "sequences": [],
        "file_formats": [],
        "stages": [],
        "streams": [],
        "pipes": [],
        "tasks": [],
        "procedures": [],
        "functions": [],
        "tags": [],
        "masking_policies": [],
        "row_access_policies": [],
        "session_policies": [],
        "dynamic_tables": [],
        "external_tables": [],
        "alerts": [],
        "cortex_search_services": [],
    }

    try:
        tables = exec_sql(conn, f"SHOW TABLES IN SCHEMA {fq(db, schema)}")
        inventory["tables"] = [r[1] for r in tables if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        views = exec_sql(conn, f"SHOW VIEWS IN SCHEMA {fq(db, schema)}")
        inventory["views"] = [r[1] for r in views if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        mvs = exec_sql(conn, f"SHOW MATERIALIZED VIEWS IN SCHEMA {fq(db, schema)}")
        inventory["materialized_views"] = [r[1] for r in mvs if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        svs = exec_sql(conn, f"SHOW SEMANTIC VIEWS IN SCHEMA {fq(db, schema)}")
        inventory["semantic_views"] = [r[1] for r in svs if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        seqs = exec_sql(conn, f"SHOW SEQUENCES IN SCHEMA {fq(db, schema)}")
        inventory["sequences"] = [r[1] for r in seqs if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        ffs = exec_sql(conn, f"SHOW FILE FORMATS IN SCHEMA {fq(db, schema)}")
        inventory["file_formats"] = [r[1] for r in ffs if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        stages = exec_sql(conn, f"SHOW STAGES IN SCHEMA {fq(db, schema)}")
        inventory["stages"] = [r[1] for r in stages if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        streams = exec_sql(conn, f"SHOW STREAMS IN SCHEMA {fq(db, schema)}")
        inventory["streams"] = [r[1] for r in streams if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        pipes = exec_sql(conn, f"SHOW PIPES IN SCHEMA {fq(db, schema)}")
        inventory["pipes"] = [r[1] for r in pipes if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        task = exec_sql(conn, f"SHOW TASKS IN SCHEMA {fq(db, schema)}")
        inventory["tasks"] = [r[1] for r in task if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        cols, procs = exec_sql_with_cols(
            conn, f"SHOW PROCEDURES IN SCHEMA {fq(db, schema)}"
        )
        log_debug(f"SHOW PROCEDURES columns: {cols}")
        log_debug(f"SHOW PROCEDURES total rows: {len(procs)}")
        i_name = _find_col(cols, "name")
        i_builtin = _find_col(cols, "is_builtin")
        log_debug(f"Column indices: name={i_name}, is_builtin={i_builtin}")
        if i_name is not None:
            valid_procs = []
            for r in procs:
                if len(r) > i_name and r[i_name]:
                    is_builtin = False
                    if i_builtin is not None and len(r) > i_builtin:
                        val = str(r[i_builtin]).upper() if r[i_builtin] else ""
                        if val == "Y":
                            is_builtin = True
                        log_debug(f"  Proc: {r[i_name]}, is_builtin={val}")
                    if not is_builtin:
                        valid_procs.append(r[i_name])
            log_debug(f"Found {len(valid_procs)} user procedures in {db}.{schema}")
            log_debug(f"User procedure names: {valid_procs}")
            inventory["procedures"] = valid_procs
        else:
            log_debug("Could not find 'name' column")
    except Exception as e:
        logger.warning(f"Ignored procedures exception: {e}")

    try:
        cols, funcs = exec_sql_with_cols(
            conn, f"SHOW FUNCTIONS IN SCHEMA {fq(db, schema)}"
        )
        i_name = _find_col(cols, "name")
        i_builtin = _find_col(cols, "is_builtin")
        if i_name is not None:
            valid_funcs = []
            for r in funcs:
                if len(r) > i_name and r[i_name]:
                    if (
                        i_builtin is not None
                        and len(r) > i_builtin
                        and str(r[i_builtin]).upper() == "Y"
                    ):
                        continue
                    valid_funcs.append(r[i_name])
            inventory["functions"] = valid_funcs
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        tags = exec_sql(conn, f"SHOW TAGS IN SCHEMA {fq(db, schema)}")
        inventory["tags"] = [r[1] for r in tags if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        mp = exec_sql(conn, f"SHOW MASKING POLICIES IN SCHEMA {fq(db, schema)}")
        inventory["masking_policies"] = [r[1] for r in mp if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        rap = exec_sql(conn, f"SHOW ROW ACCESS POLICIES IN SCHEMA {fq(db, schema)}")
        inventory["row_access_policies"] = [r[1] for r in rap if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        dt = exec_sql(conn, f"SHOW DYNAMIC TABLES IN SCHEMA {fq(db, schema)}")
        inventory["dynamic_tables"] = [r[1] for r in dt if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        alerts = exec_sql(conn, f"SHOW ALERTS IN SCHEMA {fq(db, schema)}")
        inventory["alerts"] = [r[1] for r in alerts if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        ext = exec_sql(conn, f"SHOW EXTERNAL TABLES IN SCHEMA {fq(db, schema)}")
        inventory["external_tables"] = [r[1] for r in ext if len(r) > 1]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    try:
        cols, rows = exec_sql_with_cols(conn, "SHOW CORTEX SEARCH SERVICES")
        i_name = _find_col(cols, "name")
        i_db = _find_col(cols, "database_name")
        i_schema = _find_col(cols, "schema_name")
        if i_name is not None and i_db is not None and i_schema is not None:
            db_u = db.upper()
            schema_u = schema.upper()
            for r in rows:
                if len(r) <= max(i_name, i_db, i_schema):
                    continue
                if (
                    str(r[i_db]).upper() == db_u
                    and str(r[i_schema]).upper() == schema_u
                ):
                    if r[i_name]:
                        inventory["cortex_search_services"].append(r[i_name])
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")

    total = sum(len(v) for v in inventory.values())
    inventory["_summary"] = {
        "tables": len(inventory["tables"]),
        "views": len(inventory["views"]),
        "sequences": len(inventory["sequences"]),
        "procedures": len(inventory["procedures"]),
        "functions": len(inventory["functions"]),
        "tasks": len(inventory["tasks"]),
        "streams": len(inventory["streams"]),
        "total": total,
    }

    return inventory


def list_sequences(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW SEQUENCES IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_sequence_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('SEQUENCE', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


def list_file_formats(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(
        conn, f"SHOW FILE FORMATS IN SCHEMA {fq(db, schema)}"
    )
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_file_format_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('FILE_FORMAT', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


def list_tags(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW TAGS IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_tag_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('TAG', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


def list_streams(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW STREAMS IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_stream_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('STREAM', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


def list_tasks(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW TASKS IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_task_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('TASK', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


def _task_name_only(task_ref: str) -> str:
    """Normalize a task reference and return object name only."""
    if not task_ref:
        return ""
    parts = [p.strip('"') for p in str(task_ref).split(".") if p]
    return (parts[-1] if parts else str(task_ref)).upper()


def get_task_predecessor_map(conn, db: str, schema: str) -> dict[str, set[str]]:
    """Return task -> predecessor names mapping from SHOW TASKS metadata."""
    out: dict[str, set[str]] = {}
    try:
        cols, rows = exec_sql_with_cols(conn, f"SHOW TASKS IN SCHEMA {fq(db, schema)}")
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        return out

    i_name = _find_col(cols, "name")
    i_preds = _find_col(cols, "predecessors")
    if i_name is None:
        return out

    for r in rows:
        if len(r) <= i_name or not r[i_name]:
            continue
        tname = str(r[i_name]).upper()
        out.setdefault(tname, set())
        if i_preds is None or len(r) <= i_preds or not r[i_preds]:
            continue

        preds_raw = r[i_preds]
        parsed = []
        if isinstance(preds_raw, list):
            parsed = preds_raw
        elif isinstance(preds_raw, str):
            s = preds_raw.strip()
            try:
                parsed = json.loads(s)
            except Exception as e:
                logger.warning(f"Ignored exception: {e}")
                try:
                    parsed = ast.literal_eval(s)
                except Exception as e:
                    logger.warning(f"Ignored exception: {e}")
                    parsed = []

        if isinstance(parsed, list):
            for p in parsed:
                pn = _task_name_only(p)
                if pn:
                    out[tname].add(pn)

    return out


def list_procedures(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW PROCEDURES IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    i_builtin = _find_col(cols, "is_builtin")
    i_signature = _find_col(cols, "signature")  # Also get signature for logging
    logger.info(
        f"SHOW PROCEDURES: name_idx={i_name}, builtin_idx={i_builtin}, sig_idx={i_signature}"
    )
    if i_name is None:
        return []
    valid = []
    for r in rows:
        if len(r) > i_name and r[i_name]:
            is_b = False
            if i_builtin is not None and len(r) > i_builtin:
                val = str(r[i_builtin]).upper() if r[i_builtin] else ""
                if val == "Y":
                    is_b = True
            if not is_b:
                valid.append(r[i_name])
    logger.info(f"User procedures found by list_procedures: {valid}")
    return valid


def get_procedure_ddl(conn, db: str, schema: str, name: str) -> dict:
    """Return metadata for a procedure: {'ddl': str|None, 'is_system': bool, 'signature': str|None, 'error': str|None}

    Uses SHOW PROCEDURES to locate the procedure row (so we can obtain the signature
    required for overloaded procedures) and attempts GET_DDL with the signature when available.
    """
    out = {"ddl": None, "is_system": False, "signature": None, "error": None}
    try:
        cols, rows = exec_sql_with_cols(
            conn, f"SHOW PROCEDURES IN SCHEMA {fq(db, schema)}"
        )
        i_name = _find_col(cols, "name")
        i_builtin = _find_col(cols, "is_builtin")
        # signature/arguments column names vary across Snowflake versions
        i_sig = (
            _find_col(cols, "signature")
            or _find_col(cols, "arguments")
            or _find_col(cols, "arguments_text")
        )
        # owner/creator column (heuristic to detect system objects)
        i_owner = (
            _find_col(cols, "owner")
            or _find_col(cols, "created_by")
            or _find_col(cols, "creator")
        )

        target_name = str(name).upper()
        for r in rows:
            pname = r[i_name] if (i_name is not None and len(r) > i_name) else None
            if not pname:
                continue
            try:
                pname_str = str(pname).upper()
            except Exception as e:
                logger.warning(f"Ignored exception: {e}")
                pname_str = None
            if pname_str != target_name:
                continue

            sig = None
            if i_sig is not None and len(r) > i_sig and r[i_sig]:
                sig = str(r[i_sig]).strip()
                out["signature"] = sig

            owner = None
            if i_owner is not None and len(r) > i_owner and r[i_owner]:
                owner = str(r[i_owner]).strip()

            if (
                i_builtin is not None
                and len(r) > i_builtin
                and str(r[i_builtin]).upper() == "Y"
            ):
                out["is_system"] = True
            # Heuristic: if owner/creator mentions SNOWFLAKE or system-like, mark as system
            elif owner and ("SNOWFLAKE" in owner.upper() or "SYSTEM" in owner.upper()):
                out["is_system"] = True

            # Format: GET_DDL('PROCEDURE', 'db.schema.name(arg1, arg2)')
            if sig:
                # Get just the arguments part inside parens
                start = sig.find("(")
                end = sig.rfind(")")
                if start != -1 and end != -1 and start < end:
                    args = sig[start + 1 : end]
                    ident = f"{db}.{schema}.{name}({args})"
                else:
                    ident = f"{db}.{schema}.{name}"
            else:
                ident = f"{db}.{schema}.{name}"

            try:
                out["ddl"] = fetch_one_val(
                    conn, "SELECT GET_DDL('PROCEDURE', %s, TRUE)", (ident,)
                )
            except Exception as e:
                logger.warning(f"Ignored exception: {e}")
                # fallback: try without signature
                try:
                    out["ddl"] = fetch_one_val(
                        conn,
                        "SELECT GET_DDL('PROCEDURE', %s, TRUE)",
                        (f"{db}.{schema}.{name}",),
                    )
                except Exception as e:
                    out["error"] = str(e)
                    out["ddl"] = None

            return out

        # not found in SHOW results: try a direct GET_DDL as a last resort
        try:
            out["ddl"] = fetch_one_val(
                conn,
                "SELECT GET_DDL('PROCEDURE', %s, TRUE)",
                (f"{db}.{schema}.{name}",),
            )
            return out
        except Exception as e:
            out["error"] = str(e)
            return out

    except Exception as e:
        out["error"] = str(e)
        return out


def list_functions(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW FUNCTIONS IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    i_builtin = _find_col(cols, "is_builtin")
    if i_name is None:
        return []
    valid = []
    for r in rows:
        if len(r) > i_name and r[i_name]:
            if (
                i_builtin is not None
                and len(r) > i_builtin
                and str(r[i_builtin]).upper() == "Y"
            ):
                continue
            valid.append(r[i_name])
    return valid


def get_function_ddl(conn, db: str, schema: str, name: str) -> dict:
    """Return metadata for a function: {'ddl': str|None, 'signature': str|None, 'error': str|None}.

    Uses SHOW FUNCTIONS to locate signature/arguments for overloaded functions,
    then GET_DDL with signature when available.
    """
    out = {"ddl": None, "signature": None, "error": None}
    try:
        cols, rows = exec_sql_with_cols(
            conn, f"SHOW FUNCTIONS IN SCHEMA {fq(db, schema)}"
        )
        i_name = _find_col(cols, "name")
        i_sig = (
            _find_col(cols, "signature")
            or _find_col(cols, "arguments")
            or _find_col(cols, "arguments_text")
        )

        target_name = str(name).upper()
        for r in rows:
            fname = r[i_name] if (i_name is not None and len(r) > i_name) else None
            if not fname:
                continue
            if str(fname).upper() != target_name:
                continue

            sig = None
            if i_sig is not None and len(r) > i_sig and r[i_sig]:
                sig = str(r[i_sig]).strip()
                out["signature"] = sig

            ident = f"{db}.{schema}.{name}{sig}" if sig else f"{db}.{schema}.{name}"
            try:
                out["ddl"] = fetch_one_val(
                    conn, "SELECT GET_DDL('FUNCTION', %s, TRUE)", (ident,)
                )
                return out
            except Exception as e:
                logger.warning(f"Ignored exception: {e}")
                # fallback without signature
                try:
                    out["ddl"] = fetch_one_val(
                        conn,
                        "SELECT GET_DDL('FUNCTION', %s, TRUE)",
                        (f"{db}.{schema}.{name}",),
                    )
                    return out
                except Exception as e:
                    out["error"] = str(e)
                    out["ddl"] = None
                    return out

        # not found in SHOW output: last resort direct GET_DDL
        try:
            out["ddl"] = fetch_one_val(
                conn,
                "SELECT GET_DDL('FUNCTION', %s, TRUE)",
                (f"{db}.{schema}.{name}",),
            )
            return out
        except Exception as e:
            out["error"] = str(e)
            return out
    except Exception as e:
        out["error"] = str(e)
        return out


def list_pipes(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW PIPES IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_pipe_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('PIPE', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


def list_dynamic_tables(conn, db: str, schema: str) -> list[str]:
    try:
        cols, rows = exec_sql_with_cols(
            conn, f"SHOW DYNAMIC TABLES IN SCHEMA {fq(db, schema)}"
        )
        i_name = _find_col(cols, "name")
        if i_name is None:
            return []
        return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]
    except:
        return []


def get_dynamic_table_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn,
            "SELECT GET_DDL('DYNAMIC_TABLE', %s, TRUE)",
            (f"{db}.{schema}.{name}",),
        )
    except:
        return None


def list_masking_policies(conn, db: str, schema: str) -> list[str]:
    try:
        cols, rows = exec_sql_with_cols(
            conn, f"SHOW MASKING POLICIES IN SCHEMA {fq(db, schema)}"
        )
        i_name = _find_col(cols, "name")
        if i_name is None:
            return []
        return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]
    except:
        return []


def get_masking_policy_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('POLICY', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


def list_row_access_policies(conn, db: str, schema: str) -> list[str]:
    try:
        cols, rows = exec_sql_with_cols(
            conn, f"SHOW ROW ACCESS POLICIES IN SCHEMA {fq(db, schema)}"
        )
        i_name = _find_col(cols, "name")
        if i_name is None:
            return []
        return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]
    except:
        return []


def get_row_access_policy_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('POLICY', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


def list_alerts(conn, db: str, schema: str) -> list[str]:
    try:
        cols, rows = exec_sql_with_cols(conn, f"SHOW ALERTS IN SCHEMA {fq(db, schema)}")
        i_name = _find_col(cols, "name")
        if i_name is None:
            return []
        return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]
    except:
        return []


def get_alert_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('ALERT', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


def list_materialized_views(conn, db: str, schema: str) -> list[str]:
    try:
        cols, rows = exec_sql_with_cols(
            conn, f"SHOW MATERIALIZED VIEWS IN SCHEMA {fq(db, schema)}"
        )
        i_name = _find_col(cols, "name")
        if i_name is None:
            return []
        return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]
    except:
        return []


def get_materialized_view_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn,
            "SELECT GET_DDL('VIEW', %s, TRUE)",
            (f"{db}.{schema}.{name}",),
        )
    except:
        return None


def list_views(conn, db: str, schema: str) -> list[str]:
    # Try SHOW VIEWS first
    try:
        cols, rows = exec_sql_with_cols(conn, f"SHOW VIEWS IN SCHEMA {fq(db, schema)}")
        i_name = _find_col(cols, "name")
        if i_name is not None:
            views = [r[i_name] for r in rows if len(r) > i_name and r[i_name]]
            if views:
                return views
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        pass

    # Fallback to INFORMATION_SCHEMA
    try:
        cols, rows = exec_sql_with_cols(
            conn,
            f"SELECT TABLE_NAME FROM {db}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema.upper()}'",
        )
        if rows:
            return [r[0] for r in rows if r[0]]
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        pass

    return []


def get_view_ddl(conn, db: str, schema: str, view_name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('VIEW', %s, TRUE)", (f"{db}.{schema}.{view_name}",)
        )
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        return None


def list_semantic_views(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(
        conn, f"SHOW SEMANTIC VIEWS IN SCHEMA {fq(db, schema)}"
    )
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_semantic_view_ddl(conn, db: str, schema: str, sv_name: str) -> Optional[str]:
    return fetch_one_val(
        conn, "SELECT GET_DDL('SEMANTIC VIEW', %s, TRUE)", (f"{db}.{schema}.{sv_name}",)
    )


def list_streamlits(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW STREAMLITS IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_streamlit_ddl(conn, db: str, schema: str, app_name: str) -> Optional[str]:
    return fetch_one_val(
        conn, "SELECT GET_DDL('STREAMLIT', %s, TRUE)", (f"{db}.{schema}.{app_name}",)
    )


def list_agents(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW AGENTS IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def describe_agent_row(conn, db: str, schema: str, agent: str) -> dict:
    cols, rows = exec_sql_with_cols(conn, f"DESCRIBE AGENT {fq(db, schema, agent)}")
    if not rows:
        return {}
    row = rows[0]
    out = {}
    for i, c in enumerate(cols):
        if i < len(row):
            out[c.lower()] = row[i]
    return out


def list_notebooks(conn, db: str, schema: str) -> list[dict]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW NOTEBOOKS IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    i_comment = _find_col(cols, "comment")
    i_query_wh = _find_col(cols, "query_warehouse")
    out = []
    if i_name is None:
        return out

    for r in rows:
        name = r[i_name] if len(r) > i_name else None
        if not name:
            continue
        out.append(
            {
                "name": str(name),
                "comment": r[i_comment]
                if (i_comment is not None and len(r) > i_comment)
                else None,
                "query_warehouse": r[i_query_wh]
                if (i_query_wh is not None and len(r) > i_query_wh)
                else None,
            }
        )
    return out


def list_user_stages(conn, db: str, schema: str) -> list[dict]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW STAGES IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    i_type = _find_col(cols, "type")
    i_url = _find_col(cols, "url")
    if i_name is None:
        return []
    out = []
    for r in rows:
        name = r[i_name] if len(r) > i_name else None
        if not name:
            continue
        stype = (r[i_type] if (i_type is not None and len(r) > i_type) else "") or ""
        url = (r[i_url] if (i_url is not None and len(r) > i_url) else "") or ""
        out.append({"name": str(name), "type": str(stype).upper(), "url": str(url)})
    return out


def get_table_columns(conn, db: str, schema: str, table: str) -> list[str]:
    rows = exec_sql(conn, f"DESC TABLE {fq(db, schema, table)}")
    cols = []
    for r in rows:
        if r and r[0]:
            cols.append(r[0])
    return cols


def is_iceberg_table(conn, db: str, schema: str, table: str) -> bool:
    try:
        rows = exec_sql(conn, f"SHOW TABLES IN SCHEMA {fq(db, schema)}")
        for r in rows:
            if len(r) > 1 and r[1] == table:
                if len(r) > 5 and r[5] and "ICEBERG" in str(r[5]).upper():
                    return True
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
    return False


def has_external_handler(func_ddl: str) -> bool:
    if not func_ddl:
        return False
    return "HANDLER" in func_ddl.upper() and "EXTERNAL" in func_ddl.upper()


def is_external_stage(stage_type: str) -> bool:
    return stage_type and "EXTERNAL" in stage_type.upper()


def get_stage_ddl(conn, db: str, schema: str, stage_name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn,
            "SELECT GET_DDL('STAGE', %s, TRUE)",
            (f"{db}.{schema}.{stage_name}",),
        )
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        return None


def list_cortex_search_services(conn, db: str, schema: str) -> list[str]:
    """List Cortex Search services in a schema.

    SHOW CORTEX SEARCH SERVICES does not document IN SCHEMA filtering, so we
    run SHOW and filter rows by database_name/schema_name.
    """
    cols, rows = exec_sql_with_cols(conn, "SHOW CORTEX SEARCH SERVICES")
    i_db = _find_col(cols, "database_name")
    i_schema = _find_col(cols, "schema_name")
    i_name = _find_col(cols, "name")
    if i_name is None or i_db is None or i_schema is None:
        return []
    db_u = db.upper()
    schema_u = schema.upper()
    out = []
    for r in rows:
        if len(r) <= max(i_name, i_db, i_schema):
            continue
        if str(r[i_db]).upper() == db_u and str(r[i_schema]).upper() == schema_u:
            if r[i_name]:
                out.append(r[i_name])
    return out


def get_cortex_search_service_info(
    conn, db: str, schema: str, name: str
) -> Optional[dict]:
    """Get metadata for a Cortex Search service from SHOW output.

    Returns a dict with keys matching SHOW CORTEX SEARCH SERVICES columns,
    or None if not found.
    """
    try:
        cols, rows = exec_sql_with_cols(
            conn,
            "SHOW CORTEX SEARCH SERVICES",
        )
    except Exception as e:
        logger.warning(f"Ignored exception: {e}")
        return None

    col_map = {c.lower(): i for i, c in enumerate(cols)}
    target = name.upper()
    db_u = db.upper()
    schema_u = schema.upper()
    i_db = col_map.get("database_name")
    i_schema = col_map.get("schema_name")

    for r in rows:
        i_name = col_map.get("name")
        if i_name is None or len(r) <= i_name:
            continue
        if str(r[i_name]).upper() == target:
            if i_db is not None and i_schema is not None:
                if len(r) <= max(i_db, i_schema):
                    continue
                if str(r[i_db]).upper() != db_u or str(r[i_schema]).upper() != schema_u:
                    continue
            info = {}
            for col_name, idx in col_map.items():
                if idx < len(r):
                    info[col_name] = r[idx]
            # Enrich with DESCRIBE output when available
            try:
                dcols, drows = exec_sql_with_cols(
                    conn,
                    f"DESCRIBE CORTEX SEARCH SERVICE {fq(db, schema, name)}",
                )
                for dr in drows:
                    if len(dr) >= 2 and dr[0]:
                        key = str(dr[0]).lower()
                        info[key] = dr[1]
            except Exception as e:
                logger.warning(f"Ignored exception: {e}")
                pass
            return info
    return None


def build_cortex_search_ddl(info: dict, db: str, schema: str) -> Optional[str]:
    """Reconstruct CREATE CORTEX SEARCH SERVICE DDL from SHOW metadata.

    GET_DDL does not support CORTEX_SEARCH_SERVICE, so we rebuild the
    CREATE statement from SHOW output columns.
    """
    if not info:
        return None

    name = info.get("name")
    if not name:
        return None

    search_col = info.get("search_column", "")
    attr_cols = info.get("attribute_columns", "")
    columns = info.get("columns", "")
    pk_cols = info.get("primary_key_columns", "")
    warehouse = info.get("warehouse", "")
    target_lag = info.get("target_lag", "")
    comment = info.get("comment", "")
    definition = info.get("definition", "")

    if not definition:
        return None

    parts = [f"CREATE OR REPLACE CORTEX SEARCH SERVICE {fq(db, schema, name)}"]

    # Prefer multi-index form when index columns are available in metadata.
    text_indexes = info.get("text_indexes")
    vector_indexes = info.get("vector_indexes")
    if text_indexes and vector_indexes:
        parts.append(f"  TEXT INDEXES {text_indexes}")
        parts.append(f"  VECTOR INDEXES {vector_indexes}")
    elif search_col:
        parts.append(f"  ON {search_col}")

    if attr_cols:
        parts.append(f"  ATTRIBUTES {attr_cols}")

    if pk_cols:
        parts.append(f"  PRIMARY KEY ({pk_cols})")

    if warehouse:
        parts.append(f"  WAREHOUSE = {warehouse}")

    if target_lag:
        parts.append(f"  TARGET_LAG = '{target_lag}'")

    # Optional properties when available
    embedding_model = info.get("embedding_model")
    if embedding_model:
        parts.append(f"  EMBEDDING_MODEL = '{embedding_model}'")

    refresh_mode = info.get("refresh_mode")
    if refresh_mode:
        parts.append(f"  REFRESH_MODE = {refresh_mode}")

    initialize = info.get("initialize")
    if initialize:
        parts.append(f"  INITIALIZE = {initialize}")

    full_idx_days = info.get("full_index_build_interval_days")
    if full_idx_days not in (None, "", "NULL"):
        parts.append(f"  FULL_INDEX_BUILD_INTERVAL_DAYS = {full_idx_days}")

    if comment:
        escaped = comment.replace("'", "''")
        parts.append(f"  COMMENT = '{escaped}'")

    parts.append(f"AS {definition}")

    return "\n".join(parts)
