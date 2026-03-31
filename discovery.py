from typing import Optional
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
    }

    try:
        tables = exec_sql(conn, f"SHOW TABLES IN SCHEMA {fq(db, schema)}")
        inventory["tables"] = [r[1] for r in tables if len(r) > 1]
    except:
        pass

    try:
        views = exec_sql(conn, f"SHOW VIEWS IN SCHEMA {fq(db, schema)}")
        inventory["views"] = [r[1] for r in views if len(r) > 1]
    except:
        pass

    try:
        mvs = exec_sql(conn, f"SHOW MATERIALIZED VIEWS IN SCHEMA {fq(db, schema)}")
        inventory["materialized_views"] = [r[1] for r in mvs if len(r) > 1]
    except:
        pass

    try:
        seqs = exec_sql(conn, f"SHOW SEQUENCES IN SCHEMA {fq(db, schema)}")
        inventory["sequences"] = [r[1] for r in seqs if len(r) > 1]
    except:
        pass

    try:
        ffs = exec_sql(conn, f"SHOW FILE FORMATS IN SCHEMA {fq(db, schema)}")
        inventory["file_formats"] = [r[1] for r in ffs if len(r) > 1]
    except:
        pass

    try:
        stages = exec_sql(conn, f"SHOW STAGES IN SCHEMA {fq(db, schema)}")
        inventory["stages"] = [r[1] for r in stages if len(r) > 1]
    except:
        pass

    try:
        streams = exec_sql(conn, f"SHOW STREAMS IN SCHEMA {fq(db, schema)}")
        inventory["streams"] = [r[1] for r in streams if len(r) > 1]
    except:
        pass

    try:
        pipes = exec_sql(conn, f"SHOW PIPES IN SCHEMA {fq(db, schema)}")
        inventory["pipes"] = [r[1] for r in pipes if len(r) > 1]
    except:
        pass

    try:
        task = exec_sql(conn, f"SHOW TASKS IN SCHEMA {fq(db, schema)}")
        inventory["tasks"] = [r[1] for r in task if len(r) > 1]
    except:
        pass

    try:
        procs = exec_sql(conn, f"SHOW PROCEDURES IN SCHEMA {fq(db, schema)}")
        inventory["procedures"] = [r[1] for r in procs if len(r) > 1]
    except:
        pass

    try:
        funcs = exec_sql(conn, f"SHOW FUNCTIONS IN SCHEMA {fq(db, schema)}")
        inventory["functions"] = [r[1] for r in funcs if len(r) > 1]
    except:
        pass

    try:
        tags = exec_sql(conn, f"SHOW TAGS IN SCHEMA {fq(db, schema)}")
        inventory["tags"] = [r[1] for r in tags if len(r) > 1]
    except:
        pass

    try:
        mp = exec_sql(conn, f"SHOW MASKING POLICIES IN SCHEMA {fq(db, schema)}")
        inventory["masking_policies"] = [r[1] for r in mp if len(r) > 1]
    except:
        pass

    try:
        rap = exec_sql(conn, f"SHOW ROW ACCESS POLICIES IN SCHEMA {fq(db, schema)}")
        inventory["row_access_policies"] = [r[1] for r in rap if len(r) > 1]
    except:
        pass

    try:
        dt = exec_sql(conn, f"SHOW DYNAMIC TABLES IN SCHEMA {fq(db, schema)}")
        inventory["dynamic_tables"] = [r[1] for r in dt if len(r) > 1]
    except:
        pass

    try:
        alerts = exec_sql(conn, f"SHOW ALERTS IN SCHEMA {fq(db, schema)}")
        inventory["alerts"] = [r[1] for r in alerts if len(r) > 1]
    except:
        pass

    try:
        ext = exec_sql(conn, f"SHOW EXTERNAL TABLES IN SCHEMA {fq(db, schema)}")
        inventory["external_tables"] = [r[1] for r in ext if len(r) > 1]
    except:
        pass

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
            conn, "SELECT GET_DDL('FILE FORMAT', %s, TRUE)", (f"{db}.{schema}.{name}",)
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


def list_procedures(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW PROCEDURES IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


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
            except Exception:
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

            # Heuristic: if owner/creator mentions SNOWFLAKE or system-like, mark as system
            if owner and ("SNOWFLAKE" in owner.upper() or "SYSTEM" in owner.upper()):
                out["is_system"] = True

            ident = f"{db}.{schema}.{name}{sig}" if sig else f"{db}.{schema}.{name}"
            try:
                out["ddl"] = fetch_one_val(
                    conn, "SELECT GET_DDL('PROCEDURE', %s, TRUE)", (ident,)
                )
            except Exception:
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
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_function_ddl(conn, db: str, schema: str, name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('FUNCTION', %s, TRUE)", (f"{db}.{schema}.{name}",)
        )
    except:
        return None


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
            "SELECT GET_DDL('DYNAMIC TABLE', %s, TRUE)",
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
            "SELECT GET_DDL('MATERIALIZED VIEW', %s, TRUE)",
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
    except Exception:
        pass

    # Fallback to INFORMATION_SCHEMA
    try:
        cols, rows = exec_sql_with_cols(
            conn,
            f"SELECT VIEW_NAME FROM {db}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema.upper()}'",
        )
        if rows:
            return [r[0] for r in rows if r[0]]
    except Exception:
        pass

    return []


def get_view_ddl(conn, db: str, schema: str, view_name: str) -> Optional[str]:
    try:
        return fetch_one_val(
            conn, "SELECT GET_DDL('VIEW', %s, TRUE)", (f"{db}.{schema}.{view_name}",)
        )
    except Exception:
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
    except:
        pass
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
    except Exception:
        return None
