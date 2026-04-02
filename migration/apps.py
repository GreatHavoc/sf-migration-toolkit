import os
import re
import tempfile
from typing import Optional
from connection import exec_sql, exec_script
from discovery import (
    list_streamlits,
    get_streamlit_ddl,
    list_notebooks,
    list_agents,
    describe_agent_row,
    get_all_schemas,
)
from utils import rewrite_db_in_ddl


def _escape_sql_string(v: str) -> str:
    return v.replace("'", "''")


def migrate_streamlits(
    src_conn,
    tgt_conn,
    src_db: str,
    src_schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
    rewrite_db: bool = False,
    initialize_live: bool = True,
):
    """Migrate Streamlit apps."""
    apps = list_streamlits(src_conn, src_db, src_schema)
    errors = []
    migrated = 0

    for app in apps:
        ddl = get_streamlit_ddl(src_conn, src_db, src_schema, app)
        if not ddl:
            errors.append(f"{app}: Could not get DDL")
            continue
        if rewrite_db and tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)

        if not dry_run:
            try:
                exec_script(tgt_conn, ddl, remove_comments=True)
                if initialize_live:
                    exec_sql(
                        tgt_conn,
                        f"ALTER STREAMLIT {tgt_db}.{tgt_schema}.{app} ADD LIVE VERSION FROM LAST",
                    )
                migrated += 1
            except Exception as e:
                errors.append(f"{app}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def create_or_replace_agent_sql(
    tgt_db: str,
    tgt_schema: str,
    agent_name: str,
    comment: Optional[str],
    profile: Optional[str],
    agent_spec: str,
) -> str:
    lines = [f"CREATE OR REPLACE AGENT {tgt_db}.{tgt_schema}.{agent_name}"]
    if comment:
        lines.append(f"  COMMENT = '{_escape_sql_string(str(comment))}'")
    if profile:
        lines.append(f"  PROFILE = '{_escape_sql_string(str(profile))}'")
    lines.append("FROM SPECIFICATION")
    lines.append("$$")
    lines.append(agent_spec or "")
    lines.append("$$;")
    return "\n".join(lines)


def migrate_agents(
    src_conn,
    tgt_conn,
    src_db: str,
    src_schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
    rewrite_db: bool = False,
):
    """Migrate Cortex Agents."""
    agents = list_agents(src_conn, src_db, src_schema)
    errors = []
    migrated = 0

    for a in agents:
        meta = describe_agent_row(src_conn, src_db, src_schema, a)
        agent_spec = meta.get("agent_spec") or meta.get("specification") or ""
        comment = meta.get("comment")
        profile = meta.get("profile")

        if rewrite_db and tgt_db != src_db and isinstance(agent_spec, str):
            agent_spec = rewrite_db_in_ddl(agent_spec, src_db, tgt_db)

        sql = create_or_replace_agent_sql(
            tgt_db, tgt_schema, a, comment, profile, str(agent_spec)
        )
        if not dry_run:
            try:
                exec_sql(tgt_conn, sql)
                migrated += 1
            except Exception as e:
                errors.append(f"{a}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def migrate_sequences(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate sequences from source to target."""
    from discovery import list_sequences, get_sequence_ddl

    sequences = list_sequences(src_conn, src_db, schema)
    if not sequences:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0

    for seq in sequences:
        ddl = get_sequence_ddl(src_conn, src_db, schema, seq)
        if not ddl:
            errors.append(f"{seq}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{seq}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def migrate_file_formats(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate file formats from source to target."""
    from discovery import list_file_formats, get_file_format_ddl

    ffs = list_file_formats(src_conn, src_db, schema)
    if not ffs:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0

    for ff in ffs:
        ddl = get_file_format_ddl(src_conn, src_db, schema, ff)
        if not ddl:
            errors.append(f"{ff}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{ff}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def notebook_live_stage_folder(src_db: str, src_schema: str, notebook_name: str) -> str:
    nb = notebook_name.replace('"', '\\"')
    return f'snow://notebook/{src_db}.{src_schema}."{nb}"/versions/live'


def ensure_internal_stage(conn, db: str, schema: str, stage_name: str):
    from utils import fq

    exec_sql(conn, f"CREATE STAGE IF NOT EXISTS {fq(db, schema, stage_name)}")


def sql_string_literal(s: str) -> str:
    return "'" + (s or "").replace("'", "''") + "'"


def _to_file_uri(local_path: str) -> str:
    return "file://" + local_path.replace("\\", "/")


def get_notebook_ipynb_to_local(
    src_conn, src_db: str, src_schema: str, notebook_name: str, local_dir: str
) -> str:
    os.makedirs(local_dir, exist_ok=True)
    main_file = f"{notebook_name}.ipynb"
    stage_file = (
        f"{notebook_live_stage_folder(src_db, src_schema, notebook_name)}/{main_file}"
    )
    sql = f"GET {sql_string_literal(stage_file)} {sql_string_literal('file://' + local_dir)} OVERWRITE = TRUE;"
    exec_sql(src_conn, sql)
    return os.path.join(local_dir, main_file)


def put_local_file_to_internal_stage(
    tgt_conn, local_file_path: str, internal_stage_ref: str, stage_subdir: str
):
    stage_subdir = (stage_subdir or "").strip().strip("/")
    target = (
        f"{internal_stage_ref}/{stage_subdir}/"
        if stage_subdir
        else f"{internal_stage_ref}/"
    )
    sql = f"PUT {sql_string_literal('file://' + local_file_path)} {target} OVERWRITE = TRUE AUTO_COMPRESS = FALSE;"
    exec_sql(tgt_conn, sql)


def build_create_notebook_from_stage_sql(
    tgt_db: str,
    tgt_schema: str,
    notebook_name: str,
    from_stage_folder: str,
    main_file: str,
    query_warehouse: Optional[str],
    comment: Optional[str],
) -> str:
    from utils import qident

    lines = [
        f"CREATE OR REPLACE NOTEBOOK {fq(tgt_db, tgt_schema, notebook_name)}",
        f"  FROM {sql_string_literal(from_stage_folder)}",
        f"  MAIN_FILE = {sql_string_literal(main_file)}",
    ]
    if comment:
        lines.append(f"  COMMENT = {sql_string_literal(str(comment))}")
    if query_warehouse:
        lines.append(f"  QUERY_WAREHOUSE = {qident(str(query_warehouse))}")
    return "\n".join(lines) + ";"


def migrate_notebooks(
    src_conn,
    tgt_conn,
    src_db: str,
    src_schema: str,
    tgt_db: str,
    tgt_schema: str,
    mig_db: str,
    mig_schema: str,
    nb_int_stage_name: str,
    stage_prefix: str,
    run_id: str,
    tgt_query_wh: Optional[str],
    use_source_comment: bool,
    use_source_query_wh_if_target_empty: bool,
    dry_run: bool = False,
):
    """Migrate notebooks."""
    notebooks = list_notebooks(src_conn, src_db, src_schema)
    if not notebooks:
        return {"migrated": 0, "errors": []}

    if not dry_run:
        exec_sql(tgt_conn, f"CREATE DATABASE IF NOT EXISTS {tgt_db}")
        exec_sql(tgt_conn, f"CREATE SCHEMA IF NOT EXISTS {tgt_db}.{tgt_schema}")
        ensure_internal_stage(tgt_conn, mig_db, mig_schema, nb_int_stage_name)

    internal_stage_ref = f"@{mig_db}.{mig_schema}.{nb_int_stage_name}"
    stage_subdir = f"{stage_prefix}/{run_id}/notebooks/{src_db}/{src_schema}"
    from_stage_folder = f"{internal_stage_ref}/{stage_subdir}/"
    local_root = os.path.join(
        tempfile.gettempdir(), "sf_nb_migration", run_id, src_db, src_schema
    )

    errors = []
    migrated = 0

    for meta in notebooks:
        nb = meta["name"]
        main_file = f"{nb}.ipynb"
        comment = meta.get("comment") if use_source_comment else None
        src_qwh = meta.get("query_warehouse")
        qwh = tgt_query_wh or None
        if (not qwh) and use_source_query_wh_if_target_empty:
            qwh = src_qwh or None

        local_dir = os.path.join(local_root, nb)

        if not dry_run:
            try:
                local_file = get_notebook_ipynb_to_local(
                    src_conn, src_db, src_schema, nb, local_dir
                )
                put_local_file_to_internal_stage(
                    tgt_conn, local_file, internal_stage_ref, stage_subdir
                )
                sql = build_create_notebook_from_stage_sql(
                    tgt_db=tgt_db,
                    tgt_schema=tgt_schema,
                    notebook_name=nb,
                    from_stage_folder=from_stage_folder,
                    main_file=main_file,
                    query_warehouse=qwh,
                    comment=comment,
                )
                exec_sql(tgt_conn, sql)
                migrated += 1
            except Exception as e:
                errors.append(f"{nb}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}
