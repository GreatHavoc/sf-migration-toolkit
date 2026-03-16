import re
import uuid
import os
import json
import tempfile
from io import StringIO
from typing import Optional

import streamlit as st
import snowflake.connector


# ----------------------------
# Constants
# ----------------------------
SKIP_SCHEMAS = {"INFORMATION_SCHEMA"}

DEFAULT_MIG_DB = "MIGRATION_DB"
DEFAULT_MIG_SCHEMA = "PUBLIC"
DEFAULT_STAGE = "MIGRATION_STAGE"
DEFAULT_INTEGRATION = "AZURE_MIGRATION_INT"

# Notebook migration uses an INTERNAL stage on the TARGET account because PUT supports internal stages. [page:1]
DEFAULT_NB_INT_STAGE = "NB_MIG_INT_STAGE"

# Local backup/restore uses an INTERNAL stage for COPY INTO / GET / PUT.
DEFAULT_LOCAL_INT_STAGE = "LOCAL_BACKUP_STAGE"


# ----------------------------
# Snowflake helpers
# ----------------------------
def connect_sf(account, user, password, role=None, warehouse=None, passcode=None):
    kwargs = dict(
        account=account,
        user=user,
        password=password,
        client_session_keep_alive=True,
    )
    if role:
        kwargs["role"] = role
    if warehouse:
        kwargs["warehouse"] = warehouse
    if passcode:
        kwargs["passcode"] = passcode
    return snowflake.connector.connect(**kwargs)


def exec_sql(conn, sql, params=None):
    with conn.cursor() as cur:
        try:
            cur.execute(sql, params or {})
            try:
                return cur.fetchall()
            except Exception:
                return []
        except Exception as e:
            raise RuntimeError(f"SQL failed: {sql}\nError: {e}") from e


def exec_sql_with_cols(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in (cur.description or [])]
        try:
            rows = cur.fetchall()
        except Exception:
            rows = []
        return cols, rows


def fetch_one_val(conn, sql, params=None):
    rows = exec_sql(conn, sql, params)
    return rows[0][0] if rows else None


def exec_script(conn, sql_text, remove_comments=True):
    sql_stream = StringIO(sql_text)
    for _cur in conn.execute_stream(sql_stream, remove_comments=remove_comments):
        pass


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fq(db: str, schema: str, obj: str | None = None) -> str:
    if obj is None:
        return f"{qident(db)}.{qident(schema)}"
    return f"{qident(db)}.{qident(schema)}.{qident(obj)}"


def ensure_session_namespace(conn, db: str, schema: str):
    exec_sql(conn, f"USE DATABASE {qident(db)}")
    exec_sql(conn, f"USE SCHEMA {fq(db, schema)}")


def bootstrap_db_schema(conn, mig_db: str, mig_schema: str):
    exec_sql(conn, f"CREATE DATABASE IF NOT EXISTS {qident(mig_db)}")
    exec_sql(conn, f"CREATE SCHEMA IF NOT EXISTS {fq(mig_db, mig_schema)}")
    ensure_session_namespace(conn, mig_db, mig_schema)


def _find_col(cols, *candidates):
    low = [c.lower() for c in cols]
    for cand in candidates:
        cand = cand.lower()
        if cand in low:
            return low.index(cand)
    return None


def _escape_sql_string(v: str) -> str:
    return v.replace("'", "''")


def sql_string_literal(s: str) -> str:
    return "'" + (s or "").replace("'", "''") + "'"


def stage_loc_literal(loc: str) -> str:
    """
    Wrap stage locations in single quotes to safely handle spaces/special chars in the path. [COPY INTO notes]
    Works for @stage/path as well as external URLs if used later.
    """
    loc = (loc or "").strip()
    if not loc:
        return loc
    if loc.startswith("@") or "://" in loc:
        return sql_string_literal(loc)
    return loc


def strip_streamlit_from_schema_ddl(schema_ddl: str) -> str:
    """
    Schema-level GET_DDL('SCHEMA', ...) can include CREATE STREAMLIT statements that may compile-bomb
    (e.g. older outputs that append /versions/live into the identifier).
    We migrate Streamlit apps separately via GET_DDL('STREAMLIT', ...). [CREATE STREAMLIT docs]
    """
    return re.sub(
        r"(?is)\bcreate\s+(or\s+replace\s+)?streamlit\b.*?;\s*",
        "",
        schema_ddl,
    )


# ----------------------------
# DESC parsing
# ----------------------------
def desc_storage_integration_to_dict(rows) -> dict:
    out = {}
    for r in rows:
        if len(r) >= 3:
            out[str(r[0]).upper()] = r[2]
    return out


def desc_stage_to_dict(rows) -> dict:
    out = {}
    for r in rows:
        if len(r) >= 4:
            prop = str(r[1]).upper()
            val = r[3]
            out[prop] = val
    return out


def describe_storage_integration(conn, integration_name: str) -> dict:
    rows = exec_sql(conn, f"DESC STORAGE INTEGRATION {qident(integration_name)}")
    return desc_storage_integration_to_dict(rows)


def describe_stage(conn, db: str, schema: str, stage: str) -> dict:
    try:
        rows = exec_sql(conn, f"DESC STAGE {fq(db, schema, stage)}")
        return desc_stage_to_dict(rows)
    except Exception:
        return {}


# ----------------------------
# Azure external stage: always-latest ensure
# ----------------------------

def _validate_azure_storage_account(storage_account: str):
    # Azure storage account names must be 3-24 chars, lowercase letters and numbers only.
    if not re.fullmatch(r"[a-z0-9]{3,24}", storage_account or ""):
        raise ValueError(
            "Azure storage account name must be 3-24 characters, lowercase letters and numbers only."
        )


def _validate_azure_container_name(container: str):
    # Azure container names must be 3-63 chars, lowercase letters/numbers/hyphens, start/end with letter/number.
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?", container or ""):
        raise ValueError(
            "Azure container name must be 3-63 characters, all lowercase letters/numbers/hyphens, and start/end with a letter or number."
        )


def normalize_prefix(prefix: str) -> str:
    p = (prefix or "").strip()
    if not p:
        return ""

    if "\\" in p or " " in p:
        raise ValueError("Azure prefix must not contain spaces or backslashes.")
    if "//" in p:
        raise ValueError("Azure prefix must not contain consecutive slashes (//).")

    p = p.lstrip("/")
    if p and not p.endswith("/"):
        p += "/"
    return p


def _normalize_stage_url(stage_url: str) -> str:
    """Normalize common stage URL quoting/serialization artifacts.

    Some UI/JSON paths can wrap the URL in brackets or quotes (e.g. `["azure://..."]`).
    This normalizer strips those so the URL can be validated/used correctly.
    """
    u = (stage_url or "").strip()

    # Strip list syntax: ["..."] or ['...']
    if u.startswith("[") and u.endswith("]"):
        u = u[1:-1].strip()

    # Strip enclosing quotes
    if (u.startswith('"') and u.endswith('"')) or (u.startswith("'") and u.endswith("'")):
        u = u[1:-1]

    return u.strip()


def build_azure_stage_url(storage_account: str, container: str, prefix: str) -> str:
    _validate_azure_storage_account(storage_account)
    _validate_azure_container_name(container)
    p = normalize_prefix(prefix)
    return f"azure://{storage_account}.blob.core.windows.net/{container}/{p}"


def ensure_storage_integration_azure(conn, integration_name: str, tenant_id: str, allowed_locations: list[str]):
    if not tenant_id.strip():
        raise ValueError("Azure tenant id is required.")
    allowed_sql = ", ".join(f"'{loc}'" for loc in allowed_locations if loc.strip())
    if not allowed_sql:
        raise ValueError("At least one STORAGE_ALLOWED_LOCATIONS entry is required.")

    exec_sql(conn, f"""
CREATE STORAGE INTEGRATION IF NOT EXISTS {qident(integration_name)}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '{tenant_id}'
  STORAGE_ALLOWED_LOCATIONS = ({allowed_sql});
""".strip())

    exec_sql(conn, f"""
ALTER STORAGE INTEGRATION {qident(integration_name)} SET
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ({allowed_sql});
""".strip())


def ensure_external_stage_azure(conn, mig_db: str, mig_schema: str, stage_name: str, stage_url: str, integration_name: str):
    bootstrap_db_schema(conn, mig_db, mig_schema)

    # Normalize common quoting/serialization artifacts (e.g. ["azure://..."]).
    stage_url = _normalize_stage_url(stage_url)

    if not stage_url.strip() or stage_url.strip().lower() == "azure://.blob.core.windows.net//":
        raise ValueError("Stage URL is empty/invalid. Fill storage account + container + prefix first.")

    # Validate the stage URL structure early so we can show a helpful error message.
    m = re.match(r"^azure://([a-z0-9]+)\.blob\.core\.windows\.net/([^/]+)/(.*)$", stage_url)
    if not m:
        raise ValueError(
            "Stage URL seems malformed. Expected: azure://<account>.blob.core.windows.net/<container>/<prefix>/"
        )
    acct, container, prefix = m.group(1), m.group(2), m.group(3)
    _validate_azure_storage_account(acct)
    _validate_azure_container_name(container)
    normalize_prefix(prefix)

    props = describe_stage(conn, mig_db, mig_schema, stage_name)
    stage_exists = bool(props)

    existing_url = (props.get("URL") or "").strip()
    existing_integration = (props.get("STORAGE_INTEGRATION") or "").strip()

    is_internal_like = stage_exists and existing_url == "" and existing_integration == ""

    if (not stage_exists) or is_internal_like or (existing_url and existing_url != stage_url) or (
        existing_integration and existing_integration.upper() != integration_name.upper()
    ):
        exec_sql(conn, f"DROP STAGE IF EXISTS {fq(mig_db, mig_schema, stage_name)}")
        exec_sql(conn, f"""
CREATE STAGE {fq(mig_db, mig_schema, stage_name)}
  URL = '{stage_url}'
  STORAGE_INTEGRATION = {qident(integration_name)};
""".strip())
        return

    exec_sql(conn, f"""
ALTER STAGE {fq(mig_db, mig_schema, stage_name)} SET
  URL = '{stage_url}'
  STORAGE_INTEGRATION = {qident(integration_name)};
""".strip())


def list_stage(conn, mig_db: str, mig_schema: str, stage_name: str):
    ensure_session_namespace(conn, mig_db, mig_schema)
    return exec_sql(conn, f"LIST @{mig_db}.{mig_schema}.{stage_name}")


# ----------------------------
# CSV copy (stable column order)
# ----------------------------
def get_table_columns(conn, db: str, schema: str, table: str) -> list[str]:
    rows = exec_sql(conn, f"DESC TABLE {fq(db, schema, table)}")
    cols = []
    for r in rows:
        if r and r[0]:
            cols.append(r[0])
    return cols


def build_csv_unload_sql(unload_path: str, src_db: str, src_schema: str, table: str, columns: list[str]) -> str:
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


def build_csv_load_sql(load_path: str, tgt_db: str, tgt_schema: str, table: str, columns: list[str]) -> str:
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


# ----------------------------
# Semantic view migration
# ----------------------------
def list_semantic_views(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW SEMANTIC VIEWS IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_semantic_view_ddl(conn, db: str, schema: str, sv_name: str) -> Optional[str]:
    return fetch_one_val(
        conn,
        "SELECT GET_DDL('SEMANTIC VIEW', %(n)s, TRUE)",
        {"n": f"{db}.{schema}.{sv_name}"},
    )


def migrate_semantic_views(
    src_conn, tgt_conn,
    src_db: str, src_schema: str,
    tgt_db: str, tgt_schema: str,
    dry_run: bool, rewrite_db: bool
):
    svs = list_semantic_views(src_conn, src_db, src_schema)
    for sv in svs:
        ddl = get_semantic_view_ddl(src_conn, src_db, src_schema, sv)
        if not ddl:
            continue
        if rewrite_db and tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
        if not dry_run:
            exec_script(tgt_conn, ddl, remove_comments=True)


# ----------------------------
# Cortex Agents migration (SQL)
# ----------------------------
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


def create_or_replace_agent_sql(
    tgt_db: str, tgt_schema: str, agent_name: str,
    comment: Optional[str], profile: Optional[str],
    agent_spec: str
) -> str:
    lines = [f"CREATE OR REPLACE AGENT {fq(tgt_db, tgt_schema, agent_name)}"]
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
    src_conn, tgt_conn,
    src_db: str, src_schema: str,
    tgt_db: str, tgt_schema: str,
    dry_run: bool, rewrite_db: bool
):
    agents = list_agents(src_conn, src_db, src_schema)
    for a in agents:
        meta = describe_agent_row(src_conn, src_db, src_schema, a)
        agent_spec = meta.get("agent_spec") or meta.get("specification") or ""
        comment = meta.get("comment")
        profile = meta.get("profile")

        if rewrite_db and tgt_db != src_db and isinstance(agent_spec, str):
            agent_spec = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, agent_spec)

        sql = create_or_replace_agent_sql(tgt_db, tgt_schema, a, comment, profile, str(agent_spec))
        if not dry_run:
            exec_sql(tgt_conn, sql)


# ----------------------------
# Streamlit migration (SQL)
# ----------------------------
def list_streamlits(conn, db: str, schema: str) -> list[str]:
    cols, rows = exec_sql_with_cols(conn, f"SHOW STREAMLITS IN SCHEMA {fq(db, schema)}")
    i_name = _find_col(cols, "name")
    if i_name is None:
        return []
    return [r[i_name] for r in rows if len(r) > i_name and r[i_name]]


def get_streamlit_ddl(conn, db: str, schema: str, app_name: str) -> Optional[str]:
    return fetch_one_val(
        conn,
        "SELECT GET_DDL('STREAMLIT', %(n)s, TRUE)",
        {"n": f"{db}.{schema}.{app_name}"},
    )


def migrate_streamlits(
    src_conn, tgt_conn,
    src_db: str, src_schema: str,
    tgt_db: str, tgt_schema: str,
    dry_run: bool, rewrite_db: bool,
    initialize_live: bool = True
):
    apps = list_streamlits(src_conn, src_db, src_schema)
    for app in apps:
        ddl = get_streamlit_ddl(src_conn, src_db, src_schema, app)
        if not ddl:
            continue
        if rewrite_db and tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)

        if not dry_run:
            exec_script(tgt_conn, ddl, remove_comments=True)
            if initialize_live:
                exec_sql(tgt_conn, f"ALTER STREAMLIT {fq(tgt_db, tgt_schema, app)} ADD LIVE VERSION FROM LAST")


# ----------------------------
# Notebook migration (FULLY automated)
# SHOW NOTEBOOKS -> GET snow://notebook/... -> PUT internal stage (target) -> CREATE NOTEBOOK FROM stage
# ----------------------------
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
                "comment": r[i_comment] if (i_comment is not None and len(r) > i_comment) else None,
                "query_warehouse": r[i_query_wh] if (i_query_wh is not None and len(r) > i_query_wh) else None,
            }
        )
    return out


def notebook_live_stage_folder(src_db: str, src_schema: str, notebook_name: str) -> str:
    # Notebook stage paths follow: snow://notebook/<db>.<schema>."<notebook>"/versions/live [notebooks file docs]
    nb = notebook_name.replace('"', '\\"')
    return f'snow://notebook/{src_db}.{src_schema}."{nb}"/versions/live'


def ensure_internal_stage(conn, db: str, schema: str, stage_name: str):
    exec_sql(conn, f"CREATE STAGE IF NOT EXISTS {fq(db, schema, stage_name)}")


def get_notebook_ipynb_to_local(src_conn, src_db: str, src_schema: str, notebook_name: str, local_dir: str) -> str:
    os.makedirs(local_dir, exist_ok=True)

    main_file = f"{notebook_name}.ipynb"
    stage_file = f"{notebook_live_stage_folder(src_db, src_schema, notebook_name)}/{main_file}"

    # GET downloads staged files to a local file:// directory. [GET docs]
    sql = f"GET {sql_string_literal(stage_file)} {sql_string_literal('file://' + local_dir)} OVERWRITE = TRUE;"
    exec_sql(src_conn, sql)

    return os.path.join(local_dir, main_file)


def put_local_file_to_internal_stage(tgt_conn, local_file_path: str, internal_stage_ref: str, stage_subdir: str):
    stage_subdir = (stage_subdir or "").strip().strip("/")
    target = f"{internal_stage_ref}/{stage_subdir}/" if stage_subdir else f"{internal_stage_ref}/"

    # PUT uploads local files to an INTERNAL stage only. [PUT docs]
    sql = (
        f"PUT {sql_string_literal('file://' + local_file_path)} "
        f"{target} OVERWRITE = TRUE AUTO_COMPRESS = FALSE;"
    )
    exec_sql(tgt_conn, sql)


def build_create_notebook_from_stage_sql(
    tgt_db: str,
    tgt_schema: str,
    notebook_name: str,
    from_stage_folder: str,  # '@db.schema.stage/path/'
    main_file: str,
    query_warehouse: Optional[str],
    comment: Optional[str],
) -> str:
    # CREATE NOTEBOOK supports creating from an ipynb on a stage via FROM + MAIN_FILE. [CREATE NOTEBOOK docs]
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


def migrate_notebooks_fully_automated(
    src_conn, tgt_conn,
    src_db: str, src_schema: str,
    tgt_db: str, tgt_schema: str,
    mig_db: str, mig_schema: str,
    nb_int_stage_name: str,
    stage_prefix: str,
    run_id: str,
    tgt_query_wh: Optional[str],
    use_source_comment: bool,
    use_source_query_wh_if_target_empty: bool,
    dry_run: bool,
):
    notebooks = list_notebooks(src_conn, src_db, src_schema)
    st.write({"schema": f"{src_db}.{src_schema}", "notebooks_found": len(notebooks)})
    if not notebooks:
        return

    if not dry_run:
        # Ensure target db/schema exists for CREATE NOTEBOOK.
        exec_sql(tgt_conn, f"CREATE DATABASE IF NOT EXISTS {qident(tgt_db)}")
        exec_sql(tgt_conn, f"CREATE SCHEMA IF NOT EXISTS {fq(tgt_db, tgt_schema)}")

        # Ensure internal stage exists (PUT requires internal stage). [PUT docs]
        ensure_internal_stage(tgt_conn, mig_db, mig_schema, nb_int_stage_name)

    internal_stage_ref = f"@{mig_db}.{mig_schema}.{nb_int_stage_name}"

    # Store notebook ipynb files in a stable folder on the target internal stage
    stage_subdir = f"{stage_prefix}/{run_id}/notebooks/{src_db}/{src_schema}"
    from_stage_folder = f"{internal_stage_ref}/{stage_subdir}/"

    local_root = os.path.join(tempfile.gettempdir(), "sf_nb_migration", run_id, src_db, src_schema)

    for meta in notebooks:
        nb = meta["name"]
        main_file = f"{nb}.ipynb"

        comment = meta.get("comment") if use_source_comment else None

        src_qwh = meta.get("query_warehouse")
        qwh = (tgt_query_wh or None)
        if (not qwh) and use_source_query_wh_if_target_empty:
            qwh = src_qwh or None

        local_dir = os.path.join(local_root, nb)
        local_file = os.path.join(local_dir, main_file)

        if not dry_run:
            # 1) GET from source notebook stage to local. [notebooks file docs][GET docs]
            local_file = get_notebook_ipynb_to_local(src_conn, src_db, src_schema, nb, local_dir)

            # 2) PUT to target internal stage. [PUT docs]
            put_local_file_to_internal_stage(tgt_conn, local_file, internal_stage_ref, stage_subdir)

            # 3) CREATE NOTEBOOK from staged ipynb. [CREATE NOTEBOOK docs]
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


# ----------------------------
# Stage file helpers (backup / restore files inside user-created stages)
# ----------------------------
def list_user_stages(conn, db: str, schema: str) -> list[dict]:
    """Return metadata for non-temporary, non-managed stages in a schema."""
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


def list_stage_files(conn, db: str, schema: str, stage_name: str) -> list[str]:
    """LIST files on a stage and return file paths relative to the stage root."""
    ensure_session_namespace(conn, db, schema)
    try:
        rows = exec_sql(conn, f"LIST @{fq(db, schema, stage_name)}")
    except Exception:
        return []
    # Column 0 is the fully-qualified path like '<stage>/path/file.csv.gz'
    # We strip the stage prefix to get relative paths.
    prefix = f"{stage_name.lower()}/"
    files = []
    for r in rows:
        if not r or not r[0]:
            continue
        f_path = str(r[0])
        # Strip the leading stage reference that Snowflake prepends
        # e.g. "my_stage/sub/file.csv" -> "sub/file.csv"
        low = f_path.lower()
        if low.startswith(prefix):
            f_path = f_path[len(prefix):]
        files.append(f_path)
    return files


def get_stage_ddl(conn, db: str, schema: str, stage_name: str) -> Optional[str]:
    """Get DDL for a stage via GET_DDL."""
    try:
        return fetch_one_val(
            conn,
            "SELECT GET_DDL('STAGE', %(n)s, TRUE)",
            {"n": f"{db}.{schema}.{stage_name}"},
        )
    except Exception:
        return None


def get_stage_files_to_local(conn, db: str, schema: str, stage_name: str, local_dir: str) -> list[str]:
    """Download all files from a stage to a local directory via GET. Returns list of local file paths."""
    os.makedirs(local_dir, exist_ok=True)
    stage_fq = f"@{fq(db, schema, stage_name)}"
    sql = f"GET {sql_string_literal(stage_fq)} {sql_string_literal(_to_file_uri(local_dir))} OVERWRITE=TRUE;"
    try:
        exec_sql(conn, sql)
    except Exception:
        return []
    # Walk the local dir and return all downloaded files
    downloaded = []
    for root, _dirs, fnames in os.walk(local_dir):
        for fn in fnames:
            downloaded.append(os.path.join(root, fn))
    return downloaded


def put_local_dir_to_stage(conn, local_dir: str, db: str, schema: str, stage_name: str):
    """Upload all files from a local directory to a stage via PUT."""
    stage_fq = f"@{fq(db, schema, stage_name)}"
    for root, _dirs, fnames in os.walk(local_dir):
        for fn in fnames:
            local_file = os.path.join(root, fn)
            # Compute the relative sub-path to preserve folder structure inside the stage
            rel = os.path.relpath(local_file, local_dir).replace("\\", "/")
            sub_dir = os.path.dirname(rel)
            target = f"{stage_fq}/{sub_dir}/" if sub_dir and sub_dir != "." else f"{stage_fq}/"
            sql = (
                f"PUT {sql_string_literal(_to_file_uri(local_file))} "
                f"{target} OVERWRITE=TRUE AUTO_COMPRESS=FALSE;"
            )
            exec_sql(conn, sql)


# ----------------------------
# Local Backup & Restore helpers
# ----------------------------
def _to_file_uri(local_path: str) -> str:
    """Convert a local filesystem path to a file:// URI with forward slashes."""
    return "file://" + local_path.replace("\\", "/")


def _write_local(path: str, content: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _read_local(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def run_local_backup(
    conn,
    src_db: str,
    schemas: list[str],
    backup_root: str,
    mig_db: str,
    mig_schema: str,
    int_stage_name: str,
    run_id: str,
    do_ddl: bool,
    do_semantic: bool,
    do_agents: bool,
    do_streamlits: bool,
    do_notebooks: bool,
    do_data: bool,
    do_stage_files: bool = False,
    dry_run: bool = False,
):
    """Export selected schemas from a Snowflake database to a local directory."""
    base = os.path.join(backup_root, run_id, src_db)
    manifest = {"database": src_db, "run_id": run_id, "schemas": {}}

    if not dry_run:
        os.makedirs(base, exist_ok=True)
        if do_data or do_notebooks:
            bootstrap_db_schema(conn, mig_db, mig_schema)
            ensure_internal_stage(conn, mig_db, mig_schema, int_stage_name)

    stage_ref = f"@{mig_db}.{mig_schema}.{int_stage_name}"

    for sch in schemas:
        st.subheader(f"Backup: {src_db}.{sch}")
        sdir = os.path.join(base, sch)
        smeta: dict = {"tables": [], "semantic_views": [], "agents": [], "streamlits": [], "notebooks": [], "stages": []}

        # ---- schema DDL ----
        if do_ddl:
            ddl = fetch_one_val(conn, "SELECT GET_DDL('SCHEMA', %(n)s, TRUE)", {"n": f"{src_db}.{sch}"})
            if ddl:
                if not dry_run:
                    _write_local(os.path.join(sdir, "_schema_ddl.sql"), ddl)
                st.success("Schema DDL saved.")
            else:
                st.warning("No schema DDL returned.")

        # ---- semantic views ----
        if do_semantic:
            svs = list_semantic_views(conn, src_db, sch)
            for sv in svs:
                sv_ddl = get_semantic_view_ddl(conn, src_db, sch, sv)
                if sv_ddl and not dry_run:
                    _write_local(os.path.join(sdir, "semantic_views", f"{sv}.sql"), sv_ddl)
                smeta["semantic_views"].append(sv)
            st.success(f"Semantic views backed up: {len(svs)}")

        # ---- agents ----
        if do_agents:
            agents = list_agents(conn, src_db, sch)
            for a in agents:
                info = describe_agent_row(conn, src_db, sch, a)
                spec = info.get("agent_spec") or info.get("specification") or ""
                sql = create_or_replace_agent_sql(src_db, sch, a, info.get("comment"), info.get("profile"), str(spec))
                if not dry_run:
                    _write_local(os.path.join(sdir, "agents", f"{a}.sql"), sql)
                smeta["agents"].append(a)
            st.success(f"Agents backed up: {len(agents)}")

        # ---- streamlits ----
        if do_streamlits:
            apps = list_streamlits(conn, src_db, sch)
            for app in apps:
                app_ddl = get_streamlit_ddl(conn, src_db, sch, app)
                if app_ddl and not dry_run:
                    _write_local(os.path.join(sdir, "streamlits", f"{app}.sql"), app_ddl)
                smeta["streamlits"].append(app)
            st.success(f"Streamlits backed up: {len(apps)}")

        # ---- notebooks ----
        if do_notebooks:
            nbs = list_notebooks(conn, src_db, sch)
            for nb_meta in nbs:
                nb = nb_meta["name"]
                if not dry_run:
                    nb_dir = os.path.join(sdir, "notebooks")
                    get_notebook_ipynb_to_local(conn, src_db, sch, nb, nb_dir)
                smeta["notebooks"].append(nb_meta)
            st.success(f"Notebooks backed up: {len(nbs)}")

        # ---- stage files (user-created stages) ----
        if do_stage_files:
            stages = list_user_stages(conn, src_db, sch)
            backed = 0
            for stg_meta in stages:
                stg_name = stg_meta["name"]
                stg_type = stg_meta["type"]
                # Only back up internal stages (we can GET from them); external stages hold
                # data outside Snowflake so there's nothing to download.
                if "INTERNAL" not in stg_type:
                    st.info(f"  Stage {stg_name}: type={stg_type} — skipping (external).")
                    smeta["stages"].append({"name": stg_name, "type": stg_type, "files_backed_up": False})
                    continue
                stg_ddl = get_stage_ddl(conn, src_db, sch, stg_name)
                stg_dir = os.path.join(sdir, "stages", stg_name)
                if not dry_run:
                    if stg_ddl:
                        _write_local(os.path.join(sdir, "stages", f"{stg_name}.ddl.sql"), stg_ddl)
                    files_dir = os.path.join(stg_dir, "files")
                    downloaded = get_stage_files_to_local(conn, src_db, sch, stg_name, files_dir)
                    st.write(f"  Stage {stg_name}: {len(downloaded)} files downloaded.")
                smeta["stages"].append({"name": stg_name, "type": stg_type, "files_backed_up": True})
                backed += 1
            st.success(f"Stage files backed up: {backed}/{len(stages)} stages")

        # ---- table data via internal stage -> GET -> local ----
        if do_data:
            tables = exec_sql(conn, f"SHOW TABLES IN SCHEMA {fq(src_db, sch)}")
            table_names = [r[1] for r in tables if len(r) > 1]
            for t in table_names:
                cols = get_table_columns(conn, src_db, sch, t)
                if not cols:
                    continue
                sub = f"local_backup/{run_id}/{src_db}/{sch}/{t}"
                stage_path = f"{stage_ref}/{sub}/"
                local_tbl = os.path.join(sdir, "data", t)

                if not dry_run:
                    os.makedirs(local_tbl, exist_ok=True)
                    col_list = ", ".join(qident(c) for c in cols)
                    unload = (
                        f"COPY INTO {sql_string_literal(stage_path)} "
                        f"FROM (SELECT {col_list} FROM {fq(src_db, sch, t)}) "
                        f"FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' "
                        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' COMPRESSION=GZIP) "
                        f"OVERWRITE=TRUE"
                    )
                    exec_sql(conn, unload)

                    # LIST the stage path first — COPY INTO produces no files for empty tables.
                    ensure_session_namespace(conn, mig_db, mig_schema)
                    listed = exec_sql(conn, f"LIST {sql_string_literal(stage_path)}")
                    if listed:
                        exec_sql(conn, f"GET {sql_string_literal(stage_path)} {sql_string_literal(_to_file_uri(local_tbl))} OVERWRITE=TRUE")
                        exec_sql(conn, f"REMOVE {sql_string_literal(stage_path)}")
                    else:
                        st.info(f"  Table {t}: 0 rows — nothing to download.")

                smeta["tables"].append({"name": t, "columns": cols})
                st.write(f"  Table {t}: {len(cols)} columns")
            st.success(f"Table data backed up: {len(table_names)} tables")

        manifest["schemas"][sch] = smeta

    if not dry_run:
        _write_local(os.path.join(base, "manifest.json"), json.dumps(manifest, indent=2))
    st.success(f"Backup complete -> {base}")


def run_local_restore(
    conn,
    backup_path: str,
    tgt_db: str,
    mig_db: str,
    mig_schema: str,
    int_stage_name: str,
    do_ddl: bool,
    do_semantic: bool,
    do_agents: bool,
    do_streamlits: bool,
    do_notebooks: bool,
    do_data: bool,
    do_stage_files: bool = False,
    rewrite_db: bool = True,
    init_streamlit_live: bool = True,
    tgt_query_wh: Optional[str] = None,
    dry_run: bool = False,
):
    """Import schemas from a local backup directory into a Snowflake account."""
    manifest_file = os.path.join(backup_path, "manifest.json")
    if not os.path.isfile(manifest_file):
        st.error(f"manifest.json not found in {backup_path}")
        return
    manifest = json.loads(_read_local(manifest_file))
    src_db = manifest.get("database", "")
    schemas_meta = manifest.get("schemas", {})

    if not schemas_meta:
        st.warning("Manifest contains no schemas.")
        return

    if not dry_run:
        exec_sql(conn, f"CREATE DATABASE IF NOT EXISTS {qident(tgt_db)}")
        if do_data or do_notebooks:
            bootstrap_db_schema(conn, mig_db, mig_schema)
            ensure_internal_stage(conn, mig_db, mig_schema, int_stage_name)

    stage_ref = f"@{mig_db}.{mig_schema}.{int_stage_name}"

    for sch, smeta in schemas_meta.items():
        st.subheader(f"Restore -> {tgt_db}.{sch}")
        tgt_schema = sch
        sdir = os.path.join(backup_path, sch)

        # ---- schema DDL ----
        if do_ddl:
            ddl_file = os.path.join(sdir, "_schema_ddl.sql")
            if os.path.isfile(ddl_file):
                ddl = _read_local(ddl_file)
                if rewrite_db and tgt_db != src_db:
                    ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
                if not dry_run:
                    exec_script(conn, strip_streamlit_from_schema_ddl(ddl), remove_comments=True)
                st.success("Schema DDL applied.")

        # ---- semantic views ----
        if do_semantic:
            sv_dir = os.path.join(sdir, "semantic_views")
            count = 0
            if os.path.isdir(sv_dir):
                for f in sorted(os.listdir(sv_dir)):
                    if not f.endswith(".sql"):
                        continue
                    ddl = _read_local(os.path.join(sv_dir, f))
                    if rewrite_db and tgt_db != src_db:
                        ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
                    if not dry_run:
                        exec_script(conn, ddl, remove_comments=True)
                    count += 1
            st.success(f"Semantic views restored: {count}")

        # ---- agents ----
        if do_agents:
            ag_dir = os.path.join(sdir, "agents")
            count = 0
            if os.path.isdir(ag_dir):
                for f in sorted(os.listdir(ag_dir)):
                    if not f.endswith(".sql"):
                        continue
                    sql = _read_local(os.path.join(ag_dir, f))
                    if rewrite_db and tgt_db != src_db:
                        sql = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, sql)
                    if not dry_run:
                        exec_sql(conn, sql)
                    count += 1
            st.success(f"Agents restored: {count}")

        # ---- streamlits ----
        if do_streamlits:
            sl_dir = os.path.join(sdir, "streamlits")
            count = 0
            if os.path.isdir(sl_dir):
                for f in sorted(os.listdir(sl_dir)):
                    if not f.endswith(".sql"):
                        continue
                    ddl = _read_local(os.path.join(sl_dir, f))
                    if rewrite_db and tgt_db != src_db:
                        ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
                    if not dry_run:
                        exec_script(conn, ddl, remove_comments=True)
                        if init_streamlit_live:
                            app_name = f[:-4]
                            try:
                                exec_sql(conn, f"ALTER STREAMLIT {fq(tgt_db, tgt_schema, app_name)} ADD LIVE VERSION FROM LAST")
                            except Exception:
                                pass
                    count += 1
            st.success(f"Streamlits restored: {count}")

        # ---- notebooks ----
        if do_notebooks:
            nb_dir = os.path.join(sdir, "notebooks")
            nb_list = smeta.get("notebooks", [])
            count = 0
            if os.path.isdir(nb_dir) and nb_list:
                sub = f"local_restore/{tgt_db}/{tgt_schema}"
                from_folder = f"{stage_ref}/{sub}/"
                for nb_meta in nb_list:
                    nb_name = nb_meta["name"]
                    main_file = f"{nb_name}.ipynb"
                    local_file = os.path.join(nb_dir, main_file)
                    if not os.path.isfile(local_file):
                        st.warning(f"Notebook file not found: {main_file}")
                        continue
                    qwh = tgt_query_wh or nb_meta.get("query_warehouse")
                    comment = nb_meta.get("comment")
                    if not dry_run:
                        put_local_file_to_internal_stage(conn, local_file, stage_ref, sub)
                        sql = build_create_notebook_from_stage_sql(
                            tgt_db, tgt_schema, nb_name, from_folder, main_file, qwh, comment,
                        )
                        exec_sql(conn, sql)
                    count += 1
            st.success(f"Notebooks restored: {count}")

        # ---- stage files ----
        if do_stage_files:
            stg_list = smeta.get("stages", [])
            count = 0
            for stg_info in stg_list:
                stg_name = stg_info["name"]
                if not stg_info.get("files_backed_up", False):
                    st.info(f"  Stage {stg_name}: no files to restore (external or empty).")
                    continue
                stg_ddl_file = os.path.join(sdir, "stages", f"{stg_name}.ddl.sql")
                stg_files_dir = os.path.join(sdir, "stages", stg_name, "files")

                # Re-create the stage from saved DDL (or CREATE STAGE IF NOT EXISTS)
                if not dry_run:
                    if os.path.isfile(stg_ddl_file):
                        ddl = _read_local(stg_ddl_file)
                        if rewrite_db and tgt_db != src_db:
                            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
                        try:
                            exec_script(conn, ddl, remove_comments=True)
                        except Exception as e:
                            st.warning(f"  Stage {stg_name} DDL failed ({e}), creating fallback.")
                            exec_sql(conn, f"CREATE STAGE IF NOT EXISTS {fq(tgt_db, tgt_schema, stg_name)}")
                    else:
                        exec_sql(conn, f"CREATE STAGE IF NOT EXISTS {fq(tgt_db, tgt_schema, stg_name)}")

                    # Upload files back to the stage
                    if os.path.isdir(stg_files_dir):
                        put_local_dir_to_stage(conn, stg_files_dir, tgt_db, tgt_schema, stg_name)

                count += 1
                st.write(f"  Stage {stg_name}: restored.")
            st.success(f"Stage files restored: {count}")

        # ---- table data via local -> PUT -> COPY INTO ----
        if do_data:
            tables_info = smeta.get("tables", [])
            count = 0
            for tbl in tables_info:
                t = tbl["name"]
                cols = tbl["columns"]
                data_dir = os.path.join(sdir, "data", t)
                if not os.path.isdir(data_dir):
                    continue
                gz_files = [gf for gf in os.listdir(data_dir) if gf.endswith(".gz")]
                if not gz_files:
                    continue

                sub = f"local_restore/{tgt_db}/{tgt_schema}/{t}"
                load_from = f"{stage_ref}/{sub}/"

                if not dry_run:
                    for gf in gz_files:
                        local_csv = os.path.join(data_dir, gf)
                        exec_sql(
                            conn,
                            f"PUT {sql_string_literal(_to_file_uri(local_csv))} "
                            f"{stage_ref}/{sub}/ "
                            f"OVERWRITE=TRUE AUTO_COMPRESS=FALSE",
                        )

                    tgt_table = fq(tgt_db, tgt_schema, t)
                    col_list = ", ".join(qident(c) for c in cols)
                    sel = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
                    load_sql = (
                        f"COPY INTO {tgt_table} ({col_list}) "
                        f"FROM (SELECT {sel} FROM {sql_string_literal(load_from)}) "
                        f"FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=',' "
                        f"FIELD_OPTIONALLY_ENCLOSED_BY='\"' COMPRESSION=GZIP) "
                        f"ON_ERROR='ABORT_STATEMENT'"
                    )
                    exec_sql(conn, load_sql)
                    exec_sql(conn, f"REMOVE {sql_string_literal(load_from)}")

                count += 1
                st.write(f"  Table {t}: {len(cols)} cols, {len(gz_files)} files")
            st.success(f"Table data restored: {count} tables")

    st.success("Local restore complete.")


# ----------------------------
# Streamlit UI
# ----------------------------
st.set_page_config(page_title="Snowflake Migrator (Azure Stage)", layout="wide")
st.title("Snowflake → Snowflake Migrator (Azure stage + semantic views + agents + streamlits + notebooks)")

with st.sidebar:
    st.header("Source connection")
    src_account = st.text_input("Source account", placeholder="xy12345.ap-south-1.aws")
    src_user = st.text_input("Source user")
    src_password = st.text_input("Source password", type="password")
    src_role = st.text_input("Source role", value="ACCOUNTADMIN")
    src_wh = st.text_input("Source warehouse", value="")
    src_passcode = st.text_input("Source MFA TOTP passcode", type="password", help="Leave blank if MFA is not enabled")

    st.divider()
    st.header("Target connection")
    tgt_account = st.text_input("Target account", placeholder="ab67890.ap-south-1.aws")
    tgt_user = st.text_input("Target user")
    tgt_password = st.text_input("Target password", type="password")
    tgt_role = st.text_input("Target role", value="ACCOUNTADMIN")
    tgt_wh = st.text_input("Target warehouse", value="")
    tgt_passcode = st.text_input("Target MFA TOTP passcode", type="password", help="Leave blank if MFA is not enabled")

    st.divider()
    st.header("Utility namespace")
    mig_db = st.text_input("Utility DB", value=DEFAULT_MIG_DB)
    mig_schema = st.text_input("Utility Schema", value=DEFAULT_MIG_SCHEMA)
    stage_name = st.text_input("Stage name", value=DEFAULT_STAGE)

    st.divider()
    st.header("Azure external stage")
    integration_name = st.text_input("Storage integration name", value=DEFAULT_INTEGRATION)
    azure_tenant_id = st.text_input("Azure tenant id (GUID)", value="")
    storage_account = st.text_input("Azure storage account name", value="")
    container = st.text_input("Azure container name", value="")
    prefix = st.text_input("Folder/prefix", value="exports/")

    st.divider()
    st.header("Migration copy")
    stage_prefix = st.text_input("Stage folder prefix for runs", value="sf_migration")

    st.divider()
    connect_both_btn = st.button("Connect Both")
    col_csrc, col_ctgt = st.columns(2)
    with col_csrc:
        connect_src_btn = st.button("Connect Source Only")
    with col_ctgt:
        connect_tgt_btn = st.button("Connect Target Only")

if "src_conn" not in st.session_state:
    st.session_state.src_conn = None
    st.session_state.tgt_conn = None

if connect_both_btn:
    try:
        st.session_state.src_conn = connect_sf(
            src_account, src_user, src_password,
            role=src_role, warehouse=src_wh or None,
            passcode=src_passcode or None,
        )
        st.session_state.tgt_conn = connect_sf(
            tgt_account, tgt_user, tgt_password,
            role=tgt_role, warehouse=tgt_wh or None,
            passcode=tgt_passcode or None,
        )
        st.success("Both connected.")
    except Exception as e:
        st.session_state.src_conn = None
        st.session_state.tgt_conn = None
        st.error(f"Connection failed: {e}")

if connect_src_btn:
    try:
        st.session_state.src_conn = connect_sf(
            src_account, src_user, src_password,
            role=src_role, warehouse=src_wh or None,
            passcode=src_passcode or None,
        )
        st.success("Source connected.")
    except Exception as e:
        st.session_state.src_conn = None
        st.error(f"Source connection failed: {e}")

if connect_tgt_btn:
    try:
        st.session_state.tgt_conn = connect_sf(
            tgt_account, tgt_user, tgt_password,
            role=tgt_role, warehouse=tgt_wh or None,
            passcode=tgt_passcode or None,
        )
        st.success("Target connected.")
    except Exception as e:
        st.session_state.tgt_conn = None
        st.error(f"Target connection failed: {e}")

src_conn = st.session_state.src_conn
tgt_conn = st.session_state.tgt_conn

# Show connection status
_src_status = "\u2705 Source connected" if src_conn else "\u274c Source not connected"
_tgt_status = "\u2705 Target connected" if tgt_conn else "\u274c Target not connected"
st.caption(f"{_src_status}  |  {_tgt_status}")

# Compute the Azure stage URL (may be invalid until inputs are filled)
try:
    stage_url = build_azure_stage_url(storage_account, container, prefix)
except Exception as e:
    stage_url = ""
    st.error(f"Azure stage URL invalid: {e}")

st.divider()

# ----------------------------
# Local Backup & Restore (independent of Azure / both connections)
# ----------------------------
st.header("Local Backup & Restore")

local_bk_tab, local_rs_tab = st.tabs(["Backup to Local", "Restore from Local"])

with local_bk_tab:
    st.markdown("Export schemas (DDL + data + objects) from **source** Snowflake to a local directory. "
                "No Azure stage required — data flows via internal stage and GET to your machine. "
                "**Requires source connection only.**")
    if not src_conn:
        st.warning("Connect to the **source** account first (sidebar).")
    else:
        bk_dir = st.text_input("Local backup directory", value=os.path.join(tempfile.gettempdir(), "sf_backup"))
        bk_int_stage = st.text_input("Internal stage (created on source)", value=DEFAULT_LOCAL_INT_STAGE, key="bk_stage")
        bk_auto_id = st.checkbox("Auto-generate new run id each backup", value=True, key="bk_auto_id")
        bk_run_id = st.text_input("Backup run id (ignored if auto-generate is on)", value=str(uuid.uuid4())[:8], key="bk_run_id")

        # Database / schema selection for backup (uses source connection)
        bk_db_rows = exec_sql(src_conn, "SHOW DATABASES")
        bk_db_names = [r[1] for r in bk_db_rows if len(r) > 1]
        bk_src_db = st.selectbox("Source database", bk_db_names, key="bk_src_db")

        bk_schema_rows = exec_sql(src_conn, f"SHOW SCHEMAS IN DATABASE {qident(bk_src_db)}")
        bk_schema_names = [r[1] for r in bk_schema_rows if len(r) > 1]
        bk_schema_names = [s for s in bk_schema_names if s and s.upper() not in SKIP_SCHEMAS]
        bk_selected_schemas = st.multiselect(
            "Schemas to back up",
            bk_schema_names,
            default=[s for s in bk_schema_names if s.upper() == "PUBLIC"] or [],
            key="bk_schemas",
        )

        bk_c1, bk_c2, bk_c3, bk_c4, bk_c5, bk_c6, bk_c7 = st.columns(7)
        with bk_c1:
            bk_ddl = st.checkbox("Schema DDL", value=True, key="bk_ddl")
        with bk_c2:
            bk_sv = st.checkbox("Semantic views", value=True, key="bk_sv")
        with bk_c3:
            bk_ag = st.checkbox("Agents", value=True, key="bk_ag")
        with bk_c4:
            bk_sl = st.checkbox("Streamlits", value=True, key="bk_sl")
        with bk_c5:
            bk_nb = st.checkbox("Notebooks", value=True, key="bk_nb")
        with bk_c6:
            bk_data = st.checkbox("Table data", value=True, key="bk_data")
        with bk_c7:
            bk_stg = st.checkbox("Stage files", value=True, key="bk_stg")
        bk_dry = st.checkbox("Dry run", value=False, key="bk_dry")

        if st.button("Run Backup Now"):
            if not bk_selected_schemas:
                st.error("Select at least one schema.")
            else:
                actual_run_id = str(uuid.uuid4())[:8] if bk_auto_id else bk_run_id
                st.info(f"Backup run id: **{actual_run_id}**")
                try:
                    run_local_backup(
                        conn=src_conn,
                        src_db=bk_src_db,
                        schemas=bk_selected_schemas,
                        backup_root=bk_dir,
                        mig_db=mig_db,
                        mig_schema=mig_schema,
                        int_stage_name=bk_int_stage,
                        run_id=actual_run_id,
                        do_ddl=bk_ddl,
                        do_semantic=bk_sv,
                        do_agents=bk_ag,
                        do_streamlits=bk_sl,
                        do_notebooks=bk_nb,
                        do_data=bk_data,
                        do_stage_files=bk_stg,
                        dry_run=bk_dry,
                    )
                except Exception as e:
                    st.error(f"Backup failed: {e}")

with local_rs_tab:
    st.markdown("Import schemas from a local backup into **target** Snowflake account. "
                "Upload happens via PUT to an internal stage, then COPY INTO. "
                "**Requires target connection only.**")
    if not tgt_conn:
        st.warning("Connect to the **target** account first (sidebar).")
    else:
        rs_path = st.text_input(
            "Backup path (folder containing manifest.json)",
            value="",
            help="e.g. C:/Users/.../sf_backup/<run_id>/<DATABASE>",
        )
        rs_tgt_db = st.text_input("Target database name", value="", key="rs_tgt_db")
        rs_int_stage = st.text_input("Internal stage (created on target)", value=DEFAULT_LOCAL_INT_STAGE, key="rs_stage")

        rs_c1, rs_c2, rs_c3, rs_c4, rs_c5, rs_c6, rs_c7 = st.columns(7)
        with rs_c1:
            rs_ddl = st.checkbox("Schema DDL", value=True, key="rs_ddl")
        with rs_c2:
            rs_sv = st.checkbox("Semantic views", value=True, key="rs_sv")
        with rs_c3:
            rs_ag = st.checkbox("Agents", value=True, key="rs_ag")
        with rs_c4:
            rs_sl = st.checkbox("Streamlits", value=True, key="rs_sl")
        with rs_c5:
            rs_nb = st.checkbox("Notebooks", value=True, key="rs_nb")
        with rs_c6:
            rs_data = st.checkbox("Table data", value=True, key="rs_data")
        with rs_c7:
            rs_stg = st.checkbox("Stage files", value=True, key="rs_stg")

        rs_rewrite = st.checkbox("Rewrite DB name in DDL", value=True, key="rs_rewrite")
        rs_sl_live = st.checkbox("Init Streamlit LIVE version", value=True, key="rs_sl_live")
        rs_dry = st.checkbox("Dry run", value=False, key="rs_dry")

        if st.button("Run Restore Now"):
            if not rs_path.strip():
                st.error("Please enter the backup path.")
            elif not rs_tgt_db.strip():
                st.error("Please enter the target database name.")
            else:
                try:
                    run_local_restore(
                        conn=tgt_conn,
                        backup_path=rs_path.strip(),
                        tgt_db=rs_tgt_db.strip(),
                        mig_db=mig_db,
                        mig_schema=mig_schema,
                        int_stage_name=rs_int_stage,
                        do_ddl=rs_ddl,
                        do_semantic=rs_sv,
                        do_agents=rs_ag,
                        do_streamlits=rs_sl,
                        do_notebooks=rs_nb,
                        do_data=rs_data,
                        do_stage_files=rs_stg,
                        rewrite_db=rs_rewrite,
                        init_streamlit_live=rs_sl_live,
                        tgt_query_wh=tgt_wh or None,
                        dry_run=rs_dry,
                    )
                except Exception as e:
                    st.error(f"Restore failed: {e}")

st.divider()

# ----------------------------
# Azure Migration (requires both connections)
# ----------------------------
if not (src_conn and tgt_conn):
    st.info("Connect **both** source and target accounts to use Azure migration and setup sections below.")
    st.stop()

st.subheader("Computed Azure stage URL")
st.code(stage_url)


# ----------------------------
# Setup (always latest)
# ----------------------------
st.header("Setup (always latest)")

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    if st.button("Ensure DB+Schema (both)"):
        try:
            bootstrap_db_schema(src_conn, mig_db, mig_schema)
            bootstrap_db_schema(tgt_conn, mig_db, mig_schema)
            st.success("DB+Schema ensured in both.")
        except Exception as e:
            st.error(e)

with c2:
    if st.button("Ensure Integration (both)"):
        try:
            ensure_storage_integration_azure(src_conn, integration_name, azure_tenant_id, [stage_url])
            ensure_storage_integration_azure(tgt_conn, integration_name, azure_tenant_id, [stage_url])
            st.success("Integration ensured (created/altered) in both.")
        except Exception as e:
            st.error(e)

with c3:
    if st.button("Show Consent URLs"):
        try:
            s = describe_storage_integration(src_conn, integration_name)
            t = describe_storage_integration(tgt_conn, integration_name)
            st.subheader("Source AZURE_CONSENT_URL")
            st.code(str(s.get("AZURE_CONSENT_URL", "")))
            st.subheader("Target AZURE_CONSENT_URL")
            st.code(str(t.get("AZURE_CONSENT_URL", "")))
            st.subheader("AZURE_MULTI_TENANT_APP_NAME")
            st.code(str(s.get("AZURE_MULTI_TENANT_APP_NAME", "")))
        except Exception as e:
            st.error(e)

with c4:
    if st.button("Ensure Stage (both)"):
        try:
            ensure_storage_integration_azure(src_conn, integration_name, azure_tenant_id, [stage_url])
            ensure_storage_integration_azure(tgt_conn, integration_name, azure_tenant_id, [stage_url])
            ensure_external_stage_azure(src_conn, mig_db, mig_schema, stage_name, stage_url, integration_name)
            ensure_external_stage_azure(tgt_conn, mig_db, mig_schema, stage_name, stage_url, integration_name)
            st.success("Stage ensured in both.")
        except Exception as e:
            st.error(e)

with c5:
    if st.button("Inspect Stage (both)"):
        try:
            s = describe_stage(src_conn, mig_db, mig_schema, stage_name)
            t = describe_stage(tgt_conn, mig_db, mig_schema, stage_name)
            st.subheader("Source stage")
            st.write({"URL": s.get("URL"), "STORAGE_INTEGRATION": s.get("STORAGE_INTEGRATION")})
            st.subheader("Target stage")
            st.write({"URL": t.get("URL"), "STORAGE_INTEGRATION": t.get("STORAGE_INTEGRATION")})
        except Exception as e:
            st.error(e)

if st.button("Test LIST @stage (both)"):
    try:
        src_list = list_stage(src_conn, mig_db, mig_schema, stage_name)
        tgt_list = list_stage(tgt_conn, mig_db, mig_schema, stage_name)
        st.subheader("Source LIST (first 50)")
        st.write(src_list[:50])
        st.subheader("Target LIST (first 50)")
        st.write(tgt_list[:50])
        st.success("LIST executed.")
    except Exception as e:
        st.error(f"LIST failed: {e}")

st.divider()


# ----------------------------
# Migration
# ----------------------------
st.header("Migration (DDL + semantic views + agents + streamlits + notebooks + CSV data copy)")

db_rows = exec_sql(src_conn, "SHOW DATABASES")
db_names = [r[1] for r in db_rows if len(r) > 1]
src_db = st.selectbox("Source database", db_names)

schema_rows = exec_sql(src_conn, f"SHOW SCHEMAS IN DATABASE {qident(src_db)}")
schema_names = [r[1] for r in schema_rows if len(r) > 1]
schema_names = [s for s in schema_names if s and s.upper() not in SKIP_SCHEMAS]
selected_schemas = st.multiselect(
    "Schemas to migrate",
    schema_names,
    default=[s for s in schema_names if s.upper() == "PUBLIC"] or [],
)

tgt_db = st.text_input("Target database name", value=src_db)

col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
with col1:
    migrate_ddl = st.checkbox("Migrate schema DDL", value=True)
with col2:
    migrate_semantic = st.checkbox("Migrate semantic views", value=True)
with col3:
    migrate_agents_flag = st.checkbox("Migrate Cortex Agents", value=True)
with col4:
    migrate_streamlits_flag = st.checkbox("Migrate Streamlit apps", value=True)
with col5:
    migrate_notebooks_flag = st.checkbox("Migrate Notebooks (GET->PUT->CREATE)", value=True)
with col6:
    copy_data = st.checkbox("Copy data (CSV+GZIP)", value=True)
with col7:
    dry_run = st.checkbox("Dry run", value=False)

rewrite_db = st.checkbox("Rewrite DB name inside DDL/specs when target DB differs", value=True)
init_streamlit_live = st.checkbox("Initialize Streamlit LIVE version", value=True)
run_id = st.text_input("Run id", value=str(uuid.uuid4())[:8])

st.subheader("Notebook migration settings")
nb_int_stage_name = st.text_input(
    "Target INTERNAL stage for notebooks (.ipynb uploads)",
    value=DEFAULT_NB_INT_STAGE,
    disabled=not migrate_notebooks_flag,
)
copy_nb_comment = st.checkbox(
    "Copy notebook COMMENT",
    value=False,
    disabled=not migrate_notebooks_flag,
)
use_source_nb_query_wh_if_target_empty = st.checkbox(
    "If Target warehouse is empty, try to reuse Source notebook QUERY_WAREHOUSE",
    value=False,
    disabled=not migrate_notebooks_flag,
)

if st.button("Run migration now"):
    # Precheck infra (integration + Azure stage used for data copy)
    try:
        ensure_storage_integration_azure(src_conn, integration_name, azure_tenant_id, [stage_url])
        ensure_storage_integration_azure(tgt_conn, integration_name, azure_tenant_id, [stage_url])
        ensure_external_stage_azure(src_conn, mig_db, mig_schema, stage_name, stage_url, integration_name)
        ensure_external_stage_azure(tgt_conn, mig_db, mig_schema, stage_name, stage_url, integration_name)
    except Exception as e:
        st.error(f"Pre-check failed (integration/stage): {e}")
        st.stop()

    if migrate_ddl and not dry_run:
        exec_sql(tgt_conn, f"CREATE DATABASE IF NOT EXISTS {qident(tgt_db)}")

    stage_ref = f"@{mig_db}.{mig_schema}.{stage_name}"

    for sch in selected_schemas:
        st.subheader(f"{src_db}.{sch}")
        tgt_schema = sch

        # --- Schema DDL (strip Streamlit statements so schema deploy doesn't crash)
        ddl = fetch_one_val(
            src_conn,
            "SELECT GET_DDL('SCHEMA', %(n)s, TRUE)",
            {"n": f"{src_db}.{sch}"},
        )
        if not ddl:
            st.warning("No schema DDL returned; skipping schema.")
            continue

        if rewrite_db and tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)

        st.text_area("Schema DDL preview", ddl, height=200)

        if migrate_ddl and not dry_run:
            ddl_clean = strip_streamlit_from_schema_ddl(ddl)
            exec_script(tgt_conn, ddl_clean, remove_comments=True)

        # --- Semantic views
        if migrate_semantic:
            try:
                migrate_semantic_views(
                    src_conn, tgt_conn,
                    src_db, sch,
                    tgt_db, tgt_schema,
                    dry_run=dry_run,
                    rewrite_db=rewrite_db,
                )
                st.success("Semantic views migrated.")
            except Exception as e:
                st.error(f"Semantic view migration failed: {e}")

        # --- Cortex Agents
        if migrate_agents_flag:
            try:
                migrate_agents(
                    src_conn, tgt_conn,
                    src_db, sch,
                    tgt_db, tgt_schema,
                    dry_run=dry_run,
                    rewrite_db=rewrite_db,
                )
                st.success("Cortex Agents migrated.")
            except Exception as e:
                st.error(f"Agent migration failed: {e}")

        # --- Streamlit apps
        if migrate_streamlits_flag:
            try:
                migrate_streamlits(
                    src_conn, tgt_conn,
                    src_db, sch,
                    tgt_db, tgt_schema,
                    dry_run=dry_run,
                    rewrite_db=rewrite_db,
                    initialize_live=init_streamlit_live,
                )
                st.success("Streamlit apps migrated.")
            except Exception as e:
                st.error(f"Streamlit migration failed: {e}")

        # --- Notebooks (FULLY automated: GET -> local -> PUT -> CREATE NOTEBOOK)
        if migrate_notebooks_flag:
            try:
                migrate_notebooks_fully_automated(
                    src_conn, tgt_conn,
                    src_db, sch,
                    tgt_db, tgt_schema,
                    mig_db, mig_schema,
                    nb_int_stage_name=nb_int_stage_name,
                    stage_prefix=stage_prefix,
                    run_id=run_id,
                    tgt_query_wh=(tgt_wh or None),
                    use_source_comment=copy_nb_comment,
                    use_source_query_wh_if_target_empty=use_source_nb_query_wh_if_target_empty,
                    dry_run=dry_run,
                )
                st.success("Notebooks migrated (GET->PUT->CREATE).")
            except Exception as e:
                st.error(f"Notebook migration failed: {e}")

        # --- Data copy
        if copy_data:
            tables = exec_sql(src_conn, f"SHOW TABLES IN SCHEMA {fq(src_db, sch)}")
            table_names = [r[1] for r in tables if len(r) > 1]

            for t in table_names:
                cols = get_table_columns(src_conn, src_db, sch, t)
                if not cols:
                    continue

                path = f"{stage_prefix}/{run_id}/{src_db}/{sch}/{t}/"
                unload_path = f"{stage_ref}/{path}"
                load_path = f"{stage_ref}/{path}"

                unload_sql = build_csv_unload_sql(unload_path, src_db, sch, t, cols)
                load_sql = build_csv_load_sql(load_path, tgt_db, sch, t, cols)

                with st.expander(f"Copy {sch}.{t}", expanded=False):
                    st.code(unload_sql, language="sql")
                    st.code(load_sql, language="sql")

                if not dry_run:
                    exec_sql(src_conn, unload_sql)
                    exec_sql(tgt_conn, load_sql)

    st.success("Done (check errors above if any).")
