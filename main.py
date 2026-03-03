import re
import uuid
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


# ----------------------------
# Snowflake helpers
# ----------------------------
def connect_sf(account, user, password, role=None, warehouse=None):
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
    return snowflake.connector.connect(**kwargs)


def exec_sql(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        try:
            return cur.fetchall()
        except Exception:
            return []


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
def normalize_prefix(prefix: str) -> str:
    p = (prefix or "").strip().lstrip("/")
    if p and not p.endswith("/"):
        p += "/"
    return p


def build_azure_stage_url(storage_account: str, container: str, prefix: str) -> str:
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

    if not stage_url.strip() or stage_url.strip().lower() == "azure://.blob.core.windows.net//":
        raise ValueError("Stage URL is empty/invalid. Fill storage account + container + prefix first.")

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
    return exec_sql(conn, f"LIST @{qident(stage_name)}")


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

    # IMPORTANT: If stage/path includes spaces/special chars, Snowflake requires quoting. [COPY INTO <location>]
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

    # IMPORTANT: If stage/path includes spaces/special chars, Snowflake requires quoting. [COPY INTO <table>]
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
# Streamlit UI
# ----------------------------
st.set_page_config(page_title="Snowflake Migrator (Azure Stage)", layout="wide")
st.title("Snowflake → Snowflake Migrator (Azure stage + semantic views + agents + streamlits)")

with st.sidebar:
    st.header("Source connection")
    src_account = st.text_input("Source account", placeholder="xy12345.ap-south-1.aws")
    src_user = st.text_input("Source user")
    src_password = st.text_input("Source password", type="password")
    src_role = st.text_input("Source role", value="ACCOUNTADMIN")
    src_wh = st.text_input("Source warehouse", value="")

    st.divider()
    st.header("Target connection")
    tgt_account = st.text_input("Target account", placeholder="ab67890.ap-south-1.aws")
    tgt_user = st.text_input("Target user")
    tgt_password = st.text_input("Target password", type="password")
    tgt_role = st.text_input("Target role", value="ACCOUNTADMIN")
    tgt_wh = st.text_input("Target warehouse", value="")

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
    connect_btn = st.button("Connect")

if "src_conn" not in st.session_state:
    st.session_state.src_conn = None
    st.session_state.tgt_conn = None

if connect_btn:
    try:
        st.session_state.src_conn = connect_sf(
            src_account, src_user, src_password,
            role=src_role, warehouse=src_wh or None
        )
        st.session_state.tgt_conn = connect_sf(
            tgt_account, tgt_user, tgt_password,
            role=tgt_role, warehouse=tgt_wh or None
        )
        st.success("Connected.")
    except Exception as e:
        st.session_state.src_conn = None
        st.session_state.tgt_conn = None
        st.error(f"Connection failed: {e}")

src_conn = st.session_state.src_conn
tgt_conn = st.session_state.tgt_conn
if not (src_conn and tgt_conn):
    st.stop()

stage_url = build_azure_stage_url(storage_account, container, prefix)
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
st.header("Migration (DDL + semantic views + agents + streamlits + CSV data copy)")

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

col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    migrate_ddl = st.checkbox("Migrate schema DDL", value=True)
with col2:
    migrate_semantic = st.checkbox("Migrate semantic views", value=True)
with col3:
    migrate_agents_flag = st.checkbox("Migrate Cortex Agents", value=True)
with col4:
    migrate_streamlits_flag = st.checkbox("Migrate Streamlit apps", value=True)
with col5:
    copy_data = st.checkbox("Copy data (CSV+GZIP)", value=True)
with col6:
    dry_run = st.checkbox("Dry run", value=False)

rewrite_db = st.checkbox("Rewrite DB name inside DDL/specs when target DB differs", value=True)
init_streamlit_live = st.checkbox("Initialize Streamlit LIVE version", value=True)
run_id = st.text_input("Run id", value=str(uuid.uuid4())[:8])

if st.button("Run migration now"):
    # Precheck infra (integration + stage)
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

        # --- Data copy
        if copy_data:
            tables = exec_sql(src_conn, f"SHOW TABLES IN SCHEMA {fq(src_db, sch)}")
            table_names = [r[1] for r in tables if len(r) > 1]

            for t in table_names:
                cols = get_table_columns(src_conn, src_db, sch, t)
                if not cols:
                    continue

                # NOTE: keep original table name in folder; quoting in COPY handles spaces.
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
