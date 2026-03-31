import re
from typing import Optional


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fq(db: str, schema: str, obj: str | None = None) -> str:
    if obj is None:
        return f"{qident(db)}.{qident(schema)}"
    return f"{qident(db)}.{qident(schema)}.{qident(obj)}"


def ensure_session_namespace(conn, db: str, schema: str):
    from connection import exec_sql

    exec_sql(conn, f"USE DATABASE {qident(db)}")
    exec_sql(conn, f"USE SCHEMA {fq(db, schema)}")


def bootstrap_db_schema(conn, mig_db: str, mig_schema: str):
    from connection import exec_sql

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
    loc = (loc or "").strip()
    if not loc:
        return loc
    if loc.startswith("@") or "://" in loc:
        return sql_string_literal(loc)
    return loc


def strip_streamlit_from_schema_ddl(schema_ddl: str) -> str:
    return re.sub(
        r"(?is)\bcreate\s+(or\s+replace\s+)?streamlit\b.*?;\s*",
        "",
        schema_ddl,
    )


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
    from connection import exec_sql

    rows = exec_sql(conn, f"DESC STORAGE INTEGRATION {qident(integration_name)}")
    return desc_storage_integration_to_dict(rows)


def describe_stage(conn, db: str, schema: str, stage: str) -> dict:
    from connection import exec_sql

    try:
        rows = exec_sql(conn, f"DESC STAGE {fq(db, schema, stage)}")
        return desc_stage_to_dict(rows)
    except Exception:
        return {}


def normalize_prefix(prefix: str) -> str:
    p = (prefix or "").strip().lstrip("/")
    if p and not p.endswith("/"):
        p += "/"
    return p


def build_azure_stage_url(storage_account: str, container: str, prefix: str) -> str:
    p = normalize_prefix(prefix)
    return f"azure://{storage_account}.blob.core.windows.net/{container}/{p}"


def ensure_storage_integration_azure(
    conn, integration_name: str, tenant_id: str, allowed_locations: list[str]
):
    from connection import exec_sql

    if not tenant_id.strip():
        raise ValueError("Azure tenant id is required.")
    allowed_sql = ", ".join(f"'{loc}'" for loc in allowed_locations if loc.strip())
    if not allowed_sql:
        raise ValueError("At least one STORAGE_ALLOWED_LOCATIONS entry is required.")

    exec_sql(
        conn,
        f"""
CREATE STORAGE INTEGRATION IF NOT EXISTS {qident(integration_name)}
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '{tenant_id}'
  STORAGE_ALLOWED_LOCATIONS = ({allowed_sql});
""".strip(),
    )

    exec_sql(
        conn,
        f"""
ALTER STORAGE INTEGRATION {qident(integration_name)} SET
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ({allowed_sql});
""".strip(),
    )


def ensure_external_stage_azure(
    conn,
    mig_db: str,
    mig_schema: str,
    stage_name: str,
    stage_url: str,
    integration_name: str,
):
    from connection import exec_sql

    bootstrap_db_schema(conn, mig_db, mig_schema)

    if (
        not stage_url.strip()
        or stage_url.strip().lower() == "azure://.blob.core.windows.net//"
    ):
        raise ValueError(
            "Stage URL is empty/invalid. Fill storage account + container + prefix first."
        )

    props = describe_stage(conn, mig_db, mig_schema, stage_name)
    stage_exists = bool(props)

    existing_url = (props.get("URL") or "").strip()
    existing_integration = (props.get("STORAGE_INTEGRATION") or "").strip()

    is_internal_like = (
        stage_exists and existing_url == "" and existing_integration == ""
    )

    if (
        (not stage_exists)
        or is_internal_like
        or (existing_url and existing_url != stage_url)
        or (
            existing_integration
            and existing_integration.upper() != integration_name.upper()
        )
    ):
        exec_sql(conn, f"DROP STAGE IF EXISTS {fq(mig_db, mig_schema, stage_name)}")
        exec_sql(
            conn,
            f"""
CREATE STAGE {fq(mig_db, mig_schema, stage_name)}
  URL = '{stage_url}'
  STORAGE_INTEGRATION = {qident(integration_name)};
""".strip(),
        )
        return

    exec_sql(
        conn,
        f"""
ALTER STAGE {fq(mig_db, mig_schema, stage_name)} SET
  URL = '{stage_url}'
  STORAGE_INTEGRATION = {qident(integration_name)};
""".strip(),
    )


def _to_file_uri(local_path: str) -> str:
    return "file://" + local_path.replace("\\", "/")
