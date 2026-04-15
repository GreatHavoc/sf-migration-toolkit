import logging

logger = logging.getLogger(__name__)

from connection import exec_sql, fetch_one_val


def list_stages(conn, db, schema):
    rows = exec_sql(conn, f"SHOW STAGES IN SCHEMA {db}.{schema}")
    return [r[0] for r in rows if r[0]]


def get_stage_ddl(conn, db, schema, stage_name):
    ddl = fetch_one_val(
        conn, f"SELECT GET_DDL('STAGE', '{db}.{schema}.{stage_name}', TRUE)"
    )
    return ddl


def migrate_stages(
    src_conn,
    tgt_conn,
    src_db: str,
    src_schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
    rewrite_db: bool = True,
) -> dict:
    result = {
        "migrated": 0,
        "logs": [],
        "errors": [],
        "skipped": [],
    }

    stages = list_stages(src_conn, src_db, src_schema)
    if not stages:
        result["logs"].append(f"No stages found in {src_db}.{src_schema}")
        return result

    target_schema_fqn = f"{tgt_db}.{tgt_schema}"

    for stage_name in stages:
        try:
            ddl = get_stage_ddl(src_conn, src_db, src_schema, stage_name)
            if not ddl:
                result["errors"].append(f"Could not get DDL for stage {stage_name}")
                continue

            if rewrite_db:
                ddl = ddl.replace(src_db, tgt_db)
                ddl = ddl.replace(f"{src_schema}.", f"{tgt_schema}.")

            if dry_run:
                result["logs"].append(f"(DRY RUN) Would create stage: {stage_name}")
            else:
                exec_sql(tgt_conn, ddl)
                result["logs"].append(f"Created stage: {stage_name}")

            result["migrated"] += 1

        except Exception as e:
            logger.exception(f"Failed to migrate stage {stage_name}")
            result["errors"].append(f"Stage {stage_name}: {e}")

    return result
