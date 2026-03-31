import re
from connection import exec_sql
from discovery import (
    list_tags,
    get_tag_ddl,
    list_masking_policies,
    get_masking_policy_ddl,
    list_row_access_policies,
    get_row_access_policy_ddl,
)


def migrate_tags(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate tags from source to target."""
    tags = list_tags(src_conn, src_db, schema)
    if not tags:
        return {"migrated": 0, "errors": []}

    errors = []
    migrated = 0

    for tag in tags:
        ddl = get_tag_ddl(src_conn, src_db, schema, tag)
        if not ddl:
            errors.append(f"{tag}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{tag}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}


def migrate_policies(
    src_conn,
    tgt_conn,
    src_db: str,
    schema: str,
    tgt_db: str,
    tgt_schema: str,
    dry_run: bool = False,
) -> dict:
    """Migrate masking and row access policies from source to target."""
    mp = list_masking_policies(src_conn, src_db, schema)
    rap = list_row_access_policies(src_conn, src_db, schema)

    errors = []
    migrated = 0

    for policy in mp:
        ddl = get_masking_policy_ddl(src_conn, src_db, schema, policy)
        if not ddl:
            errors.append(f"{policy}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{policy}: {str(e)}")
        else:
            migrated += 1

    for policy in rap:
        ddl = get_row_access_policy_ddl(src_conn, src_db, schema, policy)
        if not ddl:
            errors.append(f"{policy}: Could not get DDL")
            continue
        if tgt_db != src_db:
            ddl = re.sub(rf"(?i)\b{re.escape(src_db)}\b", tgt_db, ddl)
        if not dry_run:
            try:
                exec_sql(tgt_conn, ddl)
                migrated += 1
            except Exception as e:
                errors.append(f"{policy}: {str(e)}")
        else:
            migrated += 1

    return {"migrated": migrated, "errors": errors}
