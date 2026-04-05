import uuid
import os
import tempfile

import streamlit as st

from config import (
    DEFAULT_MIG_DB,
    DEFAULT_MIG_SCHEMA,
    DEFAULT_STAGE,
    DEFAULT_INTEGRATION,
    DEFAULT_NB_INT_STAGE,
    DEFAULT_LOCAL_INT_STAGE,
)
from connection import connect_sf, exec_sql
from utils import (
    qident,
    fq,
    bootstrap_db_schema,
    build_azure_stage_url,
    describe_storage_integration,
    describe_stage,
    ensure_storage_integration_azure,
    ensure_external_stage_azure,
)
from discovery import (
    get_all_schemas,
)
from dependencies import (
    validate_cross_db_dependencies,
    build_table_dependency_order_from_views,
    clear_dependency_cache,
)
from migration.orchestrator import migrate_all_objects


def _update_phase_ui(phase_containers, log_container, phase, count, error):
    """Update phase UI in real-time during migration."""
    import streamlit as st

    if phase in phase_containers:
        if error:
            phase_containers[phase].error(f"❌ **{phase}**: {error}")
        else:
            phase_containers[phase].success(f"✅ **{phase}** ({count} items)")
    log_container.info(f"Completed: {phase}")


st.set_page_config(page_title="Snowflake Migrator", layout="wide")
st.title("Snowflake → Snowflake Migrator")

with st.sidebar:
    st.header("Source connection")
    src_account = st.text_input("Source account", placeholder="xy12345.ap-south-1.aws")
    src_user = st.text_input("Source user")
    src_password = st.text_input("Source password", type="password")
    src_role = st.text_input("Source role", value="ACCOUNTADMIN")
    src_wh = st.text_input("Source warehouse", value="")
    src_passcode = st.text_input(
        "Source MFA TOTP passcode",
        type="password",
        help="Leave blank if MFA is not enabled",
    )

    st.divider()
    st.header("Target connection")
    tgt_account = st.text_input("Target account", placeholder="ab67890.ap-south-1.aws")
    tgt_user = st.text_input("Target user")
    tgt_password = st.text_input("Target password", type="password")
    tgt_role = st.text_input("Target role", value="ACCOUNTADMIN")
    tgt_wh = st.text_input("Target warehouse", value="")
    tgt_passcode = st.text_input(
        "Target MFA TOTP passcode",
        type="password",
        help="Leave blank if MFA is not enabled",
    )

    st.divider()
    st.header("Utility namespace")
    mig_db = st.text_input("Utility DB", value=DEFAULT_MIG_DB)
    mig_schema = st.text_input("Utility Schema", value=DEFAULT_MIG_SCHEMA)
    stage_name = st.text_input("Stage name", value=DEFAULT_STAGE)

    st.divider()
    st.header("Azure external stage")
    integration_name = st.text_input(
        "Storage integration name", value=DEFAULT_INTEGRATION
    )
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
            src_account,
            src_user,
            src_password,
            role=src_role,
            warehouse=src_wh or None,
            passcode=src_passcode or None,
        )
        st.session_state.tgt_conn = connect_sf(
            tgt_account,
            tgt_user,
            tgt_password,
            role=tgt_role,
            warehouse=tgt_wh or None,
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
            src_account,
            src_user,
            src_password,
            role=src_role,
            warehouse=src_wh or None,
            passcode=src_passcode or None,
        )
        st.success("Source connected.")
    except Exception as e:
        st.session_state.src_conn = None
        st.error(f"Source connection failed: {e}")

if connect_tgt_btn:
    try:
        st.session_state.tgt_conn = connect_sf(
            tgt_account,
            tgt_user,
            tgt_password,
            role=tgt_role,
            warehouse=tgt_wh or None,
            passcode=tgt_passcode or None,
        )
        st.success("Target connected.")
    except Exception as e:
        st.session_state.tgt_conn = None
        st.error(f"Target connection failed: {e}")

src_conn = st.session_state.src_conn
tgt_conn = st.session_state.tgt_conn

_src_status = "✅ Source connected" if src_conn else "❌ Source not connected"
_tgt_status = "✅ Target connected" if tgt_conn else "❌ Target not connected"
st.caption(f"{_src_status}  |  {_tgt_status}")

stage_url = build_azure_stage_url(storage_account, container, prefix)

st.divider()

st.header("Local Backup & Restore")

local_bk_tab, local_rs_tab = st.tabs(["Backup to Local", "Restore from Local"])

with local_bk_tab:
    st.markdown("Export schemas from **source** Snowflake to a local directory.")
    if not src_conn:
        st.warning("Connect to the **source** account first (sidebar).")
    else:
        bk_dir = st.text_input(
            "Local backup directory",
            value=os.path.join(tempfile.gettempdir(), "sf_backup"),
        )
        bk_int_stage = st.text_input(
            "Internal stage (created on source)",
            value=DEFAULT_LOCAL_INT_STAGE,
            key="bk_stage",
        )
        bk_auto_id = st.checkbox(
            "Auto-generate new run id each backup", value=True, key="bk_auto_id"
        )
        bk_run_id = st.text_input(
            "Backup run id", value=str(uuid.uuid4())[:8], key="bk_run_id"
        )

        bk_db_rows = exec_sql(src_conn, "SHOW DATABASES")
        bk_db_names = [r[1] for r in bk_db_rows if len(r) > 1]
        bk_src_db = st.selectbox("Source database", bk_db_names, key="bk_src_db")

        bk_schema_rows = exec_sql(
            src_conn, f"SHOW SCHEMAS IN DATABASE {qident(bk_src_db)}"
        )
        bk_schema_names = [r[1] for r in bk_schema_rows if len(r) > 1]
        bk_schema_names = [
            s for s in bk_schema_names if s and s.upper() != "INFORMATION_SCHEMA"
        ]
        bk_all_schemas = st.multiselect(
            "Schemas to back up",
            bk_schema_names,
            default=[s for s in bk_schema_names if s.upper() == "PUBLIC"] or [],
            key="bk_schemas",
        )

        bk_c1, bk_c2, bk_c3, bk_c4, bk_c5, bk_c6, bk_c7, bk_c8 = st.columns(8)
        with bk_c1:
            bk_ddl = st.checkbox("Schema DDL", value=True, key="bk_ddl")
        with bk_c2:
            bk_views = st.checkbox("Views", value=True, key="bk_views")
        with bk_c3:
            bk_sv = st.checkbox("Semantic views", value=True, key="bk_sv")
        with bk_c4:
            bk_ag = st.checkbox("Agents", value=True, key="bk_ag")
        with bk_c5:
            bk_sl = st.checkbox("Streamlits", value=True, key="bk_sl")
        with bk_c6:
            bk_nb = st.checkbox("Notebooks", value=True, key="bk_nb")
        with bk_c7:
            bk_data = st.checkbox("Table data", value=True, key="bk_data")
        with bk_c8:
            bk_stg = st.checkbox("Stage files", value=True, key="bk_stg")
        bk_dry = st.checkbox("Dry run", value=False, key="bk_dry")

        if st.button("Run Backup Now"):
            if not bk_all_schemas:
                st.error("Select at least one schema.")
            else:
                st.info(f"Backup run id: **{bk_run_id}**")
                st.warning("Local backup UI not yet migrated to new modules.")

with local_rs_tab:
    st.markdown("Import schemas from a local backup into **target** Snowflake.")
    if not tgt_conn:
        st.warning("Connect to the **target** account first (sidebar).")
    else:
        rs_path = st.text_input(
            "Backup path",
            value="",
            help="e.g. C:/Users/.../sf_backup/<run_id>/<DATABASE>",
        )
        rs_tgt_db = st.text_input("Target database name", value="", key="rs_tgt_db")
        rs_int_stage = st.text_input(
            "Internal stage (created on target)",
            value=DEFAULT_LOCAL_INT_STAGE,
            key="rs_stage",
        )

        rs_dry = st.checkbox("Dry run", value=False, key="rs_dry")

        if st.button("Run Restore Now"):
            if not rs_path.strip():
                st.error("Please enter the backup path.")
            elif not rs_tgt_db.strip():
                st.error("Please enter the target database name.")
            else:
                st.warning("Local restore UI not yet migrated to new modules.")

st.divider()

if not (src_conn and tgt_conn):
    st.info("Connect **both** source and target accounts to use Azure migration.")
    st.stop()

st.subheader("Computed Azure stage URL")
st.code(stage_url)

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
            ensure_storage_integration_azure(
                src_conn, integration_name, azure_tenant_id, [stage_url]
            )
            ensure_storage_integration_azure(
                tgt_conn, integration_name, azure_tenant_id, [stage_url]
            )
            st.success("Integration ensured in both.")
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
            ensure_storage_integration_azure(
                src_conn, integration_name, azure_tenant_id, [stage_url]
            )
            ensure_storage_integration_azure(
                tgt_conn, integration_name, azure_tenant_id, [stage_url]
            )
            ensure_external_stage_azure(
                src_conn, mig_db, mig_schema, stage_name, stage_url, integration_name
            )
            ensure_external_stage_azure(
                tgt_conn, mig_db, mig_schema, stage_name, stage_url, integration_name
            )
            st.success("Stage ensured in both.")
        except Exception as e:
            st.error(e)

with c5:
    if st.button("Inspect Stage (both)"):
        try:
            s = describe_stage(src_conn, mig_db, mig_schema, stage_name)
            t = describe_stage(tgt_conn, mig_db, mig_schema, stage_name)
            st.write(
                {
                    "URL": s.get("URL"),
                    "STORAGE_INTEGRATION": s.get("STORAGE_INTEGRATION"),
                }
            )
            st.write(
                {
                    "URL": t.get("URL"),
                    "STORAGE_INTEGRATION": t.get("STORAGE_INTEGRATION"),
                }
            )
        except Exception as e:
            st.error(e)

if st.button("Test LIST @stage (both)"):
    try:
        exec_sql(src_conn, f"USE {mig_db}.{mig_schema}")
        exec_sql(tgt_conn, f"USE {mig_db}.{mig_schema}")
        src_list = exec_sql(src_conn, f"LIST @{stage_name}")
        tgt_list = exec_sql(tgt_conn, f"LIST @{stage_name}")
        st.subheader("Source LIST (first 50)")
        st.write(src_list[:50])
        st.subheader("Target LIST (first 50)")
        st.write(tgt_list[:50])
        st.success("LIST executed.")
    except Exception as e:
        st.error(f"LIST failed: {e}")

st.divider()

st.header("Comprehensive Migration (All Objects)")

if not (src_conn and tgt_conn):
    st.warning("Connect both source and target accounts to use migration.")
    st.stop()

db_rows = exec_sql(src_conn, "SHOW DATABASES")
db_names = [r[1] for r in db_rows if len(r) > 1]
# Source DB selector
src_db = st.selectbox("Source database", db_names, key="mig_src_db")

# Keep target DB in sync with source selection: when the user selects a
# source database, update the target database text field to the same name.
# This makes it convenient to migrate into a same-named target DB by
# default while still allowing manual edits afterwards.
if "mig_tgt_db" not in st.session_state or st.session_state.get("mig_tgt_db") != src_db:
    st.session_state["mig_tgt_db"] = src_db

tgt_db = st.text_input(
    "Target database name",
    value=st.session_state.get("mig_tgt_db", src_db),
    key="mig_tgt_db",
)

run_id = st.text_input("Run id", value=str(uuid.uuid4())[:8], key="mig_run_id")

dry_run = st.checkbox(
    "Dry run (validate without executing)", value=False, key="mig_dry_run"
)

st.divider()

st.subheader("Pre-Migration Analysis")

all_schemas = get_all_schemas(src_conn, src_db)

if not all_schemas:
    st.warning(f"No schemas found in database {src_db}")
    st.stop()

st.info(f"Found {len(all_schemas)} schemas in {src_db}")

if "analysis_cache" not in st.session_state:
    st.session_state.analysis_cache = {}

if "analysis_db" not in st.session_state:
    st.session_state.analysis_db = None

refresh_btn = st.button("🔄 Refresh Analysis", type="secondary")

cache_key = f"{src_db}"

# Quick validation - only check cross-db dependencies
if "validation_done" not in st.session_state:
    st.session_state.validation_done = False
    st.session_state.validation_result = None

if refresh_btn:
    with st.spinner("Validating..."):
        try:
            validation = validate_cross_db_dependencies(src_conn, src_db, all_schemas)
            st.session_state.validation_result = validation
            st.session_state.validation_done = True
        except Exception as e:
            st.session_state.validation_result = None
            st.session_state.validation_done = False

# Show validation status
validation = st.session_state.get("validation_result")
if validation:
    if not validation["valid"]:
        st.error("❌ Migration BLOCKED: Cross-database dependencies detected!")
        for err in validation["errors"]:
            st.write(f"- {err}")
        st.stop()
    if validation.get("warnings"):
        st.warning("⚠️ Warnings:")
        for warn in validation["warnings"]:
            st.write(f"- {warn}")
else:
    st.info("Click 'Refresh Analysis' to validate schemas")

# Compute and show schema order (fast - just reads metadata)
ordered_schemas = build_table_dependency_order_from_views(src_conn, src_db, all_schemas)
with st.expander("Schema Migration Order", expanded=True):
    st.markdown("**Phases will run in this order:**")
    for i, sch in enumerate(ordered_schemas, 1):
        st.write(f"{i}. `{sch}`")

    with st.expander("Migration Execution Order", expanded=False):
        st.markdown("""
        **Objects will be migrated in this order:**
        1. SEQUENCES
        2. FILE FORMATS
        3. TAGS
        4. TABLES (DDL, skip Iceberg)
        5. TABLE DATA (FK-ordered)
        6. DYNAMIC TABLES
        7. MATERIALIZED VIEWS
        8. VIEWS (topo-first + dependency retry)
        9. CORTEX SEARCH SERVICES
        10. FUNCTIONS (skip external handlers)
        11. PROCEDURES
        11. STREAMS
        12. POLICIES (masking, row access)
        13. TASKS
        14. PIPES
        15. ALERTS
        16. SEMANTIC VIEWS
        17. STREAMLITS
        18. AGENTS
        
        **Note:** Migration stops on critical errors, but collects warnings.
        """)

st.divider()

# Phase selection
st.subheader("⚙️ Phase Selection")
all_phases = [
    "CREATE_SCHEMAS",
    "SEQUENCES",
    "FILE_FORMATS",
    "TAGS",
    "TABLE_DDLS",
    "TABLE_DATA",
    "DYNAMIC_TABLES",  # Before VIEWS - views may reference DTs
    "MATERIALIZED_VIEWS",  # Before VIEWS when views depend on MVs
    "VIEWS",
    "CORTEX_SEARCH",  # After VIEWS - depends on tables/views
    "FUNCTIONS",
    "PROCEDURES",
    "STREAMS",
    "POLICIES",
    "TASKS",
    "PIPES",
    "ALERTS",
    "SEMANTIC_VIEWS",
    "STREAMLITS",
    "AGENTS",
]
selected_phases = st.multiselect(
    "Select phases to run",
    all_phases,
    default=["VIEWS", "MATERIALIZED_VIEWS", "SEMANTIC_VIEWS"],
    key="selected_phases",
)

st.divider()

confirm_migrate = st.checkbox(
    "I confirm this will migrate ALL objects from source to target. This may overwrite existing objects.",
    key="confirm_migrate_all",
)

migrate_btn = st.button(
    "MIGRATE EVERYTHING", type="primary", disabled=not confirm_migrate
)

if migrate_btn and confirm_migrate:
    st.divider()
    st.subheader("Migration Execution")

    try:
        ensure_storage_integration_azure(
            src_conn, integration_name, azure_tenant_id, [stage_url]
        )
        ensure_storage_integration_azure(
            tgt_conn, integration_name, azure_tenant_id, [stage_url]
        )
        ensure_external_stage_azure(
            src_conn, mig_db, mig_schema, stage_name, stage_url, integration_name
        )
        ensure_external_stage_azure(
            tgt_conn, mig_db, mig_schema, stage_name, stage_url, integration_name
        )
    except Exception as e:
        st.error(f"Pre-check failed: {e}")
        st.stop()

    if not dry_run:
        exec_sql(tgt_conn, f"CREATE DATABASE IF NOT EXISTS {qident(tgt_db)}")
        bootstrap_db_schema(tgt_conn, mig_db, mig_schema)

    stage_ref = f"@{mig_db}.{mig_schema}.{stage_name}"

    # Initialize session state for logs
    if "migration_logs" not in st.session_state:
        st.session_state.migration_logs = []
    if "migration_result" not in st.session_state:
        st.session_state.migration_result = None

    st.divider()
    st.subheader("🚀 Migration Progress")

    # Use selected schemas from the UI
    migration_schemas = st.session_state.get("all_schemas", all_schemas)
    total_schemas = len(migration_schemas)

    # Create columns for status
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Schemas", total_schemas)
    with col2:
        st.metric("Source DB", src_db)
    with col3:
        st.metric("Target DB", tgt_db)

    st.divider()
    st.subheader("🚀 Live Migration Progress")

    # Phase containers for live updates
    phase_containers = {}
    for phase in [
        "CREATE_SCHEMAS",
        "SEQUENCES",
        "FILE_FORMATS",
        "TAGS",
        "TABLE_DDLS",
        "TABLE_DATA",
        "DYNAMIC_TABLES",
        "MATERIALIZED_VIEWS",
        "VIEWS",
        "CORTEX_SEARCH",
        "FUNCTIONS",
        "PROCEDURES",
        "STREAMS",
        "POLICIES",
        "TASKS",
        "PIPES",
        "ALERTS",
        "SEMANTIC_VIEWS",
        "STREAMLITS",
        "AGENTS",
    ]:
        phase_containers[phase] = st.empty()

    log_container = st.empty()
    log_container.info("Starting migration...")

    try:
        clear_dependency_cache()

        result = migrate_all_objects(
            src_conn,
            tgt_conn,
            src_db,
            tgt_db,
            migration_schemas,
            stage_ref,
            run_id,
            dry_run=dry_run,
            init_streamlit_live=True,
            tgt_query_wh=tgt_wh or None,
            nb_int_stage_name=DEFAULT_NB_INT_STAGE,
            stage_prefix=stage_prefix,
            phase_callback=lambda phase, count, error: _update_phase_ui(
                phase_containers, log_container, phase, count, error
            ),
            selected_phases=selected_phases,
        )

        # Final log update
        logs = result.get("logs", [])
        log_text = "✅ **Migration Complete!**\n\n"
        for log_msg in logs[-10:]:  # Show last 10 logs
            log_text += f"• {log_msg}\n"
        log_container.markdown(log_text)

        # Show logs
        logs = result.get("logs", [])
        if logs:
            with st.expander("📋 Migration Logs", expanded=False):
                for log_msg in logs:
                    st.write(f"• {log_msg}")

        # Show data validation results
        validation = result.get("data_validation", [])
        if validation:
            st.divider()
            st.subheader("📊 Data Validation Results")
            mismatches = [v for v in validation if v.get("status") == "MISMATCH"]
            ok_count = len(validation) - len(mismatches)

            col1, col2 = st.columns(2)
            col1.metric("✅ Row Count Match", ok_count)
            col2.metric("❌ Row Count Mismatch", len(mismatches))

            if mismatches:
                with st.expander("⚠️ Mismatched Tables", expanded=True):
                    for v in mismatches:
                        st.error(
                            f"**{v['table']}**: Source={v['src_count']}, Target={v['tgt_count']}"
                        )

        # Show warnings
        warnings = result.get("warnings", [])
        if warnings:
            st.divider()
            with st.expander("⚠️ Warnings", expanded=True):
                for w in warnings[:20]:  # Limit display
                    st.write(f"- {w}")
                if len(warnings) > 20:
                    st.write(f"... and {len(warnings) - 20} more warnings")

        # Show skipped
        skipped = result.get("skipped", [])
        if skipped:
            st.divider()
            with st.expander("⏭️ Skipped Objects", expanded=False):
                for s in skipped[:20]:
                    st.write(f"- {s}")
                if len(skipped) > 20:
                    st.write(f"... and {len(skipped) - 20} more")

        # Show errors
        errors = result.get("errors", [])
        if errors:
            st.divider()
            st.error("❌ Migration Errors")
            for e in errors:
                st.write(f"- {e}")
            st.stop()

        # Success!
        st.balloons()
        st.success(
            f"🎉 MIGRATION COMPLETE! Total objects: {result.get('total_migrated', 0)}"
        )

    except Exception as e:
        log_container.error(f"Migration failed: {str(e)}")
        st.error(f"MIGRATION FAILED: {str(e)}")
