"""
Phase-based migration orchestrator.

Migration Phases:
1. CREATE SCHEMAS - Create all target schemas
2. SEQUENCES - Migrate all sequences
3. FILE_FORMATS - Migrate all file formats
4. TAGS - Migrate all tags
5. TABLE_DDLS - Migrate all table DDLs (topological order by view refs)
6. TABLE_DATA - Migrate all table data (FK order)
7. VIEWS - Migrate all views (global topological order)
8. PROCEDURES - Migrate procedures
9. FUNCTIONS - Migrate functions
10. STREAMS - Migrate streams
11. TASKS - Migrate tasks
12. POLICIES - Migrate policies
13. DYNAMIC_TABLES - Migrate dynamic tables
14. PIPES - Migrate pipes
15. ALERTS - Migrate alerts
16. MATERIALIZED_VIEWS - Migrate materialized views
17. SEMANTIC_VIEWS - Migrate semantic views
18. STREAMLITS - Migrate Streamlit apps
19. AGENTS - Migrate AI agents
"""

from typing import Optional
from connection import exec_sql, exec_script
from discovery import get_all_schemas, inventory_all_objects
from dependencies import (
    build_table_dependency_order_from_views,
    build_global_view_dependency_order,
)
from utils import (
    fq,
    bootstrap_db_schema,
    save_checkpoint,
    load_checkpoint,
    rewrite_db_in_ddl,
)
from .tables import migrate_table_ddls, migrate_table_data_ordered
from .views import migrate_semantic_views, migrate_materialized_views
from .procedures import migrate_functions, migrate_procedures
from .policies import migrate_tags, migrate_policies
from .tasks import (
    migrate_streams,
    migrate_tasks,
    migrate_alerts,
    migrate_dynamic_tables,
    migrate_pipes,
)
from .apps import (
    migrate_streamlits,
    migrate_agents,
    migrate_sequences,
    migrate_file_formats,
)
from .cortex import migrate_cortex_search_services


def migrate_all_objects(
    src_conn,
    tgt_conn,
    src_db: str,
    tgt_db: str,
    schemas: list[str],
    stage_ref: str,
    run_id: str,
    dry_run: bool = False,
    init_streamlit_live: bool = True,
    tgt_query_wh: Optional[str] = None,
    nb_int_stage_name: str = "NB_MIG_INT_STAGE",
    stage_prefix: str = "sf_migration",
    phase_callback=None,
    selected_phases: list = None,
) -> dict:
    """
    Phase-based migration orchestrator.

    phase_callback: optional function(phase_name, count, error) called after each phase.
    selected_phases: list of phases to run. If None, runs all phases.
    """
    results = {
        "phases": [],
        "logs": [],
        "errors": [],
        "skipped": [],
        "warnings": [],
        "total_migrated": 0,
        "stopped_at": None,
        "data_validation": [],
    }

    checkpoint_path = f"checkpoint_{run_id}.json"
    checkpoint = load_checkpoint(checkpoint_path)
    completed_phases = set(checkpoint.get("completed_phases", []))

    # Default to all phases if none selected
    # NOTE: DYNAMIC_TABLES must come before VIEWS (views may reference dynamic tables)
    if not selected_phases:
        selected_phases = [
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
            "MATERIALIZED_VIEWS",
            "SEMANTIC_VIEWS",
            "STREAMLITS",
            "AGENTS",
        ]

    def should_run(phase: str) -> bool:
        if not selected_phases:
            return True  # Run all if nothing selected
        if phase in completed_phases:
            return False  # Skip already completed phases
        return phase in selected_phases

    def log(msg: str):
        results["logs"].append(msg)

    if completed_phases:
        log(f"Loaded checkpoint: {len(completed_phases)} phases already completed")

    def log_phase(phase: str, count: int, error: str = None):
        results["phases"].append(
            {
                "phase": phase,
                "count": count,
                "error": error,
            }
        )
        if error:
            results["errors"].append(f"{phase}: {error}")
        if phase_callback:
            phase_callback(phase, count, error)

        # Save checkpoint after each successfully completed phase only
        if not error:
            completed_phases.add(phase)
        save_checkpoint(
            checkpoint_path,
            {
                "completed_phases": list(completed_phases),
                "last_phase": phase,
                "total_migrated": results["total_migrated"],
            },
        )

    rewrite_db = tgt_db != src_db

    # Phase 0: Compute schema ordering based on dependencies
    ordered_schemas = build_table_dependency_order_from_views(src_conn, src_db, schemas)
    results["schema_order"] = ordered_schemas
    log(f"Schema migration order: {' → '.join(ordered_schemas)}")

    # Create all schemas first (always needed)
    log("Phase 0: Creating schemas...")
    for schema in ordered_schemas:
        if not dry_run:
            exec_sql(tgt_conn, f"CREATE DATABASE IF NOT EXISTS {tgt_db}")
            exec_sql(tgt_conn, f"CREATE SCHEMA IF NOT EXISTS {fq(tgt_db, schema)}")
    log_phase("CREATE_SCHEMAS", len(ordered_schemas))

    # Phase 1-4: Fast metadata (can run in any order)
    if should_run("SEQUENCES"):
        for schema in ordered_schemas:
            result = migrate_sequences(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("SEQUENCES", 0, result["errors"][0])
                raise Exception(f"Sequences failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
        log_phase("SEQUENCES", sum(1 for _ in ordered_schemas))

    if should_run("FILE_FORMATS"):
        for schema in ordered_schemas:
            result = migrate_file_formats(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("FILE_FORMATS", 0, result["errors"][0])
                raise Exception(f"File Formats failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
        log_phase("FILE_FORMATS", sum(1 for _ in ordered_schemas))

    if should_run("TAGS"):
        for schema in ordered_schemas:
            result = migrate_tags(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("TAGS", 0, result["errors"][0])
                raise Exception(f"Tags failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
        log_phase("TAGS", sum(1 for _ in ordered_schemas))

    # Phase 5: TABLE DDLS
    if should_run("TABLE_DDLS"):
        log("Phase 5: Migrating table DDLs...")
        table_ddl_count = 0
        for schema in ordered_schemas:
            result = migrate_table_ddls(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("TABLE_DDLS", table_ddl_count, result["errors"][0])
                raise Exception(f"Table DDLs failed: {result['errors'][0]}")
            table_ddl_count += result["migrated"]
            results["total_migrated"] += result["migrated"]
        log_phase("TABLE_DDLS", table_ddl_count)

    # Phase 6: TABLE DATA
    if should_run("TABLE_DATA"):
        log("Phase 6: Migrating table data...")
        data_count = 0
        for schema in ordered_schemas:
            result = migrate_table_data_ordered(
                src_conn,
                tgt_conn,
                src_db,
                schema,
                tgt_db,
                schema,
                stage_ref,
                run_id,
                dry_run,
            )
            if result["errors"]:
                log_phase("TABLE_DATA", data_count, result["errors"][0])
                raise Exception(f"Data migration failed: {result['errors'][0]}")
            data_count += result["migrated"]
            results["total_migrated"] += result["migrated"]

            # Collect validation results
            validation = result.get("validation", [])
            if validation:
                results.setdefault("data_validation", []).extend(validation)
        log_phase("TABLE_DATA", data_count)

    # Phase 7: DYNAMIC TABLES (before VIEWS)
    if should_run("DYNAMIC_TABLES"):
        for schema in ordered_schemas:
            result = migrate_dynamic_tables(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("DYNAMIC_TABLES", 0, result["errors"][0])
                raise Exception(f"Dynamic Tables failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
        log_phase("DYNAMIC_TABLES", sum(1 for _ in ordered_schemas))

    # Phase 8: MATERIALIZED VIEWS (before regular views)
    if should_run("MATERIALIZED_VIEWS"):
        mv_count = 0
        for schema in ordered_schemas:
            result = migrate_materialized_views(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run, rewrite_db
            )
            mv_count += result["migrated"]
            results["total_migrated"] += result["migrated"]
            if result["errors"]:
                results["warnings"].extend(result["errors"])
        log_phase("MATERIALIZED_VIEWS", mv_count)

    # Phase 9: VIEWS with topo-first then retry
    if should_run("VIEWS"):
        log("Phase 9: Migrating views (topo-first + retry)...")
        try:
            g = build_global_view_dependency_order(src_conn, src_db, ordered_schemas)
            topo_order = g.get("order", [])
            cycles = g.get("cycles", [])
            all_views = g.get("all_views", [])
            ddls = g.get("ddls", {})

            log(f"Found {len(all_views)} total views")

            if not all_views:
                log("WARNING: No views found!")
                log_phase("VIEWS", 0)
            else:
                view_count = 0
                view_errors = []
                max_retries = 4

                # Pass 1: create in topological order first
                views_to_create = list(topo_order)
                failed_initial = []
                for fqn in views_to_create:
                    ddl = ddls.get(fqn, "")
                    if rewrite_db and tgt_db != src_db:
                        ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)
                    if not dry_run:
                        try:
                            exec_script(tgt_conn, ddl, remove_comments=True)
                            view_count += 1
                        except Exception as e:
                            failed_initial.append((fqn, str(e)))
                    else:
                        view_count += 1

                # Build retry set from cycles + failed topo views
                retry_set = set(cycles)
                retry_set.update([fqn for fqn, _ in failed_initial])
                views_to_create = list(retry_set)
                log(
                    f"Topo pass done: {view_count} succeeded, {len(views_to_create)} queued for retry"
                )

                for attempt in range(1, max_retries + 1):
                    if not views_to_create:
                        break

                    failed_this_round = []
                    for fqn in views_to_create:
                        ddl = ddls.get(fqn, "")
                        if rewrite_db and tgt_db != src_db:
                            ddl = rewrite_db_in_ddl(ddl, src_db, tgt_db)
                        if not dry_run:
                            try:
                                exec_script(tgt_conn, ddl, remove_comments=True)
                                view_count += 1
                            except Exception as e:
                                failed_this_round.append((fqn, str(e)))
                        else:
                            view_count += 1

                    if failed_this_round and attempt < max_retries:
                        log(
                            f"Attempt {attempt}: {len(failed_this_round)} failed, retrying..."
                        )
                        views_to_create = [fqn for fqn, _ in failed_this_round]
                        view_errors.extend(
                            [f"{fqn}: {err}" for fqn, err in failed_this_round]
                        )
                    elif failed_this_round:
                        log(
                            f"Attempt {attempt}: {len(failed_this_round)} permanently failed"
                        )
                        view_errors.extend(
                            [f"{fqn}: {err}" for fqn, err in failed_this_round]
                        )
                        views_to_create = []
                    else:
                        views_to_create = []

                log(
                    f"View migration complete: {view_count} succeeded, {len(view_errors)} failed"
                )

                results["total_migrated"] += view_count
                if view_errors:
                    results["warnings"].extend(view_errors)
                log_phase("VIEWS", view_count)
        except Exception as e:
            log_phase("VIEWS", 0, str(e))
            results["warnings"].append(f"View migration had issues: {e}")

    # Phase 10: CORTEX SEARCH SERVICES
    if should_run("CORTEX_SEARCH"):
        for schema in ordered_schemas:
            result = migrate_cortex_search_services(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run, rewrite_db
            )
            results["total_migrated"] += result["migrated"]
            if result["errors"]:
                results["warnings"].extend(result["errors"])
            if result.get("skipped"):
                results["skipped"].extend(result["skipped"])
        log_phase("CORTEX_SEARCH", sum(1 for _ in ordered_schemas))

    # Phase 11-12: PROCEDURES & FUNCTIONS
    if should_run("FUNCTIONS"):
        for schema in ordered_schemas:
            result = migrate_functions(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("FUNCTIONS", 0, result["errors"][0])
                raise Exception(f"Functions failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            if result.get("skipped"):
                results["skipped"].extend(result["skipped"])
        log_phase("FUNCTIONS", sum(1 for _ in ordered_schemas))

    if should_run("PROCEDURES"):
        for schema in ordered_schemas:
            result = migrate_procedures(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("PROCEDURES", 0, result["errors"][0])
                raise Exception(f"Procedures failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            if result.get("skipped"):
                results["skipped"].extend(result["skipped"])
        log_phase("PROCEDURES", sum(1 for _ in ordered_schemas))

    # Phase 13-17: TASKS, STREAMS, POLICIES, etc.
    if should_run("STREAMS"):
        streams_count = 0
        for schema in ordered_schemas:
            result = migrate_streams(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("STREAMS", streams_count, result["errors"][0])
                raise Exception(f"Streams failed: {result['errors'][0]}")
            streams_count += result["migrated"]
            results["total_migrated"] += result["migrated"]
        log_phase("STREAMS", streams_count)

    if should_run("POLICIES"):
        for schema in ordered_schemas:
            result = migrate_policies(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("POLICIES", 0, result["errors"][0])
                raise Exception(f"Policies failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
        log_phase("POLICIES", sum(1 for _ in ordered_schemas))

    # TASKS
    if should_run("TASKS"):
        for schema in ordered_schemas:
            result = migrate_tasks(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("TASKS", 0, result["errors"][0])
                raise Exception(f"Tasks failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
        log_phase("TASKS", sum(1 for _ in ordered_schemas))

    if should_run("PIPES"):
        for schema in ordered_schemas:
            result = migrate_pipes(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("PIPES", 0, result["errors"][0])
                raise Exception(f"Pipes failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
        log_phase("PIPES", sum(1 for _ in ordered_schemas))

    if should_run("ALERTS"):
        for schema in ordered_schemas:
            result = migrate_alerts(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("ALERTS", 0, result["errors"][0])
                raise Exception(f"Alerts failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
        log_phase("ALERTS", sum(1 for _ in ordered_schemas))

    # Phase 18: SEMANTIC VIEWS
    if should_run("SEMANTIC_VIEWS"):
        for schema in ordered_schemas:
            result = migrate_semantic_views(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run, rewrite_db
            )
            results["total_migrated"] += result["migrated"]
            if result["errors"]:
                results["warnings"].extend(result["errors"])
        log_phase("SEMANTIC_VIEWS", sum(1 for _ in ordered_schemas))

    # Phase 18-19: APPS (Streamlit, Agents)
    if should_run("STREAMLITS"):
        for schema in ordered_schemas:
            result = migrate_streamlits(
                src_conn,
                tgt_conn,
                src_db,
                schema,
                tgt_db,
                schema,
                dry_run,
                rewrite_db,
                init_streamlit_live,
            )
            if result["errors"]:
                log_phase("STREAMLITS", 0, result["errors"][0])
                raise Exception(f"Streamlits failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
        log_phase("STREAMLITS", sum(1 for _ in ordered_schemas))

    if should_run("AGENTS"):
        for schema in ordered_schemas:
            result = migrate_agents(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run, rewrite_db
            )
            results["total_migrated"] += result["migrated"]
        log_phase("AGENTS", sum(1 for _ in ordered_schemas))

    log("Migration completed successfully!")
    return results
