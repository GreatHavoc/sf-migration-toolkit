"""
Phase-based migration orchestrator.

Migration Phases (execution order):
1. CREATE_SCHEMAS - Create all target schemas
2. SEQUENCES - Migrate all sequences
3. FILE_FORMATS - Migrate all file formats
4. TAGS - Migrate all tags
5. TABLE_DDLS - Migrate all table DDLs
6. TABLE_DATA - Migrate all table data (FK-aware)
7. DYNAMIC_TABLES - Migrate dynamic tables
8. MATERIALIZED_VIEWS - Migrate materialized views
9. VIEWS - Migrate regular views (topo-first + retry)
10. CORTEX_SEARCH - Migrate cortex search services
11. FUNCTIONS - Migrate functions
12. PROCEDURES - Migrate procedures
13. STREAMS - Migrate streams
14. POLICIES - Migrate policies
15. TASKS - Migrate tasks
16. PIPES - Migrate pipes
17. ALERTS - Migrate alerts
18. SEMANTIC_VIEWS - Migrate semantic views
19. STREAMLITS - Migrate Streamlit apps
20. AGENTS - Migrate AI agents
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
    phase_schema_done = {
        p: set(v) for p, v in checkpoint.get("phase_schema_done", {}).items()
    }

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

    def save_ckpt(last_phase: str):
        save_checkpoint(
            checkpoint_path,
            {
                "completed_phases": list(completed_phases),
                "phase_schema_done": {
                    p: sorted(list(v)) for p, v in phase_schema_done.items()
                },
                "last_phase": last_phase,
                "total_migrated": results["total_migrated"],
            },
        )

    def schema_done(phase: str, schema: str) -> bool:
        return schema.upper() in phase_schema_done.get(phase, set())

    def mark_schema_done(phase: str, schema: str):
        phase_schema_done.setdefault(phase, set()).add(schema.upper())
        save_ckpt(phase)

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
        save_ckpt(phase)

    rewrite_db = tgt_db != src_db

    # Phase 0: Compute schema ordering based on dependencies
    ordered_schemas = build_table_dependency_order_from_views(src_conn, src_db, schemas)
    results["schema_order"] = ordered_schemas
    log(f"Schema migration order: {' → '.join(ordered_schemas)}")

    # Phase 0: create schemas (supports schema-level resume)
    if should_run("CREATE_SCHEMAS"):
        log("Phase 0: Creating schemas...")
        created_count = 0
        for schema in ordered_schemas:
            if schema_done("CREATE_SCHEMAS", schema):
                continue
            if not dry_run:
                exec_sql(tgt_conn, f"CREATE DATABASE IF NOT EXISTS {tgt_db}")
                exec_sql(tgt_conn, f"CREATE SCHEMA IF NOT EXISTS {fq(tgt_db, schema)}")
            created_count += 1
            mark_schema_done("CREATE_SCHEMAS", schema)
        log_phase("CREATE_SCHEMAS", created_count)

    # Phase 1-4: Fast metadata (can run in any order)
    if should_run("SEQUENCES"):
        seq_count = 0
        for schema in ordered_schemas:
            if schema_done("SEQUENCES", schema):
                continue
            result = migrate_sequences(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("SEQUENCES", 0, result["errors"][0])
                raise Exception(f"Sequences failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            seq_count += result["migrated"]
            mark_schema_done("SEQUENCES", schema)
        log_phase("SEQUENCES", seq_count)

    if should_run("FILE_FORMATS"):
        ff_count = 0
        for schema in ordered_schemas:
            if schema_done("FILE_FORMATS", schema):
                continue
            result = migrate_file_formats(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("FILE_FORMATS", 0, result["errors"][0])
                raise Exception(f"File Formats failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            ff_count += result["migrated"]
            mark_schema_done("FILE_FORMATS", schema)
        log_phase("FILE_FORMATS", ff_count)

    if should_run("TAGS"):
        tag_count = 0
        for schema in ordered_schemas:
            if schema_done("TAGS", schema):
                continue
            result = migrate_tags(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("TAGS", 0, result["errors"][0])
                raise Exception(f"Tags failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            tag_count += result["migrated"]
            mark_schema_done("TAGS", schema)
        log_phase("TAGS", tag_count)

    # Phase 5: TABLE DDLS
    if should_run("TABLE_DDLS"):
        log("Phase 5: Migrating table DDLs...")
        table_ddl_count = 0
        for schema in ordered_schemas:
            if schema_done("TABLE_DDLS", schema):
                continue
            result = migrate_table_ddls(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("TABLE_DDLS", table_ddl_count, result["errors"][0])
                raise Exception(f"Table DDLs failed: {result['errors'][0]}")
            table_ddl_count += result["migrated"]
            results["total_migrated"] += result["migrated"]
            mark_schema_done("TABLE_DDLS", schema)
        log_phase("TABLE_DDLS", table_ddl_count)

    # Phase 6: TABLE DATA
    if should_run("TABLE_DATA"):
        log("Phase 6: Migrating table data...")
        data_count = 0
        for schema in ordered_schemas:
            if schema_done("TABLE_DATA", schema):
                continue
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
            mark_schema_done("TABLE_DATA", schema)

            # Collect validation results
            validation = result.get("validation", [])
            if validation:
                results.setdefault("data_validation", []).extend(validation)
        log_phase("TABLE_DATA", data_count)

    # Phase 7: DYNAMIC TABLES (before VIEWS)
    if should_run("DYNAMIC_TABLES"):
        dt_count = 0
        for schema in ordered_schemas:
            if schema_done("DYNAMIC_TABLES", schema):
                continue
            result = migrate_dynamic_tables(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("DYNAMIC_TABLES", 0, result["errors"][0])
                raise Exception(f"Dynamic Tables failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            dt_count += result["migrated"]
            mark_schema_done("DYNAMIC_TABLES", schema)
        log_phase("DYNAMIC_TABLES", dt_count)

    # Phase 8: MATERIALIZED VIEWS (before regular views)
    if should_run("MATERIALIZED_VIEWS"):
        mv_count = 0
        for schema in ordered_schemas:
            if schema_done("MATERIALIZED_VIEWS", schema):
                continue
            result = migrate_materialized_views(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run, rewrite_db
            )
            mv_count += result["migrated"]
            results["total_migrated"] += result["migrated"]
            if result["errors"]:
                results["warnings"].extend(result["errors"])
            mark_schema_done("MATERIALIZED_VIEWS", schema)
        log_phase("MATERIALIZED_VIEWS", mv_count)

    # Phase 9: VIEWS with topo-first then retry
    if should_run("VIEWS"):
        log("Phase 9: Migrating views (topo-first + retry)...")
        try:
            if schema_done("VIEWS", "__GLOBAL__"):
                log("Skipping VIEWS: already completed in checkpoint")
                log_phase("VIEWS", 0)
            else:
                g = build_global_view_dependency_order(
                    src_conn, src_db, ordered_schemas
                )
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
                    mark_schema_done("VIEWS", "__GLOBAL__")
                    log_phase("VIEWS", view_count)
        except Exception as e:
            log_phase("VIEWS", 0, str(e))
            results["warnings"].append(f"View migration had issues: {e}")

    # Phase 10: CORTEX SEARCH SERVICES
    if should_run("CORTEX_SEARCH"):
        cs_count = 0
        for schema in ordered_schemas:
            if schema_done("CORTEX_SEARCH", schema):
                continue
            result = migrate_cortex_search_services(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run, rewrite_db
            )
            results["total_migrated"] += result["migrated"]
            cs_count += result["migrated"]
            if result["errors"]:
                results["warnings"].extend(result["errors"])
            if result.get("skipped"):
                results["skipped"].extend(result["skipped"])
            mark_schema_done("CORTEX_SEARCH", schema)
        log_phase("CORTEX_SEARCH", cs_count)

    # Phase 11-12: PROCEDURES & FUNCTIONS
    if should_run("FUNCTIONS"):
        fn_count = 0
        for schema in ordered_schemas:
            if schema_done("FUNCTIONS", schema):
                continue
            result = migrate_functions(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("FUNCTIONS", 0, result["errors"][0])
                raise Exception(f"Functions failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            fn_count += result["migrated"]
            if result.get("skipped"):
                results["skipped"].extend(result["skipped"])
            mark_schema_done("FUNCTIONS", schema)
        log_phase("FUNCTIONS", fn_count)

    if should_run("PROCEDURES"):
        proc_count = 0
        for schema in ordered_schemas:
            if schema_done("PROCEDURES", schema):
                continue
            result = migrate_procedures(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("PROCEDURES", 0, result["errors"][0])
                raise Exception(f"Procedures failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            proc_count += result["migrated"]
            if result.get("skipped"):
                results["skipped"].extend(result["skipped"])
            mark_schema_done("PROCEDURES", schema)
        log_phase("PROCEDURES", proc_count)

    # Phase 13-17: TASKS, STREAMS, POLICIES, etc.
    if should_run("STREAMS"):
        streams_count = 0
        for schema in ordered_schemas:
            if schema_done("STREAMS", schema):
                continue
            result = migrate_streams(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("STREAMS", streams_count, result["errors"][0])
                raise Exception(f"Streams failed: {result['errors'][0]}")
            streams_count += result["migrated"]
            results["total_migrated"] += result["migrated"]
            mark_schema_done("STREAMS", schema)
        log_phase("STREAMS", streams_count)

    if should_run("POLICIES"):
        pol_count = 0
        for schema in ordered_schemas:
            if schema_done("POLICIES", schema):
                continue
            result = migrate_policies(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("POLICIES", 0, result["errors"][0])
                raise Exception(f"Policies failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            pol_count += result["migrated"]
            mark_schema_done("POLICIES", schema)
        log_phase("POLICIES", pol_count)

    # TASKS
    if should_run("TASKS"):
        task_count = 0
        for schema in ordered_schemas:
            if schema_done("TASKS", schema):
                continue
            result = migrate_tasks(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("TASKS", 0, result["errors"][0])
                raise Exception(f"Tasks failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            task_count += result["migrated"]
            mark_schema_done("TASKS", schema)
        log_phase("TASKS", task_count)

    if should_run("PIPES"):
        pipe_count = 0
        for schema in ordered_schemas:
            if schema_done("PIPES", schema):
                continue
            result = migrate_pipes(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("PIPES", 0, result["errors"][0])
                raise Exception(f"Pipes failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            pipe_count += result["migrated"]
            mark_schema_done("PIPES", schema)
        log_phase("PIPES", pipe_count)

    if should_run("ALERTS"):
        alert_count = 0
        for schema in ordered_schemas:
            if schema_done("ALERTS", schema):
                continue
            result = migrate_alerts(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run
            )
            if result["errors"]:
                log_phase("ALERTS", 0, result["errors"][0])
                raise Exception(f"Alerts failed: {result['errors'][0]}")
            results["total_migrated"] += result["migrated"]
            alert_count += result["migrated"]
            mark_schema_done("ALERTS", schema)
        log_phase("ALERTS", alert_count)

    # Phase 18: SEMANTIC VIEWS
    if should_run("SEMANTIC_VIEWS"):
        sv_count = 0
        for schema in ordered_schemas:
            if schema_done("SEMANTIC_VIEWS", schema):
                continue
            result = migrate_semantic_views(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run, rewrite_db
            )
            results["total_migrated"] += result["migrated"]
            sv_count += result["migrated"]
            if result["errors"]:
                results["warnings"].extend(result["errors"])
            mark_schema_done("SEMANTIC_VIEWS", schema)
        log_phase("SEMANTIC_VIEWS", sv_count)

    # Phase 18-19: APPS (Streamlit, Agents)
    if should_run("STREAMLITS"):
        st_count = 0
        for schema in ordered_schemas:
            if schema_done("STREAMLITS", schema):
                continue
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
            st_count += result["migrated"]
            mark_schema_done("STREAMLITS", schema)
        log_phase("STREAMLITS", st_count)

    if should_run("AGENTS"):
        ag_count = 0
        for schema in ordered_schemas:
            if schema_done("AGENTS", schema):
                continue
            result = migrate_agents(
                src_conn, tgt_conn, src_db, schema, tgt_db, schema, dry_run, rewrite_db
            )
            results["total_migrated"] += result["migrated"]
            ag_count += result["migrated"]
            mark_schema_done("AGENTS", schema)
        log_phase("AGENTS", ag_count)

    log("Migration completed successfully!")
    return results
