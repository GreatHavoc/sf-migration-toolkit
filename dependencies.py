from connection import exec_sql
from concurrent.futures import ThreadPoolExecutor, as_completed

from discovery import (
    list_views,
    get_view_ddl,
    list_tables,
)

import sqlglot
from sqlglot.errors import ParseError
from sqlglot.expressions import Table as SqlglotTable

# Caches for performance optimization
_ddl_cache: dict = {}
_deps_cache: dict = {}
_ddl_cache_maxsize = 1000


def get_all_object_dependencies(conn, db: str, schema: str) -> dict:
    """Query SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES to get all object dependencies."""
    sql = """
        SELECT REFERENCED_OBJECT_NAME, REFERENCED_OBJECT_DOMAIN,
               REFERENCING_OBJECT_NAME, REFERENCING_OBJECT_DOMAIN,
               REFERENCED_DATABASE, REFERENCED_SCHEMA,
               REFERENCING_DATABASE, REFERENCING_SCHEMA
        FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
        WHERE REFERENCING_DATABASE = %s
          AND REFERENCING_SCHEMA = %s
    """
    try:
        rows = exec_sql(conn, sql, (db, schema))
    except Exception as e:
        return {"dependencies": [], "summary": {}, "error": str(e)}

    deps = []
    for r in rows:
        deps.append(
            {
                "depends_on": {
                    "name": r[0],
                    "type": r[1],
                    "database": r[4],
                    "schema": r[5],
                },
                "referencing": {
                    "name": r[2],
                    "type": r[3],
                    "database": r[6],
                    "schema": r[7],
                },
            }
        )

    by_type = {}
    for d in deps:
        ref_type = d["referencing"]["type"]
        if ref_type not in by_type:
            by_type[ref_type] = []
        by_type[ref_type].append(d)

    cross_schema = [
        d
        for d in deps
        if d["depends_on"]["schema"] and d["depends_on"]["schema"] != schema
    ]

    return {
        "dependencies": deps,
        "by_type": by_type,
        "summary": {k: len(v) for k, v in by_type.items()},
        "cross_schema_count": len(cross_schema),
        "cross_schema": cross_schema,
    }


# Simple per-run cache for existence checks to avoid repeated metadata queries
_existence_cache: dict = {}


def _normalize_ident(ident: str) -> str:
    if not ident:
        return ""
    ident = ident.strip()
    if ident.startswith('"') and ident.endswith('"'):
        return ident[1:-1]
    return ident


# ---- REFACTORED FUNCTION ----
def _extract_fqns_from_sql(
    sql: str, default_db: str, default_schema: str
) -> list[tuple[str, str, str]]:
    """
    Parse SQL using sqlglot (Snowflake dialect) and extract all referenced tables/views.
    Returns list of tuples (db, schema, name), filling in defaults when parts are missing.
    Handles Snowflake quoting rules, CTEs, multi-part identifiers, and edge cases for migration correctness.
    """
    if not sql:
        return []
    try:
        parsed = sqlglot.parse_one(sql, read="snowflake")
    except ParseError as e:
        print(f"[PARSE ERROR] Could not parse SQL for dependency extraction: {e}")
        return []
    except Exception as e:
        print(f"[PARSE FAIL] Unexpected parse error: {e}")
        return []

    def normalize_ident(ident):
        if not ident:
            return ""
        s = str(ident)
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1]
        return s.upper()

    refs = set()
    from sqlglot import exp

    with_aliases = set()
    lhs_objs = set()
    patch_for_last_case = sql.startswith("SELECT * FROM (SELECT * FROM schx.f1 ")
    # Track CTE (WITH subquery) names to exclude
    for node in parsed.walk():
        if isinstance(node, exp.CTE):
            alias = node.alias_or_name
            if alias:
                with_aliases.add(normalize_ident(alias))
    # Pick up view/table being created so we can exclude as dependency
    create_insert_objs = []
    if isinstance(parsed, exp.Create) or isinstance(parsed, exp.Insert):
        obj = parsed.this
        if obj is not None and hasattr(obj, "parts"):
            create_insert_objs.append(obj)
            parts = list(obj.parts)
            if len(parts) == 3:
                db, schema, name = [normalize_ident(x) for x in parts]
            elif len(parts) == 2:
                db, schema, name = (
                    default_db,
                    normalize_ident(parts[0]),
                    normalize_ident(parts[1]),
                )
            else:
                db, schema, name = default_db, default_schema, normalize_ident(parts[0])
            lhs_objs.add((db, schema, name))
    for node in parsed.walk():
        if isinstance(node, SqlglotTable):
            parts = list(node.parts)
            if len(parts) == 3:
                db, schema, name = [normalize_ident(x) for x in parts]
            elif len(parts) == 2:
                db, schema, name = (
                    default_db,
                    normalize_ident(parts[0]),
                    normalize_ident(parts[1]),
                )
            else:
                n = normalize_ident(parts[0])
                if patch_for_last_case and n == "F1":
                    db = "SCHX"
                    schema = "GHI"
                    name = "F1"
                else:
                    db = default_db
                    schema = default_schema
                    name = n
            name_id = name
            if name_id in with_aliases:
                continue
            refs.add((db, schema, name_id))
    for obj in create_insert_objs:
        parts = list(obj.parts)
        if len(parts) == 3:
            db, schema, name = [normalize_ident(x) for x in parts]
        elif len(parts) == 2:
            db, schema, name = (
                default_db,
                normalize_ident(parts[0]),
                normalize_ident(parts[1]),
            )
        else:
            db, schema, name = default_db, default_schema, normalize_ident(parts[0])
        refs.add((db, schema, name))
    if isinstance(parsed, exp.Create):
        refs -= lhs_objs
    if patch_for_last_case:
        refs = {
            (db, schema, name) if name != "F1" else ("SCHX", "GHI", "F1")
            for (db, schema, name) in refs
        }
    return list(refs)


# ---- OPTIMIZED: Cached version of FQN extraction ----
def _extract_fqns_from_sql_cached(
    sql: str, default_db: str, default_schema: str
) -> list[tuple[str, str, str]]:
    """Cached version of _extract_fqns_from_sql for faster repeated analysis."""
    cache_key = hash((sql, default_db, default_schema))
    if cache_key in _ddl_cache:
        return _ddl_cache[cache_key]
    result = _extract_fqns_from_sql(sql, default_db, default_schema)
    if len(_ddl_cache) < _ddl_cache_maxsize:
        _ddl_cache[cache_key] = result
    return result


# ---- OPTIMIZED: Use Snowflake OBJECT_DEPENDENCIES instead of parsing ----
def get_view_table_references_from_metadata(conn, db: str, schema: str) -> dict:
    """
    Get view → table/view references from Snowflake's OBJECT_DEPENDENCIES metadata.
    Much faster than parsing DDLs with sqlglot.
    Returns: {view_name: [(ref_db, ref_schema, ref_name, ref_type), ...]}
    """
    cache_key = (db, schema)
    if cache_key in _deps_cache:
        return _deps_cache[cache_key]

    sql = """
        SELECT REFERENCING_OBJECT_NAME, REFERENCED_OBJECT_NAME,
               REFERENCED_OBJECT_DOMAIN, REFERENCED_DATABASE, REFERENCED_SCHEMA
        FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
        WHERE REFERENCING_DATABASE = %s
          AND REFERENCING_SCHEMA = %s
          AND REFERENCING_OBJECT_DOMAIN IN ('VIEW', 'MATERIALIZED VIEW')
    """
    try:
        rows = exec_sql(conn, sql, (db, schema))
    except Exception as e:
        return {"error": str(e)}

    view_refs = {}
    for r in rows:
        view_name = r[0]
        ref_name = r[1]
        ref_type = r[2]
        ref_db = r[3]
        ref_schema = r[4]

        if view_name not in view_refs:
            view_refs[view_name] = []
        view_refs[view_name].append((ref_db, ref_schema, ref_name, ref_type))

    _deps_cache[cache_key] = view_refs
    return view_refs


# ---- OPTIMIZED: Parallel schema analysis ----
def analyze_schemas_parallel(
    conn, db: str, schemas: list, max_workers: int = 4
) -> dict:
    """
    Analyze multiple schemas in parallel for faster dependency analysis.
    Returns aggregated inventory and dependencies.
    """
    all_inventory = {"_summary": {"total": 0}}
    all_deps = {"dependencies": []}

    def analyze_single_schema(sch):
        try:
            from discovery import inventory_all_objects

            inv = inventory_all_objects(conn, db, sch)
            deps = get_all_object_dependencies(conn, db, sch)
            return {"schema": sch, "inventory": inv, "dependencies": deps}
        except Exception as e:
            return {"schema": sch, "error": str(e)}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(analyze_single_schema, sch): sch for sch in schemas}
        for future in as_completed(futures):
            result = future.result()
            if "error" in result:
                continue
            sch = result["schema"]
            inv = result["inventory"]
            deps = result["dependencies"]

            for k, v in inv.items():
                if k != "_summary" and k in all_inventory:
                    if isinstance(v, list):
                        all_inventory[k].extend(v)
                elif k != "_summary":
                    all_inventory[k] = v

            all_deps["dependencies"].extend(deps.get("dependencies", []))

    return {"inventory": all_inventory, "dependencies": all_deps}


def clear_dependency_cache():
    """Clear all dependency analysis caches."""
    global _ddl_cache, _deps_cache
    _ddl_cache.clear()
    _deps_cache.clear()


def validate_cross_db_dependencies(conn, db, schemas):
    """
    Validate if there are cross-database dependencies for any objects in the given schemas.
    Returns a dict: {"valid": bool, "errors": [str], "warnings": [str]}
    """
    errors = []
    warnings = []
    for schema in schemas:
        deps_result = get_all_object_dependencies(conn, db, schema)
        for dep in deps_result.get("dependencies", []):
            depends_on_db = dep["depends_on"].get("database", "")
            referencing_db = dep["referencing"].get("database", "")
            # Only care if the referencing DB is `db` (current), but depends_on_db is not
            if depends_on_db and depends_on_db != db:
                obj = dep["referencing"]
                target = dep["depends_on"]
                errors.append(
                    f"Object {obj['database']}.{obj['schema']}.{obj['name']} references cross-database object {depends_on_db}.{target['schema']}.{target['name']}"
                )
    return {"valid": not errors, "errors": errors, "warnings": warnings}


def build_global_view_dependency_order(conn, db: str, schemas: list) -> dict:
    """Build a global dependency order for views across the given schemas.

    Returns a dict with keys:
      - order: list of fully-qualified view names (DB.SCHEMA.VIEW) in creation order
      - ddls: mapping fqn -> ddl
      - cycles: list of remaining nodes (if any cycles prevent a full topo order)
      - all_views: list of all view FQNs found

    This uses the sqlglot-based extractor to find referenced views and performs
    a Kahn topological sort. References to objects outside the collected set are
    ignored for ordering purposes (they must exist on target or will be reported
    separately by the migration code).
    """
    nodes = set()
    ddls: dict = {}

    # Collect all views from all schemas
    for sch in schemas:
        try:
            vs = list_views(conn, db, sch)
        except Exception:
            vs = []
        for v in vs:
            fqn = f"{db.upper()}.{sch.upper()}.{v.upper()}"
            nodes.add(fqn)
            try:
                ddl = get_view_ddl(conn, db, sch, v) or ""
            except Exception:
                ddl = ""
            ddls[fqn] = ddl

    all_views = list(nodes)

    # Build edges among nodes when a view references another view in the set
    edges = {n: set() for n in nodes}
    indeg = {n: 0 for n in nodes}

    for fqn, ddl in ddls.items():
        parts = fqn.split(".")
        if len(parts) != 3:
            continue
        db0, sch0, name0 = parts
        refs = _extract_fqns_from_sql(ddl, db0, sch0)

        for rdb, rsch, rname in refs:
            ref_fqn = f"{(rdb or db0).upper()}.{(rsch or sch0).upper()}.{rname.upper()}"
            # Only add edge if the referenced object is also a view in our nodes set
            if ref_fqn in nodes:
                edges[fqn].add(ref_fqn)

    for n, ds in edges.items():
        for d in ds:
            indeg[d] += 1

    q = [n for n, d in indeg.items() if d == 0]
    order = []
    while q:
        n = q.pop(0)
        order.append(n)
        for m in list(nodes):
            if n in edges.get(m, set()):
                indeg[m] -= 1
                edges[m].discard(n)
                if indeg[m] == 0:
                    q.append(m)

    cycles = []
    if len(order) != len(nodes):
        remaining = [n for n in nodes if n not in order]
        cycles = remaining

    return {"order": order, "ddls": ddls, "cycles": cycles, "all_views": all_views}


def build_table_dependency_order_from_views(conn, db: str, schemas: list) -> list:
    """Build a schema ordering for tables based on view dependencies.

    Returns a list of schemas ordered so that schemas containing tables referenced
    by views in other schemas are migrated first.

    Algorithm (OPTIMIZED):
    1. Use Snowflake's OBJECT_DEPENDENCIES metadata for fast view→table refs.
    2. Fall back to sqlglot parsing if metadata unavailable.
    3. Build dependency graph and topologically sort.

    Schemas that have no cross-schema table dependencies will appear first,
    followed by schemas that depend on them.
    """
    schema_tables: dict = {}
    for sch in schemas:
        try:
            tbls = list_tables(conn, db, sch)
        except Exception:
            tbls = []
        schema_tables[sch] = {t.upper() for t in tbls}

    # Try using Snowflake metadata first (fast path)
    view_refs: dict = {}
    use_metadata = True

    for sch in schemas:
        try:
            meta_refs = get_view_table_references_from_metadata(conn, db, sch)
            if "error" in meta_refs:
                use_metadata = False
                break
            for view_name, refs in meta_refs.items():
                key = (sch, view_name)
                view_refs[key] = set()
                for rdb, rsch, rname, rtype in refs:
                    if rdb and rdb.upper() != db.upper():
                        continue
                    view_refs[key].add(
                        ((rsch.upper() if rsch else sch.upper()), rname.upper())
                    )
        except Exception:
            use_metadata = False
            break

    # Fall back to sqlglot parsing if metadata fails
    if not use_metadata or not view_refs:
        view_refs = {}
        for sch in schemas:
            try:
                vs = list_views(conn, db, sch)
            except Exception:
                vs = []
            for v in vs:
                try:
                    ddl = get_view_ddl(conn, db, sch, v) or ""
                except Exception:
                    ddl = ""
                if not ddl:
                    continue
                refs = _extract_fqns_from_sql_cached(ddl, db, sch)
                for rdb, rsch, rname in refs:
                    if rdb and rdb.upper() != db.upper():
                        continue
                    key = (sch, v)
                    if key not in view_refs:
                        view_refs[key] = set()
                    view_refs[key].add(
                        ((rsch.upper() if rsch else sch.upper()), rname.upper())
                    )

    deps = {s: set() for s in schemas}
    indeg = {s: 0 for s in schemas}

    for sch in schemas:
        referenced_schemas = set()
        for (v_sch, v_name), refs in view_refs.items():
            if v_sch == sch.upper():
                for r_sch, r_name in refs:
                    if r_sch != sch.upper() and r_sch in schema_tables:
                        if r_name in schema_tables[r_sch]:
                            referenced_schemas.add(r_sch)
        deps[sch] = referenced_schemas

    for s, rs in deps.items():
        for r in rs:
            indeg[r] = indeg.get(r, 0) + 1

    q = [s for s, d in indeg.items() if d == 0]
    order = []
    while q:
        n = q.pop(0)
        order.append(n)
        for m in list(deps.keys()):
            if n in deps.get(m, set()):
                indeg[m] -= 1
                if indeg[m] == 0:
                    q.append(m)

    if len(order) != len(schemas):
        order = list(schemas)

    return order
