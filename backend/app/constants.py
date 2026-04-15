"""Shared constants for backend APIs and migration runtime."""

MIGRATION_PHASES = [
    "CREATE_SCHEMAS",
    "SEQUENCES",
    "FILE_FORMATS",
    "TAGS",
    "TABLE_DDLS",
    "TABLE_DATA",
    "STAGES",
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
    "AGENTS",
]

TERMINAL_JOB_STATES = {"succeeded", "failed", "cancelled"}
