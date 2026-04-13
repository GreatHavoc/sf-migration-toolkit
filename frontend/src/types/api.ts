export const MIGRATION_PHASES = [
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
] as const;

export type MigrationPhase = (typeof MIGRATION_PHASES)[number];

export interface SnowflakeConnectionPayload {
  account: string;
  user: string;
  password: string;
  role?: string;
  warehouse?: string;
  passcode?: string;
}

export interface StageConfig {
  storage_account: string;
  container: string;
  prefix: string;
}

export interface NamespaceConfig {
  mig_db: string;
  mig_schema: string;
  stage_name: string;
}

export interface DefaultSettingsResponse {
  mig_db: string;
  mig_schema: string;
  stage_name: string;
  integration_name: string;
  nb_int_stage_name: string;
  local_int_stage_name: string;
}

export interface ConnectionTestResponse {
  ok: boolean;
  account: string;
  user: string;
  role?: string;
  warehouse?: string;
}

export interface EnsureIntegrationResponse {
  ok: boolean;
  integration_name: string;
  stage_url: string;
  message: string;
}

export interface EnsureStageResponse {
  ok: boolean;
  stage_fqn: string;
  stage_url: string;
  integration_name: string;
  message: string;
}

export interface InspectStageResponse {
  stage_fqn: string;
  properties: Record<string, string>;
}

export interface ListStageResponse {
  stage_fqn: string;
  rows: string[][];
  count: number;
}

export interface PrecheckResponse {
  source_db: string;
  schemas: string[];
  valid: boolean;
  errors: string[];
  warnings: string[];
  inventory_summary?: Record<string, number>;
}

export interface SchemaOrderResponse {
  source_db: string;
  schemas: string[];
  ordered_schemas: string[];
}

export interface MigrationRunRequest {
  source_connection: SnowflakeConnectionPayload;
  target_connection: SnowflakeConnectionPayload;
  namespace: NamespaceConfig;
  databases: {
    source_db: string;
    target_db: string;
  };
  stage: {
    integration_name: string;
    azure_tenant_id: string;
    storage_account: string;
    container: string;
    prefix: string;
    stage_prefix: string;
  };
  schemas?: string[];
  selected_phases?: string[];
  run_id?: string;
  dry_run: boolean;
}

export interface MigrationRunResponse {
  run_id: string;
  job_id: string;
  status: string;
  message: string;
}

export interface MigrationEvent {
  event_id: number;
  job_id: string;
  run_id: string;
  event_type: string;
  message: string;
  phase?: string;
  count?: number;
  error?: string;
  created_at: string;
}

export interface MigrationJobSummary {
  job_id: string;
  run_id: string;
  status: string;
  source_db: string;
  target_db: string;
  dry_run: boolean;
  created_at: string;
  updated_at: string;
}

export interface MigrationJobDetail extends MigrationJobSummary {
  schemas: string[];
  selected_phases: string[];
  error?: string;
  result?: Record<string, unknown>;
}

export interface MigrationListResponse {
  jobs: MigrationJobSummary[];
}

export interface JobActionResponse {
  job_id: string;
  run_id: string;
  status: string;
  message: string;
}

export interface ListDatabasesResponse {
  databases: string[];
}

export interface ListSchemasResponse {
  schemas: string[];
}
