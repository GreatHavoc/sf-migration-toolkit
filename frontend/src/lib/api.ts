import type {
  ConnectionTestResponse,
  DefaultSettingsResponse,
  EnsureIntegrationResponse,
  EnsureStageResponse,
  InspectStageResponse,
  JobActionResponse,
  ListStageResponse,
  MigrationEvent,
  MigrationJobDetail,
  MigrationListResponse,
  MigrationRunRequest,
  MigrationRunResponse,
  NamespaceConfig,
  PrecheckResponse,
  SchemaOrderResponse,
  SnowflakeConnectionPayload,
  StageConfig,
} from "@/types/api";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000/api";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });

  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try {
      const body = (await response.json()) as { detail?: string };
      if (body.detail) {
        message = body.detail;
      }
    } catch {
      // ignore JSON parse errors for non-JSON responses
    }
    throw new Error(message);
  }

  return (await response.json()) as T;
}

export function splitSchemaInput(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

export async function getDefaults(): Promise<DefaultSettingsResponse> {
  return request<DefaultSettingsResponse>("/health/defaults", { method: "GET" });
}

export async function testConnection(
  connection: SnowflakeConnectionPayload,
): Promise<ConnectionTestResponse> {
  return request<ConnectionTestResponse>("/connections/test", {
    method: "POST",
    body: JSON.stringify({ connection }),
  });
}

export async function listDatabases(
  connection: SnowflakeConnectionPayload,
): Promise<{ databases: string[] }> {
  return request<{ databases: string[] }>("/connections/databases", {
    method: "POST",
    body: JSON.stringify({ connection }),
  });
}

export async function listSchemas(
  connection: SnowflakeConnectionPayload,
  database: string,
): Promise<{ schemas: string[] }> {
  return request<{ schemas: string[] }>("/connections/schemas", {
    method: "POST",
    body: JSON.stringify({ connection, database }),
  });
}

export async function ensureIntegration(
  connection: SnowflakeConnectionPayload,
  integrationName: string,
  azureTenantId: string,
  stage: StageConfig,
): Promise<EnsureIntegrationResponse> {
  return request<EnsureIntegrationResponse>("/integration/ensure", {
    method: "POST",
    body: JSON.stringify({
      connection,
      integration_name: integrationName,
      azure_tenant_id: azureTenantId,
      stage,
    }),
  });
}

export async function ensureStage(
  connection: SnowflakeConnectionPayload,
  namespace: NamespaceConfig,
  integrationName: string,
  stage: StageConfig,
): Promise<EnsureStageResponse> {
  return request<EnsureStageResponse>("/integration/stage/ensure", {
    method: "POST",
    body: JSON.stringify({
      connection,
      namespace,
      integration_name: integrationName,
      stage,
    }),
  });
}

export async function inspectStage(
  connection: SnowflakeConnectionPayload,
  namespace: NamespaceConfig,
): Promise<InspectStageResponse> {
  return request<InspectStageResponse>("/integration/stage/inspect", {
    method: "POST",
    body: JSON.stringify({ connection, namespace }),
  });
}

export async function listStage(
  connection: SnowflakeConnectionPayload,
  namespace: NamespaceConfig,
): Promise<ListStageResponse> {
  return request<ListStageResponse>("/integration/stage/list", {
    method: "POST",
    body: JSON.stringify({ connection, namespace }),
  });
}

export async function runPrecheck(
  connection: SnowflakeConnectionPayload,
  sourceDb: string,
  schemas: string[],
): Promise<PrecheckResponse> {
  return request<PrecheckResponse>("/analysis/precheck", {
    method: "POST",
    body: JSON.stringify({
      connection,
      source_db: sourceDb,
      schemas,
    }),
  });
}

export async function runSchemaOrder(
  connection: SnowflakeConnectionPayload,
  sourceDb: string,
  schemas: string[],
): Promise<SchemaOrderResponse> {
  return request<SchemaOrderResponse>("/analysis/schema-order", {
    method: "POST",
    body: JSON.stringify({
      connection,
      source_db: sourceDb,
      schemas,
    }),
  });
}

export async function startMigration(
  payload: MigrationRunRequest,
): Promise<MigrationRunResponse> {
  return request<MigrationRunResponse>("/migrations/start", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function listMigrations(limit = 50): Promise<MigrationListResponse> {
  return request<MigrationListResponse>(`/migrations?limit=${limit}`, { method: "GET" });
}

export async function getMigration(jobId: string): Promise<MigrationJobDetail> {
  return request<MigrationJobDetail>(`/migrations/${jobId}`, { method: "GET" });
}

export async function cancelMigration(jobId: string): Promise<JobActionResponse> {
  return request<JobActionResponse>(`/migrations/${jobId}/cancel`, { method: "POST" });
}

export async function resumeMigration(jobId: string): Promise<JobActionResponse> {
  return request<JobActionResponse>(`/migrations/${jobId}/resume`, { method: "POST" });
}

type StreamHandlers = {
  onEvent: (event: MigrationEvent) => void;
  onEnded?: (payload: { job_id: string; run_id: string; status: string }) => void;
  onError?: (error: Error) => void;
};

const STREAM_EVENT_TYPES = [
  "job.queued",
  "job.started",
  "job.prepared",
  "phase.completed",
  "phase.failed",
  "job.completed",
  "job.failed",
  "job.cancel_requested",
];

export function openMigrationStream(
  jobId: string,
  handlers: StreamHandlers,
  afterEventId = 0,
): () => void {
  const url = `${API_BASE}/migrations/${jobId}/events?after_event_id=${afterEventId}`;
  const source = new EventSource(url);

  const listeners: Array<{ type: string; listener: EventListener }> = [];

  for (const type of STREAM_EVENT_TYPES) {
    const listener = (raw: Event) => {
      const event = raw as MessageEvent<string>;
      try {
        handlers.onEvent(JSON.parse(event.data) as MigrationEvent);
      } catch {
        handlers.onError?.(new Error("Failed to parse stream event payload"));
      }
    };
    source.addEventListener(type, listener);
    listeners.push({ type, listener });
  }

  const streamEndedListener: EventListener = (raw: Event) => {
    const event = raw as MessageEvent<string>;
    try {
      handlers.onEnded?.(
        JSON.parse(event.data) as { job_id: string; run_id: string; status: string },
      );
    } catch {
      handlers.onError?.(new Error("Failed to parse stream end payload"));
    }
  };
  source.addEventListener("stream.ended", streamEndedListener);
  listeners.push({ type: "stream.ended", listener: streamEndedListener });

  source.onerror = () => {
    handlers.onError?.(new Error("Event stream disconnected"));
  };

  return () => {
    for (const item of listeners) {
      source.removeEventListener(item.type, item.listener);
    }
    source.close();
  };
}
