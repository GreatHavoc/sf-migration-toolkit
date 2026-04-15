"use client";

import React, { createContext, useContext, useState, useRef, useEffect, ReactNode } from "react";
import { App } from "antd";
import {
  cancelMigration,
  ensureIntegration,
  ensureStage,
  getDefaults,
  getMigration,
  inspectStage,
  listDatabases,
  listSchemas,
  listMigrations,
  listStage,
  openMigrationStream,
  resumeMigration,
  runPrecheck,
  runSchemaOrder,
  splitSchemaInput,
  startMigration,
  testConnection,
} from "@/lib/api";
import {
  MIGRATION_PHASES,
  type DefaultSettingsResponse,
  type MigrationEvent,
  type MigrationJobDetail,
  type MigrationJobSummary,
  type MigrationRunRequest,
  type NamespaceConfig,
  type PrecheckResponse,
  type SchemaOrderResponse,
  type SnowflakeConnectionPayload,
} from "@/types/api";

const EMPTY_CONNECTION: SnowflakeConnectionPayload = {
  account: "",
  user: "",
  password: "",
  role: "",
  warehouse: "",
  passcode: "",
};

const EMPTY_NAMESPACE: NamespaceConfig = {
  mig_db: "MIGRATION_DB",
  mig_schema: "PUBLIC",
  stage_name: "MIGRATION_STAGE",
};

export type StageState = {
  integration_name: string;
  azure_tenant_id: string;
  storage_account: string;
  container: string;
  prefix: string;
  stage_prefix: string;
};

const EMPTY_STAGE: StageState = {
  integration_name: "AZURE_MIGRATION_INT",
  azure_tenant_id: "",
  storage_account: "",
  container: "",
  prefix: "exports/",
  stage_prefix: "sf_migration",
};

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return "Unexpected error";
}

interface DashboardContextProps {
  currentStep: number;
  setCurrentStep: (step: number) => void;
  defaults: DefaultSettingsResponse | null;
  loadingBoot: boolean;
  busyKey: string | null;
  runBusy: (action: string, task: () => Promise<void>) => Promise<void>;

  sourceConnection: SnowflakeConnectionPayload;
  targetConnection: SnowflakeConnectionPayload;
  sourceValidated: boolean;
  targetValidated: boolean;
  updateConnectionField: (side: "source" | "target", field: keyof SnowflakeConnectionPayload, value: string) => void;
  handleTestBoth: () => Promise<void>;
  requireConnections: () => void;

  sourceDatabases: string[];
  targetDatabases: string[];
  sourceSchemas: string[];

  namespace: NamespaceConfig;
  setNamespace: React.Dispatch<React.SetStateAction<NamespaceConfig>>;
  stage: StageState;
  setStage: React.Dispatch<React.SetStateAction<StageState>>;

  sourceDb: string;
  setSourceDb: (val: string) => void;
  targetDb: string;
  setTargetDb: (val: string) => void;
  schemaInput: string[];
  setSchemaInput: (val: string[]) => void;
  selectedPhases: string[];
  setSelectedPhases: (val: string[]) => void;
  dryRun: boolean;
  setDryRun: (val: boolean) => void;
  runId: string;
  setRunId: (val: string) => void;

  precheckResult: PrecheckResponse | null;
  schemaOrderResult: SchemaOrderResponse | null;
  stageInspectData: Record<string, string> | null;
  stageListRows: string[][];
  handleOneClickSetup: () => Promise<void>;
  inspectTargetStage: () => Promise<void>;
  listTargetStage: () => Promise<void>;

  history: MigrationJobSummary[];
  activeJobId: string | null;
  activeJob: MigrationJobDetail | null;
  events: MigrationEvent[];
  setEvents: React.Dispatch<React.SetStateAction<MigrationEvent[]>>;
  refreshHistory: () => Promise<void>;
  refreshJob: (jobId: string) => Promise<void>;
  connectJobStream: (jobId: string, afterEventId?: number) => void;
  launchMigration: () => Promise<void>;
  cancelMigrationJob: () => Promise<void>;
  resumeMigrationJob: () => Promise<void>;
  
  phaseStatus: Map<string, { status: "pending" | "done" | "failed"; count: number }>;
}

const DashboardContext = createContext<DashboardContextProps | undefined>(undefined);

export function DashboardProvider({ children }: { children: ReactNode }) {
  const { message } = App.useApp();

  const [currentStep, setCurrentStep] = useState(0);

  const [defaults, setDefaults] = useState<DefaultSettingsResponse | null>(null);
  const [loadingBoot, setLoadingBoot] = useState(true);
  const [busyKey, setBusyKey] = useState<string | null>(null);

  const [sourceConnection, setSourceConnection] = useState<SnowflakeConnectionPayload>(EMPTY_CONNECTION);
  const [targetConnection, setTargetConnection] = useState<SnowflakeConnectionPayload>(EMPTY_CONNECTION);

  useEffect(() => {
    try {
      const cachedSource = localStorage.getItem("mig_source_connection");
      const cachedTarget = localStorage.getItem("mig_target_connection");
      if (cachedSource) {
        setSourceConnection((prev) => ({ ...prev, ...JSON.parse(cachedSource) }));
      }
      if (cachedTarget) {
        setTargetConnection((prev) => ({ ...prev, ...JSON.parse(cachedTarget) }));
      }
      
      const cachedNamespace = localStorage.getItem("mig_namespace");
      const cachedStage = localStorage.getItem("mig_stage");
      if (cachedNamespace) {
        setNamespace((prev) => ({ ...prev, ...JSON.parse(cachedNamespace) }));
      }
      if (cachedStage) {
        setStage((prev) => ({ ...prev, ...JSON.parse(cachedStage) }));
      }
    } catch (e) {
      console.warn("Failed to load cached connections or settings", e);
    }
  }, []);

  useEffect(() => {
    // Only save non-sensitive fields
    const { password: _p1, passcode: _pc1, ...safeSource } = sourceConnection;
    const { password: _p2, passcode: _pc2, ...safeTarget } = targetConnection;
    localStorage.setItem("mig_source_connection", JSON.stringify(safeSource));
    localStorage.setItem("mig_target_connection", JSON.stringify(safeTarget));
  }, [sourceConnection, targetConnection]);

  const [sourceValidated, setSourceValidated] = useState(false);
  const [targetValidated, setTargetValidated] = useState(false);
  const [sourceDatabases, setSourceDatabases] = useState<string[]>([]);
  const [targetDatabases, setTargetDatabases] = useState<string[]>([]);
  const [sourceSchemas, setSourceSchemas] = useState<string[]>([]);

  const [namespace, setNamespace] = useState<NamespaceConfig>(EMPTY_NAMESPACE);
  const [stage, setStage] = useState<StageState>(EMPTY_STAGE);

  useEffect(() => {
    localStorage.setItem("mig_namespace", JSON.stringify(namespace));
  }, [namespace]);

  useEffect(() => {
    localStorage.setItem("mig_stage", JSON.stringify(stage));
  }, [stage]);

  const [sourceDb, setSourceDb] = useState("");
  const [targetDb, setTargetDb] = useState("");
  const [schemaInput, setSchemaInput] = useState<string[]>([]);
  const [selectedPhases, setSelectedPhases] = useState<string[]>([...MIGRATION_PHASES]);
  const [dryRun, setDryRun] = useState(false);
  const [runId, setRunId] = useState("");

  const [precheckResult, setPrecheckResult] = useState<PrecheckResponse | null>(null);
  const [schemaOrderResult, setSchemaOrderResult] = useState<SchemaOrderResponse | null>(null);
  const [stageInspectData, setStageInspectData] = useState<Record<string, string> | null>(null);
  const [stageListRows, setStageListRows] = useState<string[][]>([]);

  const [history, setHistory] = useState<MigrationJobSummary[]>([]);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [activeJob, setActiveJob] = useState<MigrationJobDetail | null>(null);
  const [events, setEvents] = useState<MigrationEvent[]>([]);

  const streamCloseRef = useRef<(() => void) | null>(null);

  useEffect(() => {
    let alive = true;
    const run = async () => {
      setLoadingBoot(true);
      try {
        const [defaultsResponse, historyResponse] = await Promise.all([
          getDefaults(),
          listMigrations(50),
        ]);
        if (!alive) return;
        setDefaults(defaultsResponse);
        
        // Only set defaults if we didn't load from cache
        setNamespace((prev) => {
          if (prev.mig_db !== EMPTY_NAMESPACE.mig_db) return prev;
          return {
            ...prev,
            mig_db: defaultsResponse.mig_db,
            mig_schema: defaultsResponse.mig_schema,
            stage_name: defaultsResponse.stage_name,
          };
        });
        setStage((prev) => {
          if (prev.integration_name !== EMPTY_STAGE.integration_name) return prev;
          return {
            ...prev,
            integration_name: defaultsResponse.integration_name,
          };
        });
        setHistory(historyResponse.jobs);
      } catch (error) {
        if (alive) {
          message.error(toErrorMessage(error));
        }
      } finally {
        if (alive) {
          setLoadingBoot(false);
        }
      }
    };
    void run();
    return () => {
      alive = false;
      streamCloseRef.current?.();
    };
  }, [message]);

  const phaseStatus = React.useMemo(() => {
    const table = new Map<string, { status: "pending" | "done" | "failed"; count: number }>();
    for (const phase of selectedPhases) {
      table.set(phase, { status: "pending", count: 0 });
    }
    for (const event of events) {
      if (!event.phase || !table.has(event.phase)) continue;
      if (event.event_type === "phase.failed") {
        table.set(event.phase, { status: "failed", count: event.count || 0 });
      }
      if (event.event_type === "phase.completed") {
        table.set(event.phase, { status: "done", count: event.count || 0 });
      }
    }
    return table;
  }, [events, selectedPhases]);

  async function runBusy(action: string, task: () => Promise<void>) {
    setBusyKey(action);
    try {
      await task();
    } catch (error) {
      message.error(toErrorMessage(error));
    } finally {
      setBusyKey(null);
    }
  }

  function requireConnections() {
    if (!sourceValidated || !targetValidated) {
      throw new Error("Test and validate both source and target connections first.");
    }
  }

  function updateConnectionField(
    side: "source" | "target",
    field: keyof SnowflakeConnectionPayload,
    value: string,
  ) {
    if (side === "source") {
      setSourceConnection((prev) => ({ ...prev, [field]: value }));
      if (field !== "passcode") setSourceValidated(false);
    } else {
      setTargetConnection((prev) => ({ ...prev, [field]: value }));
      if (field !== "passcode") setTargetValidated(false);
    }
  }

  async function handleTestBoth() {
    await runBusy("test-both", async () => {
      // First test the connections to establish session and validate credentials
      await Promise.all([
        testConnection(sourceConnection),
        testConnection(targetConnection),
      ]);
      
      // Then fetch databases
      const [srcDbs, tgtDbs] = await Promise.all([
        listDatabases(sourceConnection),
        listDatabases(targetConnection)
      ]);
      
      // Only set validated if EVERYTHING succeeded
      setSourceDatabases(srcDbs.databases);
      setTargetDatabases(tgtDbs.databases);
      setSourceValidated(true);
      setTargetValidated(true);
      message.success("Both connections look good and databases loaded.");
    });
  }

  function getStagePayload() {
    return {
      storage_account: stage.storage_account,
      container: stage.container,
      prefix: stage.prefix,
    };
  }

  async function handleSourceDbChange(val: string) {
    if (!targetDb || targetDb === sourceDb) {
      setTargetDb(val);
    }
    setSourceDb(val);
    setSchemaInput([]); // Reset selected schemas when DB changes
    if (val && sourceValidated) {
      try {
        const res = await listSchemas(sourceConnection, val);
        setSourceSchemas(res.schemas);
      } catch (err) {
        message.error("Failed to load schemas for selected DB.");
      }
    } else {
      setSourceSchemas([]);
    }
  }

  async function handleOneClickSetup() {
    await runBusy("setup-and-precheck", async () => {
      requireConnections();
      if (!sourceDb.trim() || !targetDb.trim()) {
        throw new Error("Source and Target DBs are required for precheck.");
      }
      if (!stage.azure_tenant_id.trim() || !stage.storage_account.trim() || !stage.container.trim()) {
        throw new Error("Azure Tenant ID, Storage Account, and Container are required for stage setup.");
      }

      const stagePayload = getStagePayload();

      // Ensure Integration
      await Promise.all([
        ensureIntegration(sourceConnection, stage.integration_name, stage.azure_tenant_id, stagePayload),
        ensureIntegration(targetConnection, stage.integration_name, stage.azure_tenant_id, stagePayload),
      ]);

      // Ensure Stage
      await Promise.all([
        ensureStage(sourceConnection, namespace, stage.integration_name, stagePayload),
        ensureStage(targetConnection, namespace, stage.integration_name, stagePayload),
      ]);

      // Run Precheck & Ordering
      const schemas = schemaInput; // It's an array now
      const [precheck, ordered] = await Promise.all([
        runPrecheck(sourceConnection, sourceDb.trim(), schemas),
        runSchemaOrder(sourceConnection, sourceDb.trim(), schemas),
      ]);

      setPrecheckResult(precheck);
      setSchemaOrderResult(ordered);

      if (precheck.valid) {
        message.success("Setup complete. Precheck passed with no blockers.");
        // Auto-advance to next step
        setCurrentStep(2);
      } else {
        message.warning("Setup complete. Precheck found blockers to resolve.");
      }
    });
  }

  async function inspectTargetStage() {
    await runBusy("inspect-stage", async () => {
      requireConnections();
      const inspection = await inspectStage(targetConnection, namespace);
      setStageInspectData(inspection.properties);
      message.success(`Inspected ${inspection.stage_fqn}.`);
    });
  }

  async function listTargetStage() {
    await runBusy("list-stage", async () => {
      requireConnections();
      const listed = await listStage(targetConnection, namespace);
      setStageListRows(listed.rows);
      message.success(`Listed ${listed.count} stage entries.`);
    });
  }

  async function refreshHistory() {
    const response = await listMigrations(50);
    setHistory(response.jobs);
  }

  async function refreshJob(jobId: string) {
    const detail = await getMigration(jobId);
    setActiveJob(detail);
    setActiveJobId(jobId);
  }

  function connectJobStream(jobId: string, afterEventId = 0) {
    streamCloseRef.current?.();
    streamCloseRef.current = openMigrationStream(
      jobId,
      {
        onEvent: (event) => {
          setEvents((prev) => {
            if (prev.some((entry) => entry.event_id === event.event_id)) {
              return prev;
            }
            return [...prev, event].slice(-500);
          });
          if (event.event_type === "job.completed" || event.event_type === "job.failed") {
            void refreshJob(jobId);
            void refreshHistory();
          }
        },
        onEnded: () => {
          void refreshJob(jobId);
          void refreshHistory();
        },
        onError: (error) => {
          message.warning(error.message);
        },
      },
      afterEventId,
    );
  }

  async function launchMigration() {
    await runBusy("start-job", async () => {
      if (selectedPhases.length === 0) {
        throw new Error("Select at least one migration phase.");
      }
      const request: MigrationRunRequest = {
        source_connection: sourceConnection,
        target_connection: targetConnection,
        namespace,
        databases: {
          source_db: sourceDb.trim(),
          target_db: targetDb.trim(),
        },
        stage: {
          integration_name: stage.integration_name,
          azure_tenant_id: stage.azure_tenant_id,
          storage_account: stage.storage_account,
          container: stage.container,
          prefix: stage.prefix,
          stage_prefix: stage.stage_prefix,
        },
        schemas: schemaInput.length > 0 ? schemaInput : undefined,
        selected_phases: selectedPhases,
        run_id: runId.trim() || undefined,
        dry_run: dryRun,
      };

      const started = await startMigration(request);
      setEvents([]);
      await refreshJob(started.job_id);
      connectJobStream(started.job_id, 0);
      await refreshHistory();
      message.success(`Migration started (${started.run_id}).`);
      // Auto-advance to monitor step
      setCurrentStep(3);
    });
  }

  async function cancelMigrationJob() {
    await runBusy("cancel-job", async () => {
      if (!activeJobId) throw new Error("No active job selected.");
      await cancelMigration(activeJobId);
      await refreshJob(activeJobId);
      await refreshHistory();
      message.success("Cancel requested.");
    });
  }

  async function resumeMigrationJob() {
    await runBusy("resume-job", async () => {
      if (!activeJobId) throw new Error("No job selected.");
      const resumed = await resumeMigration(activeJobId);
      setEvents([]);
      await refreshJob(resumed.job_id);
      connectJobStream(resumed.job_id, 0);
      await refreshHistory();
      message.success("Resume requested as a new job.");
      setCurrentStep(3);
    });
  }

  return (
    <DashboardContext.Provider
      value={{
        currentStep,
        setCurrentStep,
        defaults,
        loadingBoot,
        busyKey,
        runBusy,
        sourceConnection,
        targetConnection,
        sourceValidated,
        targetValidated,
        sourceDatabases,
        targetDatabases,
        sourceSchemas,
        updateConnectionField,
        handleTestBoth,
        requireConnections,
        namespace,
        setNamespace,
        stage,
        setStage,
        sourceDb,
        setSourceDb: handleSourceDbChange,
        targetDb,
        setTargetDb,
        schemaInput,
        setSchemaInput,
        selectedPhases,
        setSelectedPhases,
        dryRun,
        setDryRun,
        runId,
        setRunId,
        precheckResult,
        schemaOrderResult,
        stageInspectData,
        stageListRows,
        handleOneClickSetup,
        inspectTargetStage,
        listTargetStage,
        history,
        activeJobId,
        activeJob,
        events,
        setEvents,
        refreshHistory,
        refreshJob,
        connectJobStream,
        launchMigration,
        cancelMigrationJob,
        resumeMigrationJob,
        phaseStatus,
      }}
    >
      {children}
    </DashboardContext.Provider>
  );
}

export function useDashboard() {
  const context = useContext(DashboardContext);
  if (context === undefined) {
    throw new Error("useDashboard must be used within a DashboardProvider");
  }
  return context;
}
