"use client";

import { Alert, Button, Card, Col, Descriptions, Divider, Input, Row, Select, Space, Typography } from "antd";
import { ExperimentOutlined, ApiOutlined, BranchesOutlined } from "@ant-design/icons";
import { useDashboard } from "../DashboardContext";
import { LabeledInput } from "./Step1Connections";

const { Text } = Typography;

export function LabeledSelect<T extends string | string[]>({
  label,
  value,
  onChange,
  options,
  placeholder,
  mode,
}: {
  label: string;
  value: T;
  onChange: (val: T) => void;
  options: string[];
  placeholder?: string;
  mode?: "multiple" | "tags";
}) {
  return (
    <Space orientation="vertical" size={2} style={{ width: "100%" }}>
      <div className="field-label">{label}</div>
      <Select
        mode={mode}
        showSearch
        style={{ width: "100%" }}
        value={value || undefined}
        onChange={onChange}
        placeholder={placeholder}
        options={options.map((opt) => ({ label: opt, value: opt }))}
        maxTagCount="responsive"
      />
    </Space>
  );
}

export default function Step2SetupAndAnalysis() {
  const {
    namespace,
    setNamespace,
    stage,
    setStage,
    sourceDb,
    setSourceDb,
    targetDb,
    setTargetDb,
    schemaInput,
    setSchemaInput,
    precheckResult,
    schemaOrderResult,
    stageInspectData,
    stageListRows,
    handleOneClickSetup,
    inspectTargetStage,
    listTargetStage,
    busyKey,
    sourceValidated,
    targetValidated,
    sourceDatabases,
    targetDatabases,
    sourceSchemas,
    setCurrentStep,
  } = useDashboard();

  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      <Card className="frost-card workflow-card" title={<div className="workflow-title">Namespace & Infrastructure</div>} variant="borderless">
        <Row gutter={[12, 12]}>
          <Col xs={24} md={8}>
            <LabeledInput label="Migration DB" value={namespace.mig_db} onChange={(v) => setNamespace((prev) => ({ ...prev, mig_db: v }))} placeholder="MIGRATION_DB" />
          </Col>
          <Col xs={24} md={8}>
            <LabeledInput label="Migration Schema" value={namespace.mig_schema} onChange={(v) => setNamespace((prev) => ({ ...prev, mig_schema: v }))} placeholder="PUBLIC" />
          </Col>
          <Col xs={24} md={8}>
            <LabeledInput label="Stage Name" value={namespace.stage_name} onChange={(v) => setNamespace((prev) => ({ ...prev, stage_name: v }))} placeholder="MIGRATION_STAGE" />
          </Col>

          <Col xs={24} md={12}>
            <LabeledInput label="Azure Integration" value={stage.integration_name} onChange={(v) => setStage((prev) => ({ ...prev, integration_name: v }))} placeholder="AZURE_MIGRATION_INT" />
          </Col>
          <Col xs={24} md={12}>
            <LabeledInput label="Azure Tenant ID" value={stage.azure_tenant_id} onChange={(v) => setStage((prev) => ({ ...prev, azure_tenant_id: v }))} placeholder="e.g. 0000-0000-0000" />
          </Col>
          <Col xs={24} md={8}>
            <LabeledInput label="Storage Account" value={stage.storage_account} onChange={(v) => setStage((prev) => ({ ...prev, storage_account: v }))} placeholder="mystorage" />
          </Col>
          <Col xs={24} md={8}>
            <LabeledInput label="Container" value={stage.container} onChange={(v) => setStage((prev) => ({ ...prev, container: v }))} placeholder="exports" />
          </Col>
          <Col xs={24} md={8}>
            <LabeledInput label="Prefix" value={stage.prefix} onChange={(v) => setStage((prev) => ({ ...prev, prefix: v }))} placeholder="exports/" />
          </Col>
          <Col xs={24} md={8}>
            <LabeledInput label="Stage Prefix" value={stage.stage_prefix} onChange={(v) => setStage((prev) => ({ ...prev, stage_prefix: v }))} placeholder="sf_migration" />
          </Col>
        </Row>
      </Card>

      <Card className="frost-card workflow-card" title={<div className="workflow-title">Target Databases & Analysis</div>} variant="borderless">
        <Row gutter={[12, 12]}>
          <Col xs={24} md={12}>
            <LabeledSelect label="Source DB" value={sourceDb} onChange={setSourceDb} options={sourceDatabases} placeholder="Select Source DB" />
          </Col>
          <Col xs={24} md={12}>
            <LabeledInput label="Target DB Name" value={targetDb} onChange={setTargetDb} placeholder="Defaults to Source DB name" />
          </Col>
          <Col xs={24}>
            <LabeledSelect 
              mode="multiple" 
              label="Schemas in Scope" 
              value={schemaInput} 
              onChange={setSchemaInput} 
              options={sourceSchemas} 
              placeholder="Select schemas (leave empty to migrate all)" 
            />
          </Col>
        </Row>
        
        <Divider />
        <Space wrap>
          <Button
            type="primary"
            icon={<ExperimentOutlined />}
            loading={busyKey === "setup-and-precheck"}
            disabled={!sourceValidated || !targetValidated}
            onClick={handleOneClickSetup}
          >
            One-Click Setup & Precheck
          </Button>

          <Button
            icon={<ApiOutlined />}
            loading={busyKey === "inspect-stage"}
            onClick={inspectTargetStage}
          >
            Inspect Target Stage
          </Button>

          <Button
            icon={<BranchesOutlined />}
            loading={busyKey === "list-stage"}
            onClick={listTargetStage}
          >
            List Target Stage
          </Button>
        </Space>
      </Card>

      {(stageInspectData || stageListRows.length > 0) && (
        <Card className="frost-card workflow-card" title={<div className="workflow-title">Stage Information</div>} variant="borderless">
          {stageInspectData && Object.keys(stageInspectData).length > 0 && (
            <Descriptions bordered size="small" column={1}>
              {Object.entries(stageInspectData).map(([key, value]) => (
                <Descriptions.Item key={key} label={key}>
                  <Text className="mono-text">{value}</Text>
                </Descriptions.Item>
              ))}
            </Descriptions>
          )}
          {stageListRows.length > 0 && (
            <>
              {stageInspectData && <Divider />}
              <Space orientation="vertical" style={{ width: '100%' }}>
                {stageListRows.slice(0, 25).map((row, idx) => (
                  <div key={idx} style={{ padding: '8px 0', borderBottom: '1px solid #f0f0f0' }}>
                    <Text className="mono-text">{`${idx + 1}. ${row.join(" | ")}`}</Text>
                  </div>
                ))}
              </Space>
            </>
          )}
        </Card>
      )}

      {precheckResult && (
        <Card className="frost-card workflow-card" title={<div className="workflow-title">Precheck Result</div>} variant="borderless">
          <Alert
            type={precheckResult.valid ? "success" : "error"}
            title={precheckResult.valid ? "Validation passed" : "Validation failed"}
            description={
              precheckResult.valid
                ? "No blocking cross-database dependencies were reported. Ready to run."
                : "Resolve blocking dependency errors before running migration."
            }
            showIcon
          />
          <Divider />
          <Row gutter={[16, 16]}>
            {precheckResult.inventory_summary && (
              <Col xs={24} md={8}>
                <Space orientation="vertical" style={{ width: "100%" }}>
                  <Text strong>Pre-Flight Estimation</Text>
                  <Descriptions bordered size="small" column={1}>
                    {Object.entries(precheckResult.inventory_summary).map(([key, count]) => (
                      <Descriptions.Item key={key} label={key.charAt(0).toUpperCase() + key.slice(1)}>
                        {count}
                      </Descriptions.Item>
                    ))}
                  </Descriptions>
                </Space>
              </Col>
            )}
            <Col xs={24} md={precheckResult.inventory_summary ? 16 : 24}>
              <Space orientation="vertical" style={{ width: "100%" }}>
                <Text strong>Schemas in scope</Text>
                <Text className="mono-text">{precheckResult.schemas.join(", ") || "(none)"}</Text>
                {schemaOrderResult && (
                  <>
                    <Text strong>Computed schema order</Text>
                    <Text className="mono-text">
                      {schemaOrderResult.ordered_schemas.join(" -> ") || "(none)"}
                    </Text>
                  </>
                )}
                {precheckResult.errors.length > 0 && (
                  <>
                    <Text strong>Errors</Text>
                    {precheckResult.errors.map((err, i) => (
                      <Alert key={i} type="error" title={err} showIcon />
                    ))}
                  </>
                )}
                {precheckResult.warnings.length > 0 && (
                  <>
                    <Text strong>Warnings</Text>
                    {precheckResult.warnings.map((wrn, i) => (
                      <Alert key={i} type="warning" title={wrn} showIcon />
                    ))}
                  </>
                )}
              </Space>
            </Col>
          </Row>
        </Card>
      )}

      <Divider />
      <Row justify="space-between">
        <Button onClick={() => setCurrentStep(0)}>Back</Button>
        <Button
          type="primary"
          disabled={!precheckResult?.valid}
          onClick={() => setCurrentStep(2)}
        >
          Next: Migration Run
        </Button>
      </Row>
    </Space>
  );
}
