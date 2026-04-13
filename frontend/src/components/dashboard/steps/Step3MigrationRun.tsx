"use client";

import { Alert, Button, Card, Col, Divider, Row, Select, Space, Switch, Typography } from "antd";
import { PlayCircleOutlined, StopOutlined, ReloadOutlined } from "@ant-design/icons";
import { useDashboard } from "../DashboardContext";
import { LabeledInput } from "./Step1Connections";
import { MIGRATION_PHASES } from "@/types/api";

const { Text } = Typography;

export default function Step3MigrationRun() {
  const {
    sourceDb,
    setSourceDb,
    targetDb,
    setTargetDb,
    runId,
    setRunId,
    selectedPhases,
    setSelectedPhases,
    dryRun,
    setDryRun,
    busyKey,
    sourceValidated,
    targetValidated,
    activeJobId,
    launchMigration,
    cancelMigrationJob,
    resumeMigrationJob,
    setCurrentStep,
  } = useDashboard();

  const canRun =
    sourceValidated &&
    targetValidated &&
    sourceDb.trim().length > 0 &&
    targetDb.trim().length > 0;

  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      <Card className="frost-card workflow-card" title={<div className="workflow-title">Launch Migration</div>} variant="borderless">
        <Row gutter={[12, 12]}>
          <Col xs={24} md={8}>
            <LabeledInput label="Source DB" value={sourceDb} onChange={setSourceDb} placeholder="SOURCE_DB" />
          </Col>
          <Col xs={24} md={8}>
            <LabeledInput label="Target DB" value={targetDb} onChange={setTargetDb} placeholder="TARGET_DB" />
          </Col>
          <Col xs={24} md={8}>
            <LabeledInput label="Run ID" value={runId} onChange={setRunId} placeholder="Optional custom ID" />
          </Col>
          <Col xs={24}>
            <div className="field-label">Selected Phases</div>
            <Select
              mode="multiple"
              value={selectedPhases}
              style={{ width: "100%" }}
              onChange={setSelectedPhases}
              options={MIGRATION_PHASES.map((p) => ({ label: p, value: p }))}
              placeholder="Select phases"
              maxTagCount="responsive"
            />
          </Col>
        </Row>
        
        <Divider />
        <Space wrap>
          <Space>
            <Switch checked={dryRun} onChange={setDryRun} />
            <Text>Dry run mode</Text>
          </Space>

          <Button
            type="primary"
            icon={<PlayCircleOutlined />}
            loading={busyKey === "start-job"}
            disabled={!canRun}
            onClick={launchMigration}
          >
            Start Migration
          </Button>

          <Button
            icon={<StopOutlined />}
            loading={busyKey === "cancel-job"}
            disabled={!activeJobId}
            onClick={cancelMigrationJob}
          >
            Cancel Active Job
          </Button>

          <Button
            icon={<ReloadOutlined />}
            loading={busyKey === "resume-job"}
            disabled={!activeJobId}
            onClick={resumeMigrationJob}
          >
            Resume Failed Job
          </Button>
        </Space>

        {!canRun && (
          <Alert
            style={{ marginTop: 14 }}
            showIcon
            type="info"
            title="Setup required"
            description="Ensure connections are validated and DB names are provided before launching."
          />
        )}
      </Card>
      
      <Divider />
      <Row justify="space-between">
        <Button onClick={() => setCurrentStep(1)}>Back</Button>
        <Button
          type="primary"
          onClick={() => setCurrentStep(3)}
        >
          Next: Monitor & History
        </Button>
      </Row>
    </Space>
  );
}
