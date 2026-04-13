"use client";

import { Card, Layout, Space, Steps, Tag, Typography } from "antd";
import {
  FolderOpenOutlined,
} from "@ant-design/icons";

import { DashboardProvider, useDashboard } from "./DashboardContext";
import Step1Connections from "./steps/Step1Connections";
import Step2SetupAndAnalysis from "./steps/Step2SetupAndAnalysis";
import Step3MigrationRun from "./steps/Step3MigrationRun";
import Step4MonitorAndHistory from "./steps/Step4MonitorAndHistory";

const { Title, Paragraph, Text } = Typography;

function WizardShell() {
  const { currentStep, defaults, loadingBoot } = useDashboard();

  const stepItems = [
    { title: "Connect" },
    { title: "Setup & Analysis" },
    { title: "Run Config" },
    { title: "Monitor" },
  ];

  return (
    <div className="workbench-root">
      <div className="ambient-orb ambient-orb-left" />
      <div className="ambient-orb ambient-orb-right" />
      <Layout className="dashboard-shell">
        <Card className="hero-panel" variant="borderless" loading={loadingBoot}>
          <Space orientation="vertical" size={2}>
            <Text className="hero-kicker">Standalone Orchestration Console</Text>
            <Title level={2} className="hero-title">
              Snowflake Migration Control Center
            </Title>
            <Paragraph className="hero-subtitle">
              Manage and orchestrate data migration pipelines across Snowflake environments.
            </Paragraph>
            <Space wrap className="hero-meta">
              {defaults && (
                <Tag icon={<FolderOpenOutlined />} color="blue">
                  Stage: {defaults.stage_name}
                </Tag>
              )}
            </Space>
          </Space>
        </Card>

        <Card className="section-tabs" variant="borderless" style={{ marginBottom: 24 }}>
          <Steps current={currentStep} items={stepItems} />
        </Card>

        {currentStep === 0 && <Step1Connections />}
        {currentStep === 1 && <Step2SetupAndAnalysis />}
        {currentStep === 2 && <Step3MigrationRun />}
        {currentStep === 3 && <Step4MonitorAndHistory />}
      </Layout>
    </div>
  );
}

export default function DashboardClient() {
  return (
    <DashboardProvider>
      <WizardShell />
    </DashboardProvider>
  );
}
