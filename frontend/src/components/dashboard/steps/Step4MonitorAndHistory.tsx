"use client";

import { Button, Card, Col, Descriptions, Divider, Empty, List, Row, Space, Tag, Timeline, Typography, Progress } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import { useDashboard } from "../DashboardContext";

const { Text } = Typography;

function statusColor(status: string): string {
  if (status === "succeeded") return "green";
  if (status === "failed") return "red";
  if (status === "running") return "blue";
  if (status === "cancel_requested") return "orange";
  if (status === "cancelled") return "volcano";
  return "default";
}

function phaseColor(status: "pending" | "done" | "failed"): string {
  if (status === "done") return "green";
  if (status === "failed") return "red";
  return "gold";
}

function timelineColor(eventType: string): "green" | "red" | "blue" | "gray" {
  if (eventType.includes("failed")) return "red";
  if (eventType.includes("completed")) return "green";
  if (eventType.includes("started") || eventType.includes("queued")) return "blue";
  return "gray";
}

export default function Step4MonitorAndHistory() {
  const {
    activeJob,
    activeJobId,
    events,
    selectedPhases,
    phaseStatus,
    history,
    busyKey,
    refreshHistory,
    connectJobStream,
    refreshJob,
    setEvents,
    setCurrentStep,
  } = useDashboard();

  const totalPhases = selectedPhases.length;
  const completedPhases = selectedPhases.filter(p => phaseStatus.get(p)?.status === "done").length;
  const failedPhases = selectedPhases.filter(p => phaseStatus.get(p)?.status === "failed").length;
  const inProgressPhase = selectedPhases.find(p => phaseStatus.get(p)?.status === "pending" && activeJob?.status === "running");

  const progressPercent = totalPhases > 0 ? Math.round((completedPhases / totalPhases) * 100) : 0;
  const statusColorMap: Record<string, "normal" | "exception" | "active" | "success"> = {
    succeeded: "success",
    failed: "exception",
    running: "active",
    cancelled: "exception",
  };
  const progressStatus = activeJob ? statusColorMap[activeJob.status] || "normal" : "normal";

  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      <Row gutter={[16, 16]}>
        <Col xs={24} xl={16}>
          <Card className="frost-card workflow-card" title={<div className="workflow-title">Live Run Monitor</div>} variant="borderless">
            {activeJob ? (
              <>
                <Descriptions bordered size="small" column={{ xs: 1, lg: 2 }}>
                  <Descriptions.Item label="Job ID">
                    <Text className="mono-text">{activeJob.job_id}</Text>
                  </Descriptions.Item>
                  <Descriptions.Item label="Run ID">
                    <Text className="mono-text">{activeJob.run_id}</Text>
                  </Descriptions.Item>
                  <Descriptions.Item label="Status">
                    <Tag color={statusColor(activeJob.status)}>{activeJob.status.toUpperCase()}</Tag>
                  </Descriptions.Item>
                  <Descriptions.Item label="Mode">
                    <Tag>{activeJob.dry_run ? "Dry Run" : "Live Run"}</Tag>
                  </Descriptions.Item>
                </Descriptions>

                <Divider />
                <Space orientation="vertical" style={{ width: "100%" }}>
                  <Text strong>Overall Progress</Text>
                  <Progress 
                    percent={progressPercent} 
                    status={progressStatus} 
                    strokeColor={{ '0%': '#108ee9', '100%': '#87d068' }}
                  />
                  {inProgressPhase && <Text type="secondary">Currently working on: <Text strong>{inProgressPhase}</Text></Text>}
                </Space>

                <Divider />
                <Text strong>Phase Progress</Text>
                <List
                  className="phase-list"
                  size="small"
                  dataSource={selectedPhases}
                  renderItem={(phase) => {
                    const status = phaseStatus.get(phase) || { status: "pending", count: 0 };
                    return (
                      <List.Item className="phase-line">
                        <Space className="phase-line-label">
                          <Text className="mono-text">{phase}</Text>
                        </Space>
                        <Space>
                          <Tag color={phaseColor(status.status)}>{status.status.toUpperCase()}</Tag>
                          <Text type="secondary">objects: {status.count}</Text>
                        </Space>
                      </List.Item>
                    );
                  }}
                />

                <Divider />
                {events.length > 0 ? (
                  <Timeline
                    items={events
                      .slice(-25)
                      .reverse()
                      .map((event) => ({
                        color: timelineColor(event.event_type),
                        children: (
                          <Space orientation="vertical" size={0}>
                            <Text strong>{event.event_type}</Text>
                            <Text>{event.message}</Text>
                            <Text type="secondary" className="mono-text">
                              {new Date(event.created_at).toLocaleString()}
                            </Text>
                          </Space>
                        ),
                      }))}
                  />
                ) : (
                  <Empty description="No events yet. Start or select a run." />
                )}
              </>
            ) : (
              <Empty description="No active job. Launch a run or select one from history." />
            )}
          </Card>
        </Col>

        <Col xs={24} xl={8}>
          <Card
            className="frost-card workflow-card"
            title={<div className="workflow-title">Recent Runs</div>}
            variant="borderless"
            extra={
              <Button
                icon={<ReloadOutlined />}
                loading={busyKey === "refresh-history"}
                onClick={refreshHistory}
              >
                Refresh
              </Button>
            }
          >
            {history.length === 0 ? (
              <Empty description="No runs recorded in local SQLite yet." />
            ) : (
              <List
                className="history-list"
                itemLayout="horizontal"
                dataSource={history}
                renderItem={(item) => (
                  <List.Item
                    actions={[
                      <Button
                        key={`open-${item.job_id}`}
                        type={activeJobId === item.job_id ? "primary" : "default"}
                        onClick={async () => {
                          setEvents([]);
                          await refreshJob(item.job_id);
                          connectJobStream(item.job_id, 0);
                        }}
                      >
                        Open
                      </Button>,
                    ]}
                  >
                    <List.Item.Meta
                      title={
                        <Space wrap>
                          <Text className="mono-text">{item.run_id}</Text>
                          <Tag color={statusColor(item.status)}>{item.status.toUpperCase()}</Tag>
                        </Space>
                      }
                      description={
                        <Space wrap>
                          <Text>{item.source_db}</Text>
                          <Text type="secondary">to</Text>
                          <Text>{item.target_db}</Text>
                          <Tag>{item.dry_run ? "Dry Run" : "Live Run"}</Tag>
                          <Text type="secondary">{new Date(item.created_at).toLocaleString()}</Text>
                        </Space>
                      }
                    />
                  </List.Item>
                )}
              />
            )}
          </Card>
        </Col>
      </Row>
      
      <Divider />
      <Row justify="space-between">
        <Button onClick={() => setCurrentStep(2)}>Back</Button>
      </Row>
    </Space>
  );
}
