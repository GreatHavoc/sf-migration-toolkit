"use client";

import React, { useState } from "react";
import { Button, Card, Col, Divider, Input, Row, Space, Tag, Alert } from "antd";
import { ApiOutlined } from "@ant-design/icons";
import { useDashboard } from "../DashboardContext";
import { SnowflakeConnectionPayload } from "@/types/api";

type ConnectionCardProps = {
  title: string;
  value: SnowflakeConnectionPayload;
  validated: boolean;
  busy: boolean;
  onChange: (field: keyof SnowflakeConnectionPayload, value: string) => void;
};

export function LabeledInput({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  required = false,
  status,
}: {
  label: string;
  value: string;
  onChange: (val: string) => void;
  placeholder?: string;
  type?: "text" | "password";
  required?: boolean;
  status?: "error" | "warning" | "";
}) {
  return (
    <Space orientation="vertical" size={2} style={{ width: "100%" }}>
      <div className="field-label">
        {label} {required && <span style={{ color: "#ff4d4f" }}>*</span>}
      </div>
      {type === "password" ? (
        <Input.Password
          status={status}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
        />
      ) : (
        <Input
          status={status}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
        />
      )}
    </Space>
  );
}

function ConnectionCard({
  title,
  value,
  validated,
  onChange,
  showErrors,
}: ConnectionCardProps & { showErrors: boolean }) {
  const getStatus = (field: string) => {
    if (!showErrors) return "";
    return field.trim() === "" ? "error" : "";
  };

  return (
    <Card
      className="frost-card workflow-card"
      title={<div className="workflow-title">{title}</div>}
      variant="borderless"
      extra={
        <Tag color={validated ? "green" : "gold"}>
          {validated ? "Validated" : "Needs Test"}
        </Tag>
      }
    >
      <Space orientation="vertical" size={12} style={{ width: "100%" }}>
        <LabeledInput
          label="Account"
          required
          status={getStatus(value.account)}
          value={value.account}
          onChange={(v) => onChange("account", v)}
          placeholder="xy12345.ap-south-1"
        />
        <LabeledInput
          label="User"
          required
          status={getStatus(value.user)}
          value={value.user}
          onChange={(v) => onChange("user", v)}
          placeholder="SNOWFLAKE_USER"
        />
        <LabeledInput
          label="Password"
          type="password"
          required
          status={getStatus(value.password)}
          value={value.password}
          onChange={(v) => onChange("password", v)}
          placeholder="Account password"
        />
        <LabeledInput
          label="Role"
          value={value.role || ""}
          onChange={(v) => onChange("role", v)}
          placeholder="ACCOUNTADMIN"
        />
        <LabeledInput
          label="Warehouse"
          value={value.warehouse || ""}
          onChange={(v) => onChange("warehouse", v)}
          placeholder="COMPUTE_WH"
        />
        <LabeledInput
          label="MFA Passcode"
          type="password"
          value={value.passcode || ""}
          onChange={(v) => onChange("passcode", v)}
          placeholder="Optional token"
        />
      </Space>
    </Card>
  );
}

export default function Step1Connections() {
  const {
    sourceConnection,
    targetConnection,
    sourceValidated,
    targetValidated,
    busyKey,
    updateConnectionField,
    handleTestBoth,
    setCurrentStep,
  } = useDashboard();

  const [localShowErrors, setLocalShowErrors] = useState(false);

  const busy = busyKey === "test-both";
  
  const isSourceValid = !!(sourceConnection.account && sourceConnection.user && sourceConnection.password);
  const isTargetValid = !!(targetConnection.account && targetConnection.user && targetConnection.password);
  const canTest = isSourceValid && isTargetValid;

  const onTestClick = () => {
    if (!canTest) {
      setLocalShowErrors(true);
      return;
    }
    setLocalShowErrors(false);
    handleTestBoth();
  };

  return (
    <Space orientation="vertical" size={16} style={{ width: "100%" }}>
      {localShowErrors && (
        <Alert
          type="error"
          title="Validation Error"
          description="Please fill in all required fields (Account, User, Password) for both connections."
          showIcon
        />
      )}
      <Row justify="end">
        <Button
          type="primary"
          icon={<ApiOutlined />}
          loading={busy}
          onClick={onTestClick}
        >
          Test Both Connections
        </Button>
      </Row>
      <Row gutter={[16, 16]}>
        <Col xs={24} xl={12}>
          <ConnectionCard
            title="Source Snowflake"
            value={sourceConnection}
            validated={sourceValidated}
            busy={busy}
            showErrors={localShowErrors}
            onChange={(field, value) => updateConnectionField("source", field, value)}
          />
        </Col>
        <Col xs={24} xl={12}>
          <ConnectionCard
            title="Target Snowflake"
            value={targetConnection}
            validated={targetValidated}
            busy={busy}
            showErrors={localShowErrors}
            onChange={(field, value) => updateConnectionField("target", field, value)}
          />
        </Col>
      </Row>
      
      <Divider />
      <Row justify="end">
        <Button
          type="primary"
          disabled={!sourceValidated || !targetValidated}
          onClick={() => setCurrentStep(1)}
        >
          Next: Setup & Analysis
        </Button>
      </Row>
    </Space>
  );
}
