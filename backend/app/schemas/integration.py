"""Azure integration and stage setup schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator
from .connections import SnowflakeConnectionPayload


class AzureStageConfig(BaseModel):
    storage_account: str = Field(min_length=1)
    container: str = Field(min_length=1)
    prefix: str = "exports/"

    @field_validator("storage_account")
    @classmethod
    def validate_storage_account(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("storage_account cannot be empty")
        return value.strip()

    @field_validator("container")
    @classmethod
    def validate_container(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("container cannot be empty")
        return value.strip()


class NamespaceConfig(BaseModel):
    mig_db: str = Field(min_length=1)
    mig_schema: str = Field(min_length=1)
    stage_name: str = Field(min_length=1)


class EnsureIntegrationRequest(BaseModel):
    connection: "SnowflakeConnectionPayload"
    integration_name: str = Field(min_length=1)
    azure_tenant_id: str = Field(min_length=1)
    stage: AzureStageConfig


class EnsureIntegrationResponse(BaseModel):
    ok: bool
    integration_name: str
    stage_url: str
    message: str


class EnsureStageRequest(BaseModel):
    connection: "SnowflakeConnectionPayload"
    namespace: NamespaceConfig
    integration_name: str = Field(min_length=1)
    stage: AzureStageConfig


class EnsureStageResponse(BaseModel):
    ok: bool
    stage_fqn: str
    stage_url: str
    integration_name: str
    message: str


class InspectStageRequest(BaseModel):
    connection: "SnowflakeConnectionPayload"
    namespace: NamespaceConfig


class InspectStageResponse(BaseModel):
    stage_fqn: str
    properties: dict[str, str]


class ListStageRequest(BaseModel):
    connection: "SnowflakeConnectionPayload"
    namespace: NamespaceConfig


class ListStageResponse(BaseModel):
    stage_fqn: str
    rows: list[list[str]]
    count: int
