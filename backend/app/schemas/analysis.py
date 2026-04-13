"""Pre-migration analysis schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field
from .connections import SnowflakeConnectionPayload


class AnalysisRequest(BaseModel):
    connection: "SnowflakeConnectionPayload"
    source_db: str = Field(min_length=1)
    schemas: list[str] | None = None


class CrossDbValidationIssue(BaseModel):
    message: str


class PrecheckResponse(BaseModel):
    source_db: str
    schemas: list[str]
    valid: bool
    errors: list[str]
    warnings: list[str]
    inventory_summary: dict[str, int] | None = None


class SchemaOrderResponse(BaseModel):
    source_db: str
    schemas: list[str]
    ordered_schemas: list[str]
