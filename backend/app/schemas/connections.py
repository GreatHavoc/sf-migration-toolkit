"""Connection request and response schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SnowflakeConnectionPayload(BaseModel):
    account: str = Field(min_length=1)
    user: str = Field(min_length=1)
    password: str = Field(min_length=1)
    role: str | None = None
    warehouse: str | None = None
    passcode: str | None = None


class ConnectionTestRequest(BaseModel):
    connection: SnowflakeConnectionPayload


class ConnectionTestResponse(BaseModel):
    ok: bool
    account: str
    user: str
    role: str | None = None
    warehouse: str | None = None


class ListDatabasesRequest(BaseModel):
    connection: SnowflakeConnectionPayload


class ListDatabasesResponse(BaseModel):
    databases: list[str]


class ListSchemasRequest(BaseModel):
    connection: SnowflakeConnectionPayload
    database: str


class ListSchemasResponse(BaseModel):
    schemas: list[str]
