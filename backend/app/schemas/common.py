"""Shared API schema primitives."""

from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel


class ApiMessage(BaseModel):
    message: str


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str
    version: str
    timestamp: datetime
