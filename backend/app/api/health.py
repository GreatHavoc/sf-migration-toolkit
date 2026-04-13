"""Health and metadata endpoints."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from ..config import settings
from ..schemas.common import HealthResponse
from ..schemas.settings import DefaultSettingsResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get("", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        service=settings.app_name,
        version=settings.app_version,
        timestamp=datetime.now(timezone.utc),
    )


@router.get("/defaults", response_model=DefaultSettingsResponse)
def defaults() -> DefaultSettingsResponse:
    return DefaultSettingsResponse(
        mig_db=settings.default_mig_db,
        mig_schema=settings.default_mig_schema,
        stage_name=settings.default_stage,
        integration_name=settings.default_integration,
        nb_int_stage_name=settings.default_nb_int_stage,
        local_int_stage_name=settings.default_local_int_stage,
    )
