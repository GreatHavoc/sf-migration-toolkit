"""Settings-related API schemas."""

from __future__ import annotations

from pydantic import BaseModel


class DefaultSettingsResponse(BaseModel):
    mig_db: str
    mig_schema: str
    stage_name: str
    integration_name: str
    nb_int_stage_name: str
    local_int_stage_name: str
