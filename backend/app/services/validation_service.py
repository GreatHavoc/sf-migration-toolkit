"""Input validation helpers shared by API routes."""

from __future__ import annotations

import re


STORAGE_ACCOUNT_RE = re.compile(r"^[a-z0-9]{3,24}$")
CONTAINER_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$")
GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)


def validate_azure_tenant_id(tenant_id: str) -> str:
    tenant = (tenant_id or "").strip()
    if not tenant:
        raise ValueError("Azure tenant id is required.")
    if not GUID_RE.fullmatch(tenant):
        raise ValueError("Azure tenant id must be a GUID.")
    return tenant


def validate_storage_account_name(name: str) -> str:
    value = (name or "").strip().lower()
    if not STORAGE_ACCOUNT_RE.fullmatch(value):
        raise ValueError(
            "Azure storage account must be 3-24 chars of lowercase letters and digits."
        )
    return value


def validate_container_name(name: str) -> str:
    value = (name or "").strip().lower()
    if not CONTAINER_RE.fullmatch(value):
        raise ValueError(
            "Azure container must be 3-63 chars, lowercase letters/digits/hyphens."
        )
    if "--" in value:
        raise ValueError("Azure container cannot contain consecutive hyphens.")
    return value


def normalize_stage_prefix(prefix: str) -> str:
    value = (prefix or "").strip()
    if not value:
        return ""
    value = value.lstrip("/")
    if value and not value.endswith("/"):
        value += "/"
    return value
