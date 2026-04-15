"""Snowflake connection/session service helpers for API handlers."""

from __future__ import annotations

import hashlib
import threading
from contextlib import contextmanager

from connection import connect_sf, exec_sql

# Global connection cache
# Key: SHA256 of (account, user, password, role, warehouse)
# Value: dict with 'conn': connection object, 'lock': threading.Lock, 'last_used': timestamp
import time

_CONN_CACHE: dict[str, dict] = {}
_CONN_LOCK = threading.Lock()
MAX_CACHE_SIZE = 20


def _get_cache_key(payload) -> str:
    key_str = f"{payload.account}:{payload.user}:{payload.password}:{payload.role or ''}:{payload.warehouse or ''}"
    return hashlib.sha256(key_str.encode("utf-8")).hexdigest()


def _evict_oldest():
    if len(_CONN_CACHE) > MAX_CACHE_SIZE:
        oldest_key = min(_CONN_CACHE.keys(), key=lambda k: _CONN_CACHE[k]["last_used"])
        entry = _CONN_CACHE.pop(oldest_key, None)
        if entry and entry["conn"]:
            try:
                entry["conn"].close()
            except Exception:
                pass


@contextmanager
def snowflake_connection(payload):
    key = _get_cache_key(payload)
    entry = None

    with _CONN_LOCK:
        entry = _CONN_CACHE.get(key)
        if entry is not None:
            entry["last_used"] = time.time()

    if entry is not None:
        # Acquire the per-connection lock to prevent concurrent queries on the same connection
        with entry["lock"]:
            try:
                exec_sql(entry["conn"], "SELECT 1")
            except Exception:
                try:
                    entry["conn"].close()
                except Exception:
                    pass
                with _CONN_LOCK:
                    _CONN_CACHE.pop(key, None)
                entry = None

    if entry is None:
        conn = connect_sf(
            payload.account,
            payload.user,
            payload.password,
            role=payload.role,
            warehouse=payload.warehouse,
            passcode=payload.passcode,
        )
        entry = {"conn": conn, "lock": threading.RLock(), "last_used": time.time()}
        with _CONN_LOCK:
            _CONN_CACHE[key] = entry
            _evict_oldest()

    # Hold the connection lock while yielding so no other thread can execute queries concurrently
    with entry["lock"]:
        try:
            yield entry["conn"]
        finally:
            # Do not close the connection so it can be reused across API calls.
            # This keeps the MFA session alive.
            pass
