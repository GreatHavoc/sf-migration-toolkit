"""Snowflake connection/session service helpers for API handlers."""

from __future__ import annotations

import hashlib
import threading
from contextlib import contextmanager

from connection import connect_sf, exec_sql

# Global connection cache
# Key: SHA256 of (account, user, password, role, warehouse)
# Value: connection object
_CONN_CACHE: dict[str, object] = {}
_CONN_LOCK = threading.Lock()


def _get_cache_key(payload) -> str:
    key_str = f"{payload.account}:{payload.user}:{payload.password}:{payload.role or ''}:{payload.warehouse or ''}"
    return hashlib.sha256(key_str.encode("utf-8")).hexdigest()


@contextmanager
def snowflake_connection(payload):
    key = _get_cache_key(payload)
    conn = None

    with _CONN_LOCK:
        conn = _CONN_CACHE.get(key)

    if conn is not None:
        # Verify if connection is still alive outside the global lock
        try:
            exec_sql(conn, "SELECT 1")
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            with _CONN_LOCK:
                _CONN_CACHE.pop(key, None)

    if conn is None:
        conn = connect_sf(
            payload.account,
            payload.user,
            payload.password,
            role=payload.role,
            warehouse=payload.warehouse,
            passcode=payload.passcode,
        )
        with _CONN_LOCK:
            _CONN_CACHE[key] = conn

    try:
        # Yield the connection to the caller
        yield conn
    finally:
        # Do not close the connection so it can be reused across API calls.
        # This keeps the MFA session alive.
        pass
