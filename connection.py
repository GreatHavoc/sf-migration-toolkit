import snowflake.connector
from io import StringIO


def connect_sf(account, user, password, role=None, warehouse=None, passcode=None):
    kwargs = dict(
        account=account,
        user=user,
        password=password,
        client_session_keep_alive=True,
    )
    if role:
        kwargs["role"] = role
    if warehouse:
        kwargs["warehouse"] = warehouse
    if passcode:
        kwargs["passcode"] = passcode
    return snowflake.connector.connect(**kwargs)


def exec_sql(conn, sql, params=None):
    with conn.cursor() as cur:
        try:
            cur.execute(sql, params or {})
            try:
                return cur.fetchall()
            except Exception:
                return []
        except Exception as e:
            raise RuntimeError(f"SQL failed: {sql}\nError: {e}") from e


def exec_sql_with_cols(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d[0] for d in (cur.description or [])]
        try:
            rows = cur.fetchall()
        except Exception:
            rows = []
        return cols, rows


def fetch_one_val(conn, sql, params=None):
    rows = exec_sql(conn, sql, params)
    return rows[0][0] if rows else None


def exec_script(conn, sql_text, remove_comments=True):
    sql_stream = StringIO(sql_text)
    for _cur in conn.execute_stream(sql_stream, remove_comments=remove_comments):
        pass
