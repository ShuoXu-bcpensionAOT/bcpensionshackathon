"""Direct config-DB access (pyodbc + AAD token) — used by the planner and workers to read
authored config and (for discovery) register source_object rows."""
from .runtime import WS_ID, CONFIG_DB_NAME, notebookutils, _fabric_api_token


def _config_props():
    import requests
    tk = _fabric_api_token()
    h = {"Authorization": f"Bearer {tk}"}
    for d in requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/SqlDatabases",
                          headers=h).json().get("value", []):
        if d["displayName"] == CONFIG_DB_NAME:
            p = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/"
                             f"SqlDatabases/{d['id']}", headers=h).json()["properties"]
            return p["serverFqdn"].split(",")[0], p["databaseName"]
    raise Exception(f"{CONFIG_DB_NAME} not found")


def config_conn():
    import pyodbc
    import struct
    host, database = _config_props()
    tok = notebookutils.credentials.getToken("https://database.windows.net/").encode("utf-16-le")
    ts = struct.pack(f"<I{len(tok)}s", len(tok), tok)
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host};DATABASE={database};Encrypt=yes",
        attrs_before={1256: ts})


def config_query(sql, params=()):
    cn = config_conn()
    cur = cn.cursor()
    cur.execute(sql, *params)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cn.close()
    return rows


def config_exec(sql, params=()):
    """Execute a write against config_db (used by object discovery to register source_object)."""
    cn = config_conn()
    cur = cn.cursor()
    cur.execute(sql, *params)
    cn.commit()
    cn.close()


def config_exec_many(sql, rows):
    """Batch-insert into config_db on a single connection (one connect per datasource, not per row)."""
    if not rows:
        return
    cn = config_conn()
    cur = cn.cursor()
    cur.fast_executemany = True
    cur.executemany(sql, rows)
    cn.commit()
    cn.close()
