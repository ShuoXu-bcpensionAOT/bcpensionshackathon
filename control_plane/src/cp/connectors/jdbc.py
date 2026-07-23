"""JDBC connector for any dialect (SQL Server / Postgres / MySQL bundled; generic jdbc)."""
from ..runtime import SOURCE_SERVER
from ..audit import get_watermark
from . import ingest_connector
from .base import JDBC_DIALECTS, COMPLEX_TYPES, _resolve_conn, _opts, _jdbc_driver, _jdbc_load


@ingest_connector("sqlserver", "postgresql", "mysql", "jdbc")
def _ic_jdbc(o, user, password):
    """JDBC connector for any dialect. Server/driver from connection_json (or a dialect
    preset); falls back to the cp_vars source_server for SQL Server (back-compat)."""
    name = (o.get("connector") or o.get("source_type") or "sqlserver").lower()
    if name in ("sql", "mssql", "custom_jdbc"):
        name = "sqlserver"
    d = JDBC_DIALECTS.get(name)
    c, opts = _resolve_conn(o), _opts(o)
    user = c.get("user") or user
    password = c.get("password") or password
    driver = _jdbc_driver(c, d)
    if c.get("url") or c.get("connection_string"):
        url = c.get("url") or c.get("connection_string")
    elif d:
        url = d["url"].format(host=c.get("host") or SOURCE_SERVER,
                              port=c.get("port") or d["port"],
                              database=c.get("database") or o.get("database_name"))
    else:
        raise Exception(f"jdbc connector '{name}': set connection_json.url + .driver")
    if not driver:
        raise Exception(f"jdbc connector '{name}': no driver class")
    schema, table, wm_col = o.get("source_schema"), o.get("source_table"), o.get("watermark_column")
    wm = get_watermark(o["object_id"]) if (o.get("load_type") == "incremental" and wm_col) else None
    if opts.get("query"):
        query = opts["query"]
    elif name == "sqlserver":                          # prune JDBC-unreadable complex-type columns
        cols = _jdbc_load(url, driver, user, password, query=(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'")).collect()
        keep = [r["COLUMN_NAME"] for r in cols if r["DATA_TYPE"].lower() not in COMPLEX_TYPES]
        col_sql = ", ".join(f"[{x}]" for x in keep)
        pred = f" WHERE [{wm_col}] > '{wm}'" if wm else ""
        query = f"SELECT {col_sql} FROM [{schema}].[{table}]{pred}"
    else:
        tbl = f"{schema}.{table}" if schema else table
        pred = f" WHERE {wm_col} > '{wm}'" if wm else ""
        query = f"SELECT * FROM {tbl}{pred}"
    return _jdbc_load(url, driver, user, password, query=query)
