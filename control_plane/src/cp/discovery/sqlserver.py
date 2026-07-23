"""SQL Server object discovery — enumerate base tables + primary-key columns."""
import json

from ..runtime import SOURCE_SERVER
from ..connectors.base import JDBC_DIALECTS, _resolve_conn, _jdbc_driver, _jdbc_load
from . import discoverer


@discoverer("sqlserver")
def _discover_sqlserver(ds):
    """Enumerate every base table in the source database + its primary-key columns."""
    c = _resolve_conn(ds)
    d = JDBC_DIALECTS["sqlserver"]
    url = c.get("url") or c.get("connection_string") or d["url"].format(
        host=c.get("host") or SOURCE_SERVER, port=c.get("port") or d["port"],
        database=c.get("database") or ds.get("database_name"))
    drv, u, p = _jdbc_driver(c, d), c.get("user"), c.get("password")
    tables = _jdbc_load(url, drv, u, p, query=(
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_TYPE='BASE TABLE'")).collect()
    # NB: Spark wraps a `query` as a derived table, and SQL Server forbids ORDER BY there —
    # so select ORDINAL_POSITION and sort in Python instead.
    pk = _jdbc_load(url, drv, u, p, query=(
        "SELECT KU.TABLE_SCHEMA t_s, KU.TABLE_NAME t_n, KU.COLUMN_NAME c_n, KU.ORDINAL_POSITION ord "
        "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC "
        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU "
        "ON TC.CONSTRAINT_NAME=KU.CONSTRAINT_NAME AND TC.CONSTRAINT_SCHEMA=KU.CONSTRAINT_SCHEMA "
        "WHERE TC.CONSTRAINT_TYPE='PRIMARY KEY'")).collect()
    keys = {}
    for r in sorted(pk, key=lambda x: x["ord"]):
        keys.setdefault((r["t_s"], r["t_n"]), []).append(r["c_n"])
    return [{"source_schema": t["TABLE_SCHEMA"], "source_table": t["TABLE_NAME"],
             "key_columns_json": json.dumps(keys.get((t["TABLE_SCHEMA"], t["TABLE_NAME"]), [])),
             "source_options_json": None} for t in tables]
