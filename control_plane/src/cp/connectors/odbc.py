"""Generic ODBC connector (driver-side pyodbc)."""
from ..runtime import spark
from . import ingest_connector
from .base import _resolve_conn, _opts


@ingest_connector("odbc")
def _ic_odbc(o, user, password):
    """Generic ODBC via pyodbc on the driver node (not distributed — modest volumes).
    connection_json.odbc = an ODBC connection string ({user}/{password} placeholders
    substituted at runtime). Needs the platform ODBC driver/DSN in the Fabric Environment."""
    import pyodbc
    c, opts = _resolve_conn(o), _opts(o)
    user = c.get("user") or user
    password = c.get("password") or password
    cs = c.get("odbc") or c.get("connection_string")
    if not cs:
        raise Exception("odbc connector needs a connection string (secret or connection_json.odbc)")
    cs = cs.format(user=user, password=password,
                   **{k: v for k, v in c.items() if k not in ("odbc", "user", "password")})
    query = opts.get("query")
    if not query:
        schema, table = o.get("source_schema"), o.get("source_table")
        query = f"SELECT * FROM {schema + '.' if schema else ''}{table}"
    cn = pyodbc.connect(cs)
    cur = cn.cursor()
    cur.execute(query)
    cols = [dd[0] for dd in cur.description]
    rows = [tuple(r) for r in cur.fetchall()]
    cn.close()
    if not rows:
        return spark.createDataFrame([], ", ".join(f"`{x}` string" for x in cols))
    return spark.createDataFrame(rows, cols)
