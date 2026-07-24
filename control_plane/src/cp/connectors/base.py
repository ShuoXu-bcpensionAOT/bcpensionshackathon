"""Shared connector helpers: connection resolution (KV + connection_json), JDBC dialects/loader,
driver-side DB-API bridge, on-demand pip, JSON pointer, and config-driven column selection."""
import json
import re

from pyspark.sql import functions as F

from ..runtime import spark, SOURCE_SERVER
from ..secrets import get_secret

COMPLEX_TYPES = {"xml", "geography", "geometry", "hierarchyid", "varbinary", "image", "sql_variant"}

# jdbc dialects: driver class + url template + default port. The SQL Server driver ships with
# Fabric Spark; Oracle/DB2/Postgres/MySQL need their JDBC jar added to the Fabric Environment.
JDBC_DIALECTS = {
    "sqlserver":  {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "port": 1433,
                   "url": "jdbc:sqlserver://{host}:{port};database={database};encrypt=true;trustServerCertificate=true;loginTimeout=60"},
    "oracle":     {"driver": "oracle.jdbc.OracleDriver", "port": 1521,
                   "url": "jdbc:oracle:thin:@//{host}:{port}/{database}"},
    "db2":        {"driver": "com.ibm.db2.jcc.DB2Driver", "port": 50000,
                   "url": "jdbc:db2://{host}:{port}/{database}"},
    "postgresql": {"driver": "org.postgresql.Driver", "port": 5432,
                   "url": "jdbc:postgresql://{host}:{port}/{database}"},
    "mysql":      {"driver": "com.mysql.cj.jdbc.Driver", "port": 3306,
                   "url": "jdbc:mysql://{host}:{port}/{database}"},
}


def _jdbc_driver(c, d):
    """Resolve the JDBC driver class. Accepts a real class, a dialect name (e.g. 'sqlserver' in a
    secret), or falls back to the dialect preset."""
    drv = c.get("driver")
    if drv in JDBC_DIALECTS:
        return JDBC_DIALECTS[drv]["driver"]
    return drv or (d["driver"] if d else None)


def _jdbc_load(url, driver, user, password, dbtable=None, query=None, tries=4):
    import time
    r = (spark.read.format("jdbc").option("url", url)
         .option("user", user).option("password", password).option("driver", driver))
    r = r.option("query", query) if query else r.option("dbtable", dbtable)
    last = None
    for a in range(tries):
        try:
            return r.load()
        except Exception as e:  # transient connect/timeout -> back off and retry
            last = e
            if any(s in str(e).lower() for s in ("connect", "timed out", "tcp/ip", "reset")):
                time.sleep(15 * (a + 1))
                continue
            raise
    raise last


def jdbc_read(server, database, user, password, dbtable=None, query=None, tries=4):
    """SQL Server JDBC read (back-compat helper used by metadata schema discovery)."""
    d = JDBC_DIALECTS["sqlserver"]
    url = d["url"].format(host=server, port=d["port"], database=database)
    return _jdbc_load(url, d["driver"], user, password, dbtable, query, tries)


def run_catalog_query(o, sql, user="", password=""):
    """Run a read-only catalog/metadata query against a source database using the SAME connection
    path its ingest connector uses — pure-Python DB-API for oracle/db2 (thin, pip-installed on
    demand), JDBC otherwise (and for oracle/db2 with connection_json.mode='jdbc'). Returns a Spark
    DataFrame. Discoverers use this to read INFORMATION_SCHEMA / ALL_* / SYSCAT without duplicating
    connection logic. (Alias resolution mirrors resolve_connector; inlined to avoid an import cycle.)"""
    name = (o.get("connector") or o.get("source_type") or "sqlserver").lower()
    name = {"sql": "sqlserver", "mssql": "sqlserver", "custom_jdbc": "sqlserver"}.get(name, name)
    c = _resolve_conn(o)
    u, p = c.get("user") or user, c.get("password") or password
    db = c.get("database") or o.get("database_name")
    jdbc_mode = (c.get("mode") or "").lower() == "jdbc"
    if name == "oracle" and not jdbc_mode:
        oracledb = _ensure_pkg("oracledb")
        dsn = c.get("dsn") or (f"{c.get('host')}:{c.get('port', 1521)}/"
                               f"{c.get('service') or c.get('database') or db}")
        return _dbapi_to_spark(oracledb.connect(user=u, password=p, dsn=dsn), sql)
    if name == "db2" and not jdbc_mode:
        dbi = _ensure_pkg("ibm_db_dbi", "ibm_db")
        cs = (f"DATABASE={db};HOSTNAME={c.get('host')};PORT={c.get('port', 50000)};"
              f"PROTOCOL=TCPIP;UID={u};PWD={p};")
        return _dbapi_to_spark(dbi.connect(cs, "", ""), sql)
    d = JDBC_DIALECTS.get(name)
    url = c.get("url") or c.get("connection_string") or (
        d["url"].format(host=c.get("host") or SOURCE_SERVER, port=c.get("port") or d["port"],
                        database=db) if d else None)
    if not url:
        raise Exception(f"run_catalog_query: no JDBC url/dialect for connector '{name}'")
    return _jdbc_load(url, _jdbc_driver(c, d), u, p, query=sql)


def _opts(o):
    return json.loads(o["source_options_json"]) if o.get("source_options_json") else {}


def _conn(o):
    return json.loads(o["connection_json"]) if o.get("connection_json") else {}


def _resolve_conn(o):
    """Full connection params for a source: the KV secret named by datasource.secret_name is the
    base (the COMPLETE connection info — host/port/db/user/password or a raw connection string/url),
    with connection_json layered on top for non-secret overrides (e.g. mode, driver). The secret
    value may be a JSON object (parsed) or a raw string (kept under 'connection_string')."""
    conn = {}
    raw = get_secret(o.get("secret_name")) if o.get("secret_name") else None
    if raw:
        try:
            parsed = json.loads(raw)
            conn = parsed if isinstance(parsed, dict) else {"connection_string": raw}
        except (ValueError, TypeError):
            conn = {"connection_string": raw}
    if o.get("connection_json"):
        conn = {**conn, **json.loads(o["connection_json"])}
    return conn


def _dig(obj, path):
    """Follow a dot-path (e.g. 'a.b.0') into nested JSON/lists."""
    for k in [x for x in str(path or "").split(".") if x]:
        obj = obj[int(k)] if isinstance(obj, list) else obj[k]
    return obj


def _ensure_pkg(import_name, pip_name=None):
    """Import a package, pip-installing it into the session on first use (plug-and-play).
    Used by the pure-Python DB connectors so no Fabric Environment/driver jar is needed."""
    import importlib
    try:
        return importlib.import_module(import_name)
    except ImportError:
        import subprocess
        import sys
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pip_name or import_name],
                       check=True)
        return importlib.import_module(import_name)


def _default_query(o):
    schema, table = o.get("source_schema"), o.get("source_table")
    return f"SELECT * FROM {schema + '.' if schema else ''}{table}"


def _dbapi_to_spark(cn, query):
    """Run a query over a DB-API connection (driver-side) -> Spark DataFrame."""
    import pandas as pd
    try:
        cur = cn.cursor()
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        rows = [tuple(r) for r in cur.fetchall()]
    finally:
        cn.close()
    if not rows:
        return spark.createDataFrame([], ", ".join(f"`{c}` string" for c in cols))
    pdf = pd.DataFrame(rows, columns=cols)
    pdf = pdf.where(pd.notnull(pdf), None)
    return spark.createDataFrame(pdf)


def apply_select(df, spec):
    """Config-driven schema selection for the landed data (source_options_json.select).
    Controls exactly which columns land, their order, names and types. Forms:
      - ["colA","colB"]                              projection (keep, in order)
      - [{"source":"C","name":"c","type":"double"}]  project + rename + cast
      - {"columns":[...],"rename":{s:n},"cast":{c:t}} projection + rename + cast
    `source` names are the connector's raw output column names. No spec -> land full schema."""
    if not spec:
        return df
    if isinstance(spec, dict):
        sel = spec.get("columns") or df.columns
        out = df.select(*[F.col(c) for c in sel if c in df.columns])
        for src, new in (spec.get("rename") or {}).items():
            if src in out.columns:
                out = out.withColumnRenamed(src, new)
        for c, t in (spec.get("cast") or {}).items():
            if c in out.columns:
                out = out.withColumn(c, F.col(c).cast(t))
        return out
    exprs = []
    for item in spec:
        if isinstance(item, str):
            if item in df.columns:
                exprs.append(F.col(item))
        else:
            src = item.get("source") or item.get("name")
            if src in df.columns:
                e = F.col(src).cast(item["type"]) if item.get("type") else F.col(src)
                exprs.append(e.alias(item.get("name", src)))
    return df.select(*exprs) if exprs else df
