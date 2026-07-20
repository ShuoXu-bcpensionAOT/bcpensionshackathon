"""cp_framework — shared helpers for the Fabric control plane (run via %run cp_framework).

Deployed as a Fabric notebook; engine notebooks `%run cp_framework` to import
these functions/constants into their session. GUID-based OneLake paths only
(workspace-name paths are unreliable).
"""
import json
import re
from datetime import datetime, timezone

import notebookutils
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# --- runtime self-configuration: NO hardcoded IDs ---
# Workspace id from the running context; environment config (lakehouse names,
# source server) from the cp_vars Variable Library (the active value set is
# swapped per environment by CICD); lakehouse ids resolved by name.
try:
    _VL = notebookutils.variableLibrary.getLibrary("cp_vars")
except Exception:
    _VL = None


def var(name, default=None):
    return getattr(_VL, name, default) if _VL is not None else default


LAYER_NAMES = {
    "config": var("config_lakehouse", "metadata"),
    "bronze": var("bronze_lakehouse", "bronze"),
    "silver": var("silver_lakehouse", "silver"),
    "gold":   var("gold_lakehouse", "gold"),
}
SOURCE_SERVER = var("source_server", None)
SOURCE_CONNECTION = var("source_connection", "")
KEY_VAULT_URL = var("key_vault_url", None)

WS_ID = notebookutils.runtime.context["currentWorkspaceId"]
_lh_by_name = {l["displayName"]: l["id"] for l in notebookutils.lakehouse.list()}
LH = {logical: _lh_by_name[name] for logical, name in LAYER_NAMES.items()}
STAGE_LH, QUAR_LH = LH["gold"], LH["silver"]  # stage_/quarantine_ prefixed tables

# Authored config lives in a Fabric SQL Database (users edit it via T-SQL). The engine
# reads it from the SQL DB's OneLake mirror (Delta). Runtime state stays in the lakehouse.
CONFIG_DB_NAME = "config_db"


def _fabric_api_token():
    import requests  # noqa: F401
    for aud in ("pbi", "https://api.fabric.microsoft.com", "https://analysis.windows.net/powerbi/api"):
        try:
            return notebookutils.credentials.getToken(aud)
        except Exception:
            continue
    return None


def _resolve_config_sqldb():
    import requests
    tk = _fabric_api_token()
    r = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/items?type=SQLDatabase",
                     headers={"Authorization": f"Bearer {tk}"})
    for i in r.json().get("value", []):
        if i["displayName"] == CONFIG_DB_NAME:
            return i["id"]
    raise Exception(f"{CONFIG_DB_NAME} SQL Database not found in workspace {WS_ID}")


CONFIG_SQLDB_ID = _resolve_config_sqldb()

CONTROL_COLS = {
    "_run_id", "_source_system", "_source_table", "_bronze_ingest_ts",
    "_silver_run_id", "_silver_updated_at", "_row_hash", "_is_current",
    "_effective_start_ts", "_effective_end_ts", "_gold_run_id", "_gold_updated_at",
    "_ingested_at",
}


def tpath(lh_key_or_guid, table):
    guid = LH.get(lh_key_or_guid, lh_key_or_guid)
    return f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{guid}/Tables/{table}"


def now_ts():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def snake(name):
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", str(name))
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower()


def _norm_ident(s):
    """Lowercase, non-alphanumeric -> _, collapse/strip underscores."""
    return re.sub(r"_+", "_", re.sub(r"[^0-9A-Za-z]+", "_", str(s or "").strip().lower())).strip("_")


def landed_table(o):
    """The landed table name for a source object. Explicit target_name wins (back-compat);
    otherwise derive the flat namespaced convention
        {source_name}_{source_schema|dbo}_{source_table}[_{suffix}]
    e.g. source 'Stats Can', schema null, table 'sales', suffix 'bc' -> stats_can_dbo_sales_bc."""
    if o.get("target_name"):
        return o["target_name"]
    parts = [o.get("source_name"), (o.get("source_schema") or "dbo"), o.get("source_table")]
    name = "_".join(_norm_ident(p) for p in parts if p)
    if o.get("suffix"):
        name = f"{name}_{_norm_ident(o['suffix'])}"
    return name


def delta_exists(path):
    try:
        return DeltaTable.isDeltaTable(spark, path)  # noqa: F821
    except Exception:
        return False


def read_path(path):
    return spark.read.format("delta").load(path)  # noqa: F821


def read_config(table):
    """Read an authored-config table from the config SQL DB's OneLake mirror.
    BIT columns mirror as boolean; normalize is_active so filters are robust."""
    df = read_path(f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/"
                   f"{CONFIG_SQLDB_ID}/Tables/dbo/{table}")
    if "is_active" in df.columns:
        df = df.withColumn("is_active", F.col("is_active").cast("boolean"))
    return df


# --- config SQL DB direct access (pyodbc + AAD) — used by planners/workers ---
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


# --- cleansing (transform) functions — applied on silver, config-driven, registry-extensible ---
def _cf_trim(df, cols, p):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c).cast("string")))
    return df


def _cf_normalize_text(df, cols, p):
    case = p.get("case")
    for c in cols:
        if c not in df.columns:
            continue
        col = F.trim(F.col(c).cast("string"))
        if p.get("collapse_spaces", True):
            col = F.regexp_replace(col, r"\s+", " ")
        if case == "lower":
            col = F.lower(col)
        elif case == "upper":
            col = F.upper(col)
        elif case == "title":
            col = F.initcap(col)
        if p.get("empty_as_null", True):
            col = F.when(col == "", None).otherwise(col)
        df = df.withColumn(c, col)
    return df


def _cf_fill_nulls(df, cols, p):
    default = p.get("default", p.get("value"))
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.coalesce(F.col(c), F.lit(default)))
    return df


def _cf_parse_datetime(df, cols, p):
    conv = F.to_date if p.get("target_type", "date") == "date" else F.to_timestamp
    formats = p.get("formats", ["yyyy-MM-dd"])
    for c in cols:
        if c not in df.columns:
            continue
        parsed = F.lit(None)
        for fmt in formats:
            parsed = F.coalesce(parsed, conv(F.col(c).cast("string"), fmt))
        df = df.withColumn(p.get("into") or c, parsed)
    return df


def _cf_case(fn):
    def apply(df, cols, p):
        for c in cols:
            if c in df.columns:
                df = df.withColumn(c, fn(F.col(c).cast("string")))
        return df
    return apply


def _cf_replace(df, cols, p):
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.regexp_replace(F.col(c).cast("string"),
                                                   p.get("pattern", ""), p.get("replacement", "")))
    return df


CLEANSE_FUNCS = {
    "trim": _cf_trim, "normalize_text": _cf_normalize_text, "fill_nulls": _cf_fill_nulls,
    "parse_datetime": _cf_parse_datetime, "replace": _cf_replace,
    "to_upper": _cf_case(F.upper), "to_lower": _cf_case(F.lower), "to_title": _cf_case(F.initcap),
}


def register_cleanse_function(name, fn):
    """Extend the cleansing library (fn signature: (df, cols:list, params:dict) -> df)."""
    CLEANSE_FUNCS[name] = fn


def apply_cleansing(df, rules):
    """Apply active cleanse rules (list of dicts) in apply_order. Ignores unknown functions."""
    import json
    for r in sorted(rules, key=lambda x: (x.get("apply_order") or 0)):
        fn = CLEANSE_FUNCS.get(r.get("function"))
        if not fn:
            continue
        cols = [c.strip() for c in str(r.get("columns") or "").split(";") if c.strip()]
        params = json.loads(r["parameters_json"]) if r.get("parameters_json") else {}
        df = fn(df, cols, params)
    return df


def write_path(df, path, mode="overwrite"):
    w = df.write.format("delta").mode(mode)
    w = w.option("overwriteSchema", "true") if mode == "overwrite" else w.option("mergeSchema", "true")
    w.save(path)


def business_cols(df):
    return [c for c in df.columns if c not in CONTROL_COLS and not c.startswith("_")]


def row_hash(df, cols=None, out="_row_hash"):
    cols = cols or business_cols(df)
    if not cols:
        return df.withColumn(out, F.sha2(F.lit(""), 256))
    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in cols]
    return df.withColumn(out, F.sha2(F.concat_ws("||", *exprs), 256))


# --- ingestion connectors (registry-extensible; each datasource declares its connector) ---
# A connector is a function (object_config: dict, user, password) -> raw Spark DataFrame.
# object_config carries source_object + datasource fields (see cp_plan), plus the parsed
# JSON columns connection_json (datasource-level params) and source_options_json (per object).
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


def _jdbc_load(url, driver, user, password, dbtable=None, query=None, tries=4):
    import time
    r = (spark.read.format("jdbc").option("url", url)  # noqa: F821
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


def get_secret(name):
    """Read a secret from the Key Vault named by the cp_vars `key_vault_url` variable
    (uses the running identity's token — grant it KV 'get' on that vault)."""
    if not name:
        return None
    if not KEY_VAULT_URL:
        raise Exception("cp_vars.key_vault_url is not set — cannot resolve secret " + str(name))
    return notebookutils.credentials.getSecret(KEY_VAULT_URL, name)


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
    driver = c.get("driver") or (d["driver"] if d else None)
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
        return spark.createDataFrame([], ", ".join(f"`{x}` string" for x in cols))  # noqa: F821
    return spark.createDataFrame(rows, cols)           # noqa: F821


def _ic_rest_api(o, user, password):
    """Generic REST/JSON connector. source_options_json: url, method, headers, params, body,
    record_path (dot-path to the list of records). connection_json may hold base headers."""
    import requests
    import pandas as pd
    c, opts = _conn(o), _opts(o)
    url = opts.get("url") or c.get("base_url")
    resp = requests.request((opts.get("method") or "GET").upper(), url,
                            headers={**c.get("headers", {}), **opts.get("headers", {})},
                            params=opts.get("params"), json=opts.get("body"), timeout=120)
    resp.raise_for_status()
    data = resp.json()
    for key in [k for k in (opts.get("record_path") or "").split(".") if k]:
        data = data[key]
    if isinstance(data, dict):
        data = [data]
    pdf = pd.json_normalize(data)
    pdf.columns = [re.sub(r"[^A-Za-z0-9_]", "_", str(x)) for x in pdf.columns]
    pdf = pdf.where(pd.notnull(pdf), None)
    return spark.createDataFrame(pdf)                  # noqa: F821


def _ic_statcan_wds(o, user, password):
    """Statistics Canada WDS full-table download (getFullTableDownloadCSV -> zip -> CSV).
    source_options_json: {table_id, language(en/fr), filters:{<original column>:<value>}}.
    Equality filters (on ORIGINAL column names) are applied at ingest to land a subset."""
    import requests
    import zipfile
    import io
    import pandas as pd
    opts = _opts(o)
    table_id, lang = str(opts["table_id"]), opts.get("language", "en")
    meta = requests.get(
        f"https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{table_id}/{lang}",
        timeout=60).json()
    if str(meta.get("status")).upper() != "SUCCESS":
        raise Exception(f"StatCan WDS error for table {table_id}: {meta}")
    zf = zipfile.ZipFile(io.BytesIO(requests.get(meta["object"], timeout=600).content))
    csv_name = [f for f in zf.namelist() if "_Meta" not in f and f.lower().endswith(".csv")][0]
    pdf = pd.read_csv(zf.open(csv_name), low_memory=False, dtype=str)
    for col, val in (opts.get("filters") or {}).items():
        if col in pdf.columns:
            pdf = pdf[pdf[col].astype(str).str.strip() == str(val)]
    pdf.columns = [re.sub(r"[^A-Za-z0-9_]", "_", str(x)) for x in pdf.columns]
    pdf = pdf.where(pd.notnull(pdf), None)
    return spark.createDataFrame(pdf)                  # noqa: F821


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
        return spark.createDataFrame([], ", ".join(f"`{c}` string" for c in cols))  # noqa: F821
    pdf = pd.DataFrame(rows, columns=cols)
    pdf = pdf.where(pd.notnull(pdf), None)
    return spark.createDataFrame(pdf)                  # noqa: F821


def _ic_oracle(o, user, password):
    """Oracle. Default: pure-Python `oracledb` THIN mode (pip-installed on demand, no Oracle
    client, no jar) reading driver-side. Opt in to distributed Spark JDBC (needs the ojdbc jar
    on an attached Fabric Environment) with connection_json.mode='jdbc'."""
    c, opts = _resolve_conn(o), _opts(o)
    if (c.get("mode") or "").lower() == "jdbc":
        return _ic_jdbc(o, user, password)
    user = c.get("user") or user
    password = c.get("password") or password
    oracledb = _ensure_pkg("oracledb")
    dsn = c.get("dsn") or (f"{c.get('host')}:{c.get('port', 1521)}/"
                           f"{c.get('service') or c.get('database') or o.get('database_name')}")
    cn = oracledb.connect(user=user, password=password, dsn=dsn)   # thin mode
    return _dbapi_to_spark(cn, opts.get("query") or _default_query(o))


def _ic_db2(o, user, password):
    """IBM DB2. Default: pure-Python `ibm_db` (pip-installed on demand; the wheel bundles the
    client) reading driver-side. Opt in to distributed Spark JDBC (needs the db2jcc jar on an
    attached Fabric Environment) with connection_json.mode='jdbc'."""
    c, opts = _resolve_conn(o), _opts(o)
    if (c.get("mode") or "").lower() == "jdbc":
        return _ic_jdbc(o, user, password)
    user = c.get("user") or user
    password = c.get("password") or password
    dbi = _ensure_pkg("ibm_db_dbi", "ibm_db")
    cs = (f"DATABASE={c.get('database') or o.get('database_name')};HOSTNAME={c.get('host')};"
          f"PORT={c.get('port', 50000)};PROTOCOL=TCPIP;UID={user};PWD={password};")
    return _dbapi_to_spark(dbi.connect(cs, "", ""), opts.get("query") or _default_query(o))


INGEST_CONNECTORS = {
    # bundled JDBC jars in the Fabric runtime -> distributed Spark JDBC, zero setup
    "sqlserver": _ic_jdbc, "postgresql": _ic_jdbc, "mysql": _ic_jdbc, "jdbc": _ic_jdbc,
    # not bundled -> pure-Python driver-side by default (self-installing), JDBC via mode='jdbc'
    "oracle": _ic_oracle, "db2": _ic_db2,
    "odbc": _ic_odbc, "rest_api": _ic_rest_api, "statcan_wds": _ic_statcan_wds,
}
# connectors that support metadata schema discovery (INFORMATION_SCHEMA over JDBC)
DISCOVERABLE_CONNECTORS = {"sqlserver", "oracle", "db2", "postgresql", "mysql", "jdbc"}


def register_ingest_connector(name, fn):
    """Register a source connector: fn(object_config:dict, user, password) -> Spark DataFrame."""
    INGEST_CONNECTORS[name] = fn


def resolve_connector(o):
    """The connector key for a source object: explicit `connector`, else `source_type`."""
    name = (o.get("connector") or o.get("source_type") or "sqlserver").lower()
    return {"sql": "sqlserver", "mssql": "sqlserver", "custom_jdbc": "sqlserver",
            "api": "rest_api", "statcan": "statcan_wds"}.get(name, name)


def run_connector(o, user, password):
    """Dispatch to the datasource's connector and return the raw extract DataFrame."""
    name = resolve_connector(o)
    fn = INGEST_CONNECTORS.get(name)
    if not fn:
        raise Exception(f"no ingest connector registered for '{name}' (object {o.get('object_id')})")
    return fn(o, user, password)


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


# Explicit schemas so all-None columns (e.g. run_completed_at) don't break inference.
SCHEMAS = {
    "ingestion_run": "run_id string, run_started_at timestamp, run_completed_at timestamp, status string, details string",
    "object_load_run": ("run_id string, object_id string, layer string, status string, "
                        "source_count long, target_count long, quarantine_count long, "
                        "started_at timestamp, ended_at timestamp, details string"),
    "watermark_state": "object_id string, watermark_value string, updated_at timestamp",
    "schema_drift_event": ("event_id string, run_id string, object_id string, column_name string, "
                          "drift_type string, severity string, details string, detected_at timestamp"),
    "dq_result": ("run_id string, object_id string, rule_id string, failed_count long, "
                  "passed_count long, status string, evaluated_at timestamp"),
    "parity_result": ("run_id string, object_id string, check_scope string, check_name string, "
                      "source_value string, target_value string, status string, checked_at timestamp"),
    "pipeline_run_log": ("pipeline_name string, run_id string, load_group int, activity string, "
                         "message string, logged_at timestamp"),
}


def files_put(name, text):
    """Write a text file to the config lakehouse Files area (GUID path)."""
    p = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH['config']}/Files/{name}"
    notebookutils.fs.put(p, text, True)  # noqa: F821


# --- audit / control writes (append; first write creates the table) ---
def append_rows(config_table, rows):
    if not rows:
        return
    rows = [_json_safe(r) for r in rows]
    schema = SCHEMAS.get(config_table)
    if schema:
        # order dict values to the schema's column order
        cols = [c.strip().split()[0] for c in schema.split(",")]
        rows = [[r.get(c) for c in cols] for r in rows]
        df = spark.createDataFrame(rows, schema)  # noqa: F821
    else:
        df = spark.createDataFrame(rows)  # noqa: F821
    write_path(df, tpath("config", config_table), mode="append")


def _json_safe(d):
    return {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in d.items()}


def start_run(run_id, details=None):
    append_rows("ingestion_run", [{
        "run_id": run_id, "run_started_at": now_ts(), "run_completed_at": None,
        "status": "RUNNING", "details": json.dumps(details or {})}])
    return run_id


def finish_run(run_id, status="SUCCEEDED", details=None):
    # append-only completion marker (avoids UPDATE for simplicity/idempotence)
    append_rows("ingestion_run", [{
        "run_id": run_id, "run_started_at": now_ts(), "run_completed_at": now_ts(),
        "status": status, "details": json.dumps(details or {})}])


def log_object_run(run_id, object_id, layer, status, source_count=None,
                   target_count=None, quarantine_count=None, details=None):
    append_rows("object_load_run", [{
        "run_id": run_id, "object_id": object_id, "layer": layer, "status": status,
        "source_count": _to_int(source_count), "target_count": _to_int(target_count),
        "quarantine_count": _to_int(quarantine_count), "started_at": now_ts(),
        "ended_at": now_ts(), "details": json.dumps(details or {})}])


def _to_int(v):
    return int(v) if v is not None else None


def get_watermark(object_id):
    p = tpath("config", "watermark_state")
    if not delta_exists(p):
        return None
    rows = (read_path(p).where(F.col("object_id") == object_id)
            .orderBy(F.col("updated_at").desc()).limit(1).collect())
    return rows[0]["watermark_value"] if rows else None


def update_watermark(object_id, value):
    if value is None:
        return
    append_rows("watermark_state", [{
        "object_id": object_id, "watermark_value": str(value), "updated_at": now_ts()}])


# --- merge helper (upsert) ---
def merge_upsert(target_path, source_df, keys):
    if not delta_exists(target_path):
        write_path(source_df, target_path, mode="overwrite")
        return
    tgt = DeltaTable.forPath(spark, target_path)  # noqa: F821
    cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
    (tgt.alias("t").merge(source_df.alias("s"), cond)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


# --- reusable gold writers (called by source-query notebooks) ---
def gold_write(stage, gold_type, gold_table, keys, run_id):
    stage = (stage.withColumn("_gold_run_id", F.lit(run_id))
                  .withColumn("_gold_updated_at", F.current_timestamp()))
    path = tpath("gold", gold_table)
    if gold_type in ("scd1", "fact"):
        merge_upsert(path, stage, keys)
    elif gold_type == "scd2":
        _scd2_merge(stage, path, keys)
    else:
        write_path(stage, path, "overwrite")
    return read_path(path).count()


def _scd2_merge(stage, path, keys):
    stage = row_hash(stage)
    incoming = (stage.withColumn("_effective_start_ts", F.current_timestamp())
                     .withColumn("_effective_end_ts", F.lit(None).cast("timestamp"))
                     .withColumn("_is_current", F.lit(True)))
    if not delta_exists(path):
        write_path(incoming, path, "overwrite")
        return
    tgt = DeltaTable.forPath(spark, path)  # noqa: F821
    cur = tgt.toDF().where(F.col("_is_current"))
    keycond = [incoming[k] == cur[k] for k in keys]
    changed = (incoming.join(cur, keycond, "inner")
               .where(incoming["_row_hash"] != cur["_row_hash"])
               .select(*[incoming[k].alias(k) for k in keys]))
    if changed.count():
        ec = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
        (tgt.alias("t").merge(changed.alias("s"), ec)
            .whenMatchedUpdate(set={"_is_current": F.lit(False),
                                    "_effective_end_ts": F.current_timestamp()}).execute())
    cur_keys = tgt.toDF().where(F.col("_is_current")).select(*keys)
    to_insert = incoming.join(cur_keys, keys, "left_anti")
    if to_insert.count():
        write_path(to_insert, path, "append")


def build_stage_and_gold(gold_object_id, stage_df, gold_type, stage_table, gold_table, keys, run_id):
    """Standard source-query epilogue: persist the stage table, then merge into gold + log."""
    write_path(stage_df, tpath(STAGE_LH, f"stage_{stage_table}"), "overwrite")
    cnt = gold_write(stage_df, gold_type, gold_table, keys, run_id)
    log_object_run(run_id, gold_object_id, "gold", "SUCCEEDED", target_count=cnt,
                   details={"gold_type": gold_type})
    print(f"gold {gold_table} ({gold_type}): {cnt} rows")
    return cnt


# --- DAG ---
def topo_levels(nodes, edges):
    remaining, done = set(nodes), set()
    parents = {n: set() for n in nodes}
    for p, c in edges:
        if c in parents and p in remaining:
            parents[c].add(p)
    levels = []
    while remaining:
        ready = sorted([n for n in remaining if parents[n] <= done])
        if not ready:
            raise ValueError(f"cycle in gold DAG: {remaining}")
        levels.append(ready)
        done |= set(ready)
        remaining -= set(ready)
    return levels


print("cp_framework loaded")
