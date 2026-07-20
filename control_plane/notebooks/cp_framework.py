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


# --- JDBC source ---
def jdbc_read(server, database, user, password, dbtable=None, query=None, tries=4):
    import time
    url = (f"jdbc:sqlserver://{server}:1433;database={database};"
           "encrypt=true;trustServerCertificate=true;loginTimeout=60")
    r = (spark.read.format("jdbc").option("url", url)  # noqa: F821
         .option("user", user).option("password", password)
         .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"))
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
