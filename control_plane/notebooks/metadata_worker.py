# PARAMETERS
run_id = "manual"
load_group = 1
src_user = ""
src_password = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Metadata worker:
#  (1) DISCOVER objects at each datasource and register them into source_object as is_active=0
#      (never hand-author objects — review/tweak filters, then activate). Existing objects are
#      preserved (matched by source_id + schema + table).
#  (2) Snapshot columns + log schema drift for the ACTIVE objects (SQL Server only).
import json
import traceback
from pyspark.sql import functions as F

_INSERT = ("INSERT INTO dbo.source_object (object_id,source_id,source_schema,source_table,"
           "target_name,load_type,key_columns_json,watermark_column,watermark_type,is_active,"
           "processing_state,source_options_json,suffix) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")


def register_discovered(lg):
    for ds in config_query("SELECT * FROM dbo.datasource WHERE load_group=? AND is_active=1", (lg,)):
        conn = resolve_connector(ds)
        try:
            cands = discover_objects(ds)
        except Exception:
            files_put(f"_cp_err_discover_{ds['source_name']}_{run_id}.txt", traceback.format_exc())
            print(f"discover {ds['source_name']} FAILED (see Files) — continuing")
            continue
        if cands is None:
            print(f"discover {ds['source_name']}: no discoverer for '{conn}', skip")
            continue
        sid = ds["source_id"]
        existing = {(r["source_schema"] or "", r["source_table"]) for r in config_query(
            "SELECT source_schema, source_table FROM dbo.source_object WHERE source_id=?", (sid,))}
        inserts = []
        for cand in cands:
            sch, tbl = cand.get("source_schema"), cand.get("source_table")
            if (sch or "", tbl) in existing:               # already represented — preserve tweaks
                continue
            oid = _norm_ident(f"{ds['source_name']}_{sch or 'dbo'}_{tbl}")
            inserts.append((oid, sid, sch, tbl, None, "full", cand.get("key_columns_json"),
                            None, None, 0, "ACTIVE", cand.get("source_options_json"), None))
        config_exec_many(_INSERT, inserts)
        log_object_run(run_id, ds["source_name"], "discover", "SUCCEEDED", target_count=len(inserts))
        print(f"discover {ds['source_name']}: +{len(inserts)} new object(s) is_active=0 "
              f"({len(cands)} found, {len(existing)} already registered)")


def snapshot_columns(lg):
    objs = config_query(
        "SELECT o.object_id,o.source_schema,o.source_table,d.database_name,d.connector,"
        "d.source_type,d.secret_name,d.connection_json "
        "FROM dbo.source_object o JOIN dbo.datasource d ON o.source_id=d.source_id "
        "WHERE o.is_active=1 AND o.processing_state='ACTIVE' AND d.load_group=? ORDER BY o.object_id",
        (lg,))
    p = tpath("config", "source_column")
    d = JDBC_DIALECTS["sqlserver"]
    for o in objs:
        oid, schema, table = o["object_id"], o["source_schema"], o["source_table"]
        if resolve_connector(o) != "sqlserver":
            log_object_run(run_id, oid, "metadata", "SKIPPED", details={"connector": resolve_connector(o)})
            print(f"metadata {oid}: skip column snapshot (connector {resolve_connector(o)})")
            continue
        c = _resolve_conn(o)
        url = c.get("url") or c.get("connection_string") or d["url"].format(
            host=c.get("host") or SOURCE_SERVER, port=c.get("port") or d["port"],
            database=c.get("database") or o.get("database_name"))
        rows = _jdbc_load(url, _jdbc_driver(c, d), c.get("user") or src_user,
                          c.get("password") or src_password, query=(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'")).collect()
        cur = {r["COLUMN_NAME"]: r["DATA_TYPE"] for r in rows}
        prev = set()
        if delta_exists(p):
            prev = {r["column_name"] for r in read_path(p).where(F.col("object_id") == oid).collect()}
        curset = set(cur)
        if prev:
            ev = [{"event_id": f"{run_id}_{oid}_{col}", "run_id": run_id, "object_id": oid,
                   "column_name": col, "drift_type": dt, "severity": sv, "details": "{}",
                   "detected_at": now_ts()}
                  for col, dt, sv in ([(x, "COLUMN_ADDED", "info") for x in sorted(curset - prev)] +
                                      [(x, "COLUMN_REMOVED", "warning") for x in sorted(prev - curset)])]
            if ev:
                append_rows("schema_drift_event", ev)
        snap = [{"object_id": oid, "column_name": col, "source_data_type": t,
                 "discovered_at": now_ts(), "is_active": True} for col, t in cur.items()]
        merge_upsert(p, spark.createDataFrame(snap), ["object_id", "column_name"])
        log_object_run(run_id, oid, "metadata", "SUCCEEDED", target_count=len(cur))
        print(f"metadata {oid}: {len(cur)} cols")


def work():
    lg = int(load_group)
    seed_control_tables()       # pre-create audit tables so parallel workers don't race to create
    start_run(run_id, {"engine": "pipeline", "load_group": lg})
    register_discovered(lg)     # 1) auto-register objects (is_active=0)
    snapshot_columns(lg)        # 2) column drift for active objects


try:
    work()
except Exception:
    files_put(f"_cp_err_metadata_{run_id}.txt", traceback.format_exc())
    raise
