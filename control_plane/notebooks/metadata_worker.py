# PARAMETERS
run_id = "manual"
load_group = 1
src_user = ""
src_password = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Metadata worker: start the run, discover source schema per object (INFORMATION_SCHEMA),
# snapshot source_column + log schema drift for the load group.
import traceback
from pyspark.sql import functions as F


def work():
    lg = int(load_group)
    start_run(run_id, {"engine": "pipeline", "load_group": lg})
    objs = config_query(
        "SELECT o.object_id,o.source_schema,o.source_table,d.database_name,d.connector,d.source_type "
        "FROM dbo.source_object o JOIN dbo.datasource d ON o.source_id=d.source_id "
        "WHERE o.is_active=1 AND o.processing_state='ACTIVE' AND d.load_group=? "
        "ORDER BY o.object_id", (lg,))
    server = SOURCE_SERVER
    p = tpath("config", "source_column")
    for o in objs:
        oid, schema, table, database = (o["object_id"], o["source_schema"],
                                        o["source_table"], o["database_name"])
        # Schema discovery uses SQL Server INFORMATION_SCHEMA; skip connectors without it
        # (API/file sources define their columns at ingest time).
        if resolve_connector(o) != "sqlserver":
            log_object_run(run_id, oid, "metadata", "SKIPPED",
                           details={"connector": resolve_connector(o)})
            print(f"metadata {oid}: skip (connector {resolve_connector(o)}, no schema discovery)")
            continue
        rows = jdbc_read(server, database, src_user, src_password, query=(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'")).collect()
        cur = {r["COLUMN_NAME"]: r["DATA_TYPE"] for r in rows}
        prev = set()
        if delta_exists(p):
            prev = {r["column_name"] for r in read_path(p).where(F.col("object_id") == oid).collect()}
        curset = set(cur)
        if prev:
            ev = [{"event_id": f"{run_id}_{oid}_{c}", "run_id": run_id, "object_id": oid,
                   "column_name": c, "drift_type": d, "severity": s, "details": "{}",
                   "detected_at": now_ts()}
                  for c, d, s in ([(c, "COLUMN_ADDED", "info") for c in sorted(curset - prev)] +
                                  [(c, "COLUMN_REMOVED", "warning") for c in sorted(prev - curset)])]
            if ev:
                append_rows("schema_drift_event", ev)
        snap = [{"object_id": oid, "column_name": c, "source_data_type": t,
                 "discovered_at": now_ts(), "is_active": True} for c, t in cur.items()]
        merge_upsert(p, spark.createDataFrame(snap), ["object_id", "column_name"])
        log_object_run(run_id, oid, "metadata", "SUCCEEDED", target_count=len(cur))
        print(f"metadata {oid}: {len(cur)} cols")


try:
    work()
except Exception:
    files_put(f"_cp_err_metadata_{run_id}.txt", traceback.format_exc())
    raise
