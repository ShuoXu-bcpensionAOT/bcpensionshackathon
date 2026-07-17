# PARAMETERS
run_id = "manual"
object_json = "{}"
src_user = ""
src_password = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Bronze worker: load ONE source object (config passed as object_json by the pipeline).
import json
import traceback
from pyspark.sql import functions as F

COMPLEX = {"xml", "geography", "geometry", "hierarchyid", "varbinary", "image", "sql_variant"}
o = json.loads(object_json)


def work():
    oid, schema, table = o["object_id"], o["source_schema"], o["source_table"]
    target, database, load_type = o["target_name"], o["database_name"], o["load_type"]
    wm_col = o.get("watermark_column")
    server = SOURCE_SERVER

    cols = jdbc_read(server, database, src_user, src_password, query=(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'")).collect()
    keep = [c["COLUMN_NAME"] for c in cols if c["DATA_TYPE"].lower() not in COMPLEX]
    col_sql = ", ".join(f"[{c}]" for c in keep)

    pred, wm = "", None
    if load_type == "incremental" and wm_col:
        wm = get_watermark(oid)
        if wm:
            pred = f" WHERE [{wm_col}] > '{wm}'"
    df = jdbc_read(server, database, src_user, src_password,
                   query=f"SELECT {col_sql} FROM [{schema}].[{table}]{pred}")
    df = (df.withColumn("_run_id", F.lit(run_id))
            .withColumn("_source_system", F.lit(o.get("source_name", "")))
            .withColumn("_source_table", F.lit(f"{schema}.{table}"))
            .withColumn("_bronze_ingest_ts", F.current_timestamp()))
    cnt = df.count()
    mode = "append" if load_type == "incremental" else "overwrite"
    write_path(df, tpath("bronze", target), mode=mode)
    if wm_col and wm_col in df.columns and cnt:
        update_watermark(oid, df.agg(F.max(F.col(wm_col))).collect()[0][0])
    log_object_run(run_id, oid, "bronze", "SUCCEEDED", source_count=cnt, target_count=cnt,
                   details={"mode": mode})
    print(f"bronze {target}: {cnt} rows ({mode})")


try:
    work()
except Exception:
    files_put(f"_cp_err_bronze_{o.get('object_id', 'x')}_{run_id}.txt", traceback.format_exc())
    raise
