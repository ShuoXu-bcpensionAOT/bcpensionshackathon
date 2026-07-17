# PARAMETERS
run_id = "manual"
src_user = ""
src_password = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Source server comes from the cp_vars Variable Library (per-environment), not a param.
src_server = SOURCE_SERVER

# COMMAND ----------
# Ingest active source objects into bronze. full -> overwrite; incremental -> append
# rows with [watermark_column] > stored watermark. Complex SQL types are excluded.
import traceback
from pyspark.sql import functions as F

COMPLEX = {"xml", "geography", "geometry", "hierarchyid", "varbinary", "image", "sql_variant"}


def ingest_bronze():
    ds = read_config("datasource").select("source_id", "source_name", "database_name")
    objs = (read_config("source_object")
            .where((F.col("is_active")) & (F.col("processing_state") == "ACTIVE"))
            .join(ds, "source_id"))

    summary = []
    for r in objs.collect():
        oid, schema, table = r["object_id"], r["source_schema"], r["source_table"]
        database, target, load_type = r["database_name"], r["target_name"], r["load_type"]
        wm_col = r["watermark_column"]

        cols = jdbc_read(src_server, database, src_user, src_password, query=(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'")).collect()
        keep = [c["COLUMN_NAME"] for c in cols if c["DATA_TYPE"].lower() not in COMPLEX]
        col_sql = ", ".join(f"[{c}]" for c in keep)

        pred, wm = "", None
        if load_type == "incremental" and wm_col:
            wm = get_watermark(oid)
            if wm:
                pred = f" WHERE [{wm_col}] > '{wm}'"

        query = f"SELECT {col_sql} FROM [{schema}].[{table}]{pred}"
        df = jdbc_read(src_server, database, src_user, src_password, query=query)
        df = (df.withColumn("_run_id", F.lit(run_id))
                .withColumn("_source_system", F.lit(r["source_name"]))
                .withColumn("_source_table", F.lit(f"{schema}.{table}"))
                .withColumn("_bronze_ingest_ts", F.current_timestamp()))
        cnt = df.count()
        mode = "append" if load_type == "incremental" else "overwrite"
        write_path(df, tpath("bronze", target), mode=mode)

        if wm_col and wm_col in df.columns and cnt:
            new_wm = df.agg(F.max(F.col(wm_col))).collect()[0][0]
            update_watermark(oid, new_wm)

        log_object_run(run_id, oid, "bronze", "SUCCEEDED",
                       source_count=cnt, target_count=cnt,
                       details={"mode": mode, "predicate": pred, "from_watermark": wm})
        summary.append((target, cnt, mode))
        print(f"bronze {target}: {cnt} rows ({mode})")

    print("=== bronze summary ===")
    for t, c, m in summary:
        print(f"  {t:<48} {c:>8}  {m}")


try:
    ingest_bronze()
except Exception:
    files_put(f"_cp_err_bronze_{run_id}.txt", traceback.format_exc())
    raise
