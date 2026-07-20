# PARAMETERS
run_id = "manual"
object_json = "{}"
src_user = ""
src_password = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Bronze worker: load ONE source object via its registered connector (config passed as
# object_json by the pipeline). The connector fetches the raw extract; apply_select controls
# the landed schema (config-driven); control columns are added; the result lands in bronze.
import json
import traceback
from pyspark.sql import functions as F

o = json.loads(object_json)


def work():
    oid, target, load_type = o["object_id"], landed_table(o), o["load_type"]
    wm_col = o.get("watermark_column")
    connector = resolve_connector(o)

    df = run_connector(o, src_user, src_password)          # dispatch by connector type
    df = apply_select(df, _opts(o).get("select"))          # config-driven landed schema

    label = ".".join(x for x in [o.get("source_schema"), o.get("source_table")] if x) or target
    df = (df.withColumn("_run_id", F.lit(run_id))
            .withColumn("_source_system", F.lit(o.get("source_name", "")))
            .withColumn("_source_table", F.lit(label))
            .withColumn("_bronze_ingest_ts", F.current_timestamp()))
    cnt = df.count()
    mode = "append" if load_type == "incremental" else "overwrite"
    write_path(df, tpath("bronze", target), mode=mode)
    if wm_col and wm_col in df.columns and cnt:
        update_watermark(oid, df.agg(F.max(F.col(wm_col))).collect()[0][0])
    log_object_run(run_id, oid, "bronze", "SUCCEEDED", source_count=cnt, target_count=cnt,
                   details={"mode": mode, "connector": connector})
    print(f"bronze {target}: {cnt} rows ({mode}, connector={connector})")


try:
    work()
except Exception:
    files_put(f"_cp_err_bronze_{o.get('object_id', 'x')}_{run_id}.txt", traceback.format_exc())
    raise
