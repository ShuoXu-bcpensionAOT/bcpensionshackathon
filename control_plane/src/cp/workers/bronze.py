"""Bronze entrypoint: load ONE source object via its registered connector and land it in bronze.
All logic lives here so the bronze_worker notebook is a 3-cell shell."""
import json
import traceback

from pyspark.sql import functions as F

from ..naming import landed_table
from ..runtime import tpath
from ..storage import write_path, files_put
from ..connectors import resolve_connector, run_connector
from ..connectors.base import _opts, apply_select
from ..audit import update_watermark, log_object_run


def bronze(run_id="manual", object_json="{}", object=None, src_user="", src_password="", **kw):
    """object_json: JSON string of the object config (from the planner); or pass `object` as a dict."""
    o = object if object is not None else json.loads(object_json or "{}")

    def _work():
        oid, load_type = o["object_id"], o["load_type"]
        schema, table = landed_table(o)                        # schema-enabled: (schema, table)
        wm_col = o.get("watermark_column")
        connector = resolve_connector(o)

        df = run_connector(o, src_user, src_password)          # dispatch by connector type
        df = apply_select(df, _opts(o).get("select"))          # config-driven landed schema

        label = ".".join(x for x in [o.get("source_schema"), o.get("source_table")] if x) or table
        df = (df.withColumn("_run_id", F.lit(run_id))
                .withColumn("_source_system", F.lit(o.get("source_name", "")))
                .withColumn("_source_table", F.lit(label))
                .withColumn("_bronze_ingest_ts", F.current_timestamp()))
        cnt = df.count()
        mode = "append" if load_type in ("incremental", "append") else "overwrite"
        write_path(df, tpath("bronze", table, schema), mode=mode)
        if wm_col and wm_col in df.columns and cnt:
            update_watermark(oid, df.agg(F.max(F.col(wm_col))).collect()[0][0])
        log_object_run(run_id, oid, "bronze", "SUCCEEDED", source_count=cnt, target_count=cnt,
                       details={"mode": mode, "connector": connector})
        print(f"bronze {schema}.{table}: {cnt} rows ({mode}, connector={connector})")

    try:
        _work()
    except Exception:
        files_put(f"_cp_err_bronze_{o.get('object_id', 'x')}_{run_id}.txt", traceback.format_exc())
        raise
