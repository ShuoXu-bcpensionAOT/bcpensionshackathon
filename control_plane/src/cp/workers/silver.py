"""Silver entrypoint: build silver for ONE object — dedupe by key, row-hash, cleanse, DQ +
quarantine, upsert. All logic lives here so the silver_worker notebook is a 3-cell shell."""
import json
import traceback

from pyspark.sql import functions as F, Window

from ..naming import landed_table, snake, now_ts
from ..runtime import tpath, QUAR_LH
from ..storage import delta_exists, read_path, write_path, files_put
from ..config_db import config_query
from ..cleanse import apply_cleansing
from ..transform import row_hash, merge_upsert
from ..audit import append_rows, log_object_run


def _dq_condition(rule, colmap):
    col = colmap.get(snake(rule["column_name"])) if rule.get("column_name") else None
    if col is None:
        return F.lit(True)
    t = rule["rule_type"]
    if t == "not_null":
        return col.isNotNull()
    if t == "min":
        return col.isNull() | (col >= float(rule["min_value"]))
    if t == "max":
        return col.isNull() | (col <= float(rule["max_value"]))
    if t == "allowed_values":
        return col.isNull() | col.isin(json.loads(rule["allowed_values_json"]))
    if t == "expression":
        return F.expr(rule["rule_expression"])
    return F.lit(True)


def silver(run_id="manual", object_json="{}", object=None, **kw):
    """object_json: JSON string of the object config (from the planner); or pass `object` as a dict."""
    o = object if object is not None else json.loads(object_json or "{}")

    def _work():
        oid = o["object_id"]
        schema, table = landed_table(o)                        # schema-enabled: (schema, table)
        keys = [snake(k) for k in json.loads(o["key_columns_json"])]
        bp = tpath("bronze", table, schema)
        if not delta_exists(bp):
            print(f"skip {schema}.{table}: no bronze")
            return
        df = read_path(bp)
        ingest_ts = df["_bronze_ingest_ts"] if "_bronze_ingest_ts" in df.columns else F.current_timestamp()
        biz = [c for c in df.columns if not c.startswith("_")]
        sdf = df.select([F.col(c).alias(snake(c)) for c in biz] + [ingest_ts.alias("_bronze_ingest_ts")])
        if "rowguid" in sdf.columns:
            sdf = sdf.drop("rowguid")
        if all(k in sdf.columns for k in keys):
            w = Window.partitionBy(*keys).orderBy(F.col("_bronze_ingest_ts").desc())
            sdf = sdf.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
        sdf = sdf.drop("_bronze_ingest_ts")

        # cleanse (fix rows) BEFORE DQ validation (quarantine)
        crules = config_query(
            "SELECT * FROM dbo.cleanse_rule WHERE object_id=? AND is_active=1 ORDER BY apply_order", (oid,))
        sdf = apply_cleansing(sdf, crules)

        colmap = {c: F.col(c) for c in sdf.columns}
        rules = config_query("SELECT * FROM dbo.dq_rule WHERE object_id=? AND is_active=1", (oid,))
        pass_all = F.lit(True)
        for rule in rules:
            cond = _dq_condition(rule, colmap)
            failed = sdf.where(~cond).count() if rule.get("column_name") else 0
            append_rows("dq_result", [{"run_id": run_id, "object_id": oid, "rule_id": rule["rule_id"],
                                       "failed_count": failed, "passed_count": sdf.count() - failed,
                                       "status": "FAIL" if failed else "PASS", "evaluated_at": now_ts()}])
            if str(rule["severity"]).lower() == "error":
                pass_all = pass_all & cond

        good, bad = sdf.where(pass_all), sdf.where(~pass_all)
        q_cnt = bad.count()
        if q_cnt:
            write_path(bad.withColumn("_run_id", F.lit(run_id)).withColumn("_quarantined_at", F.current_timestamp()),
                       tpath(QUAR_LH, f"quarantine_{table}", schema), mode="overwrite")
        good = row_hash(good).withColumn("_silver_run_id", F.lit(run_id)) \
                             .withColumn("_silver_updated_at", F.current_timestamp())
        sp = tpath("silver", table, schema)
        if all(k in good.columns for k in keys):
            # dedup on the key set before merge so a merge never sees duplicate source keys — this
            # is also how keyless sources dedup on the full row (key_columns_json = ["_row_hash"]).
            merge_upsert(sp, good.dropDuplicates(keys), keys)
        else:
            write_path(good, sp, mode="overwrite")
        s_cnt = read_path(sp).count()
        log_object_run(run_id, oid, "silver", "SUCCEEDED", source_count=sdf.count(),
                       target_count=s_cnt, quarantine_count=q_cnt)
        print(f"silver {schema}.{table}: {s_cnt} rows, quarantined {q_cnt}")

    try:
        _work()
    except Exception:
        files_put(f"_cp_err_silver_{o.get('object_id', 'x')}_{run_id}.txt", traceback.format_exc())
        raise
