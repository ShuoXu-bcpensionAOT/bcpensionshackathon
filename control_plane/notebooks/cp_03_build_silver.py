# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Build silver from bronze: snake_case, dedupe by business key (latest wins),
# row-hash, DQ rules (error-severity failures quarantined), and schema-drift logging.
import json
import traceback
from pyspark.sql import functions as F, Window


def dq_condition(rule, colmap):
    """Return a Spark boolean column that is True when the row PASSES the rule."""
    col = colmap.get(snake(rule["column_name"]))
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
        vals = json.loads(rule["allowed_values_json"])
        return col.isNull() | col.isin(vals)
    if t == "expression":
        return F.expr(rule["rule_expression"])
    return F.lit(True)


def detect_drift(oid, cur_cols):
    p = tpath("config", "source_column")
    prev = set()
    if delta_exists(p):
        prev = {r["column_name"] for r in read_path(p).where(F.col("object_id") == oid).collect()}
    cur = set(cur_cols)
    if prev:
        events = []
        for c in sorted(cur - prev):
            events.append({"event_id": f"{run_id}_{oid}_{c}", "run_id": run_id, "object_id": oid,
                           "column_name": c, "drift_type": "COLUMN_ADDED", "severity": "info",
                           "details": "{}", "detected_at": now_ts()})
        for c in sorted(prev - cur):
            events.append({"event_id": f"{run_id}_{oid}_{c}", "run_id": run_id, "object_id": oid,
                           "column_name": c, "drift_type": "COLUMN_REMOVED", "severity": "warning",
                           "details": "{}", "detected_at": now_ts()})
        if events:
            append_rows("schema_drift_event", events)
    snap = [{"object_id": oid, "column_name": c, "discovered_at": now_ts(), "is_active": True}
            for c in cur]
    merge_upsert(tpath("config", "source_column"),
                 spark.createDataFrame(snap), ["object_id", "column_name"])


def build_silver():
    objs = read_config("source_object").where(
        (F.col("is_active")) & (F.col("processing_state") == "ACTIVE")).collect()
    rules_all = read_config("dq_rule").where(F.col("is_active")).collect() \
        if delta_exists(tpath("config", "dq_rule")) else []

    summary = []
    for r in objs:
        oid, target = r["object_id"], r["target_name"]
        keys = [snake(k) for k in json.loads(r["key_columns_json"])]
        bp = tpath("bronze", target)
        if not delta_exists(bp):
            print(f"skip {target}: no bronze")
            continue

        df = read_path(bp)
        # snake_case business columns; keep bronze ingest ts for dedupe ordering
        ingest_ts = df["_bronze_ingest_ts"] if "_bronze_ingest_ts" in df.columns else F.current_timestamp()
        biz = [c for c in df.columns if not c.startswith("_")]
        sdf = df.select([F.col(c).alias(snake(c)) for c in biz] + [ingest_ts.alias("_bronze_ingest_ts")])
        if "rowguid" in sdf.columns:
            sdf = sdf.drop("rowguid")

        # dedupe by key: latest bronze ingest wins
        if all(k in sdf.columns for k in keys):
            w = Window.partitionBy(*keys).orderBy(F.col("_bronze_ingest_ts").desc())
            sdf = sdf.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
        sdf = sdf.drop("_bronze_ingest_ts")

        colmap = {c: F.col(c) for c in sdf.columns}
        # DQ: quarantine rows failing any ERROR rule; count all rules
        obj_rules = [rr for rr in rules_all if rr["object_id"] == oid]
        pass_all = F.lit(True)
        for rule in obj_rules:
            cond = dq_condition(rule, colmap)
            failed = sdf.where(~cond).count() if rule["column_name"] else 0
            append_rows("dq_result", [{"run_id": run_id, "object_id": oid, "rule_id": rule["rule_id"],
                                       "failed_count": failed, "passed_count": sdf.count() - failed,
                                       "status": "FAIL" if failed else "PASS", "evaluated_at": now_ts()}])
            if str(rule["severity"]).lower() == "error":
                pass_all = pass_all & cond

        good = sdf.where(pass_all)
        bad = sdf.where(~pass_all)
        q_cnt = bad.count()
        if q_cnt:
            qrows = bad.withColumn("_run_id", F.lit(run_id)).withColumn("_quarantined_at", F.current_timestamp())
            write_path(qrows, tpath(QUAR_LH, f"quarantine_{target}"), mode="overwrite")

        good = row_hash(good)
        good = good.withColumn("_silver_run_id", F.lit(run_id)) \
                   .withColumn("_silver_updated_at", F.current_timestamp())

        if all(k in good.columns for k in keys):
            merge_upsert(tpath("silver", target), good, keys)
        else:
            write_path(good, tpath("silver", target), mode="overwrite")

        detect_drift(oid, [c for c in good.columns if not c.startswith("_")])
        s_cnt = read_path(tpath("silver", target)).count()
        log_object_run(run_id, oid, "silver", "SUCCEEDED",
                       source_count=sdf.count(), target_count=s_cnt, quarantine_count=q_cnt)
        summary.append((target, s_cnt, q_cnt))
        print(f"silver {target}: {s_cnt} rows, quarantined {q_cnt}")

    print("=== silver summary ===")
    for t, s, q in summary:
        print(f"  {t:<48} silver={s:>8}  quarantine={q}")


try:
    build_silver()
except Exception:
    files_put(f"_cp_err_silver_{run_id}.txt", traceback.format_exc())
    raise
