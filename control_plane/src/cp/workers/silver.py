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
        keys = [snake(k) for k in json.loads(o.get("key_columns_json") or "[]")]
        try:
            opts = json.loads(o.get("source_options_json") or "{}") or {}
        except (ValueError, TypeError):
            opts = {}
        load_type = str(o.get("load_type", "full")).lower()
        has_key = bool(keys) and keys != ["_row_hash"]
        # Snapshot loads (full, or file/append where each batch is a COMPLETE image of the source)
        # let us reason about deletes; an incremental (watermark) delta only carries changed rows and
        # can't reveal a row removed at source, so delete-detection never applies there.
        is_snapshot = load_type != "incremental"
        dd = str(opts.get("delete_detection", "")).lower()
        # Auto-on: a KEYED SNAPSHOT load flags business keys that vanish between snapshots as
        # soft-deletes (kept with _is_deleted / _deleted_at, latest values retained; a reappearing
        # key flips back). Opt out per object with source_options_json.delete_detection = "off".
        soft_delete = has_key and is_snapshot and dd != "off"
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
        # bronze is append-only (every load retained for audit) — isolate the LATEST snapshot.
        latest_ts = sdf.agg(F.max("_bronze_ingest_ts")).first()[0] if is_snapshot else None
        current_keys_df, max_ts = None, None
        if soft_delete and all(k in sdf.columns for k in keys):
            max_ts = latest_ts                                 # keys in the newest snapshot = alive
            current_keys_df = sdf.where(F.col("_bronze_ingest_ts") == F.lit(max_ts)) \
                                 .select(*keys).distinct()
        if has_key and all(k in sdf.columns for k in keys):
            # keyed: keep the latest value per key across ALL retained snapshots.
            w = Window.partitionBy(*keys).orderBy(F.col("_bronze_ingest_ts").desc())
            sdf = sdf.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
        elif is_snapshot and latest_ts is not None:
            # keyless snapshot: silver mirrors the LATEST snapshot only (row-hash de-duped below);
            # earlier appended snapshots are audit history in bronze, not current state.
            sdf = sdf.where(F.col("_bronze_ingest_ts") == F.lit(latest_ts))
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
        # Flag soft-deletes: keys not in the latest snapshot are gone at source. Row is retained
        # (last known values) with _is_deleted=true; a key that re-appears later flips back to false.
        del_cnt = 0
        if current_keys_df is not None:
            good = good.join(current_keys_df.withColumn("_present", F.lit(True)), keys, "left")
            good = good.withColumn("_is_deleted", F.col("_present").isNull()) \
                       .withColumn("_deleted_at",
                                   F.when(F.col("_present").isNull(), F.lit(max_ts))
                                    .otherwise(F.lit(None).cast("timestamp"))) \
                       .drop("_present")
            del_cnt = good.where(F.col("_is_deleted")).count()
        sp = tpath("silver", table, schema)
        if has_key and all(k in good.columns for k in keys):
            # keyed: upsert the latest-per-key set (+ soft-delete flags). Dedup on the key first so
            # the merge never sees duplicate source keys.
            merge_upsert(sp, good.dropDuplicates(keys), keys)
        else:
            # keyless: silver = the latest snapshot, row-hash de-duplicated (latest drop/load wins).
            dedup_key = keys if (keys and all(k in good.columns for k in keys)) else ["_row_hash"]
            write_path(good.dropDuplicates(dedup_key), sp, mode="overwrite")
        s_cnt = read_path(sp).count()
        log_object_run(run_id, oid, "silver", "SUCCEEDED", source_count=sdf.count(),
                       target_count=s_cnt, quarantine_count=q_cnt)
        tail = f", deleted {del_cnt}" if current_keys_df is not None else ""
        print(f"silver {schema}.{table}: {s_cnt} rows, quarantined {q_cnt}{tail}")

    try:
        _work()
    except Exception:
        files_put(f"_cp_err_silver_{o.get('object_id', 'x')}_{run_id}.txt", traceback.format_exc())
        raise
