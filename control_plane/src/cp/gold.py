"""Reusable gold writers (called by the source-query notebooks): SCD1/SCD2/fact merge + the
standard stage-then-gold epilogue."""
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from .runtime import spark, STAGE_LH, tpath
from .storage import delta_exists, read_path, write_path
from .transform import row_hash, merge_upsert
from .audit import log_object_run


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
    tgt = DeltaTable.forPath(spark, path)
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
