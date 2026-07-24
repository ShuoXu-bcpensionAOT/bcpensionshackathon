"""Type-2 dimension: track history — close the current row when its row-hash changes and insert
the new version, stamping effective-from/-to and _is_current."""
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from . import gold_strategy
from ..runtime import spark
from ..storage import delta_exists, write_path
from ..transform import row_hash


@gold_strategy("scd2")
def scd2(path, stage, keys):
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
        ec = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
        (tgt.alias("t").merge(changed.alias("s"), ec)
            .whenMatchedUpdate(set={"_is_current": F.lit(False),
                                    "_effective_end_ts": F.current_timestamp()}).execute())
    cur_keys = tgt.toDF().where(F.col("_is_current")).select(*keys)
    to_insert = incoming.join(cur_keys, keys, "left_anti")
    if to_insert.count():
        write_path(to_insert, path, "append")
