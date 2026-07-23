"""Row-level transform helpers: business-column selection, row hashing, and Delta upsert."""
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from .runtime import spark, CONTROL_COLS
from .storage import delta_exists, write_path


def business_cols(df):
    return [c for c in df.columns if c not in CONTROL_COLS and not c.startswith("_")]


def row_hash(df, cols=None, out="_row_hash"):
    cols = cols or business_cols(df)
    if not cols:
        return df.withColumn(out, F.sha2(F.lit(""), 256))
    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in cols]
    return df.withColumn(out, F.sha2(F.concat_ws("||", *exprs), 256))


def merge_upsert(target_path, source_df, keys):
    if not delta_exists(target_path):
        write_path(source_df, target_path, mode="overwrite")
        return
    tgt = DeltaTable.forPath(spark, target_path)
    cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
    (tgt.alias("t").merge(source_df.alias("s"), cond)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
