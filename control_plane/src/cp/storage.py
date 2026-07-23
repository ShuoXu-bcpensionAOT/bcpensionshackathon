"""OneLake Delta I/O + the config-DB OneLake mirror reader + Files writes."""
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from .runtime import spark, WS_ID, LH, CONFIG_SQLDB_ID, notebookutils


def delta_exists(path):
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def read_path(path):
    return spark.read.format("delta").load(path)


def write_path(df, path, mode="overwrite"):
    w = df.write.format("delta").mode(mode)
    w = w.option("overwriteSchema", "true") if mode == "overwrite" else w.option("mergeSchema", "true")
    w.save(path)


def read_config(table):
    """Read an authored-config table from the config SQL DB's OneLake mirror.
    BIT columns mirror as boolean; normalize is_active so filters are robust."""
    df = read_path(f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/"
                   f"{CONFIG_SQLDB_ID}/Tables/dbo/{table}")
    if "is_active" in df.columns:
        df = df.withColumn("is_active", F.col("is_active").cast("boolean"))
    return df


def files_put(name, text):
    """Write a text file to the config lakehouse Files area (GUID path)."""
    p = f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH['config']}/Files/{name}"
    notebookutils.fs.put(p, text, True)
