"""Runtime self-configuration: the Spark session, notebookutils, the cp_vars Variable Library,
resolved lakehouse GUIDs, workspace id, and the config SQL DB id. Imported by every other
module. NO hardcoded IDs — everything resolves from the running Fabric context.

In a notebook (%run bundle or wheel) `spark`/`notebookutils` come from the live session; off
cluster these imports fail, which is expected — only the pure modules (naming, dag) import
cleanly off cluster.
"""
import notebookutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:
    _VL = notebookutils.variableLibrary.getLibrary("cp_vars")
except Exception:
    _VL = None


def var(name, default=None):
    return getattr(_VL, name, default) if _VL is not None else default


LAYER_NAMES = {
    "config": var("config_lakehouse", "metadata"),
    "bronze": var("bronze_lakehouse", "bronze"),
    "silver": var("silver_lakehouse", "silver"),
    "gold":   var("gold_lakehouse", "gold"),
}
SOURCE_SERVER = var("source_server", None)
SOURCE_CONNECTION = var("source_connection", "")
KEY_VAULT_URL = var("key_vault_url", None)

WS_ID = notebookutils.runtime.context["currentWorkspaceId"]
_lh_by_name = {l["displayName"]: l["id"] for l in notebookutils.lakehouse.list()}
LH = {logical: _lh_by_name[name] for logical, name in LAYER_NAMES.items()}
STAGE_LH, QUAR_LH = LH["gold"], LH["silver"]  # stage_/quarantine_ prefixed tables

# Authored config lives in a Fabric SQL Database (users edit it via T-SQL). The engine
# reads it from the SQL DB's OneLake mirror (Delta). Runtime state stays in the lakehouse.
CONFIG_DB_NAME = "config_db"

CONTROL_COLS = {
    "_run_id", "_source_system", "_source_table", "_bronze_ingest_ts",
    "_silver_run_id", "_silver_updated_at", "_row_hash", "_is_current",
    "_effective_start_ts", "_effective_end_ts", "_gold_run_id", "_gold_updated_at",
    "_ingested_at",
}


def _fabric_api_token():
    import requests  # noqa: F401
    for aud in ("pbi", "https://api.fabric.microsoft.com", "https://analysis.windows.net/powerbi/api"):
        try:
            return notebookutils.credentials.getToken(aud)
        except Exception:
            continue
    return None


def _resolve_config_sqldb():
    import requests
    tk = _fabric_api_token()
    r = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/items?type=SQLDatabase",
                     headers={"Authorization": f"Bearer {tk}"})
    for i in r.json().get("value", []):
        if i["displayName"] == CONFIG_DB_NAME:
            return i["id"]
    raise Exception(f"{CONFIG_DB_NAME} SQL Database not found in workspace {WS_ID}")


CONFIG_SQLDB_ID = _resolve_config_sqldb()


def tpath(lh_key_or_guid, table, schema="dbo"):
    # Schema-enabled lakehouses store tables at Tables/<schema>/<table>. Control/audit tables
    # default to the `dbo` schema; bronze/silver land under their datasource schema.
    guid = LH.get(lh_key_or_guid, lh_key_or_guid)
    return f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{guid}/Tables/{schema}/{table}"
