"""Fabric SQL Database access for the authored config tables.

Local tooling connects via pyodbc + AAD token. The Spark engine reads the same
tables from the SQL DB's OneLake mirror (see cp_framework). Runtime state/logs
stay in the lakehouse.
"""
import struct
import sys

import pyodbc
import requests

import cp_common as C

API = "https://api.fabric.microsoft.com/v1"
SQL_COPT_SS_ACCESS_TOKEN = 1256
CONFIG_DB_NAME = "config_db"

# Ordered DDL (parents before children for FK creation).
DDL = [
    ("datasource", """CREATE TABLE dbo.datasource(
        source_id INT IDENTITY(1,1) PRIMARY KEY, source_name NVARCHAR(128), source_type NVARCHAR(50),
        database_name NVARCHAR(128), load_group INT, ingestion_mode NVARCHAR(50), is_active BIT,
        connector NVARCHAR(50), connection_json NVARCHAR(MAX), secret_name NVARCHAR(256))"""),
    ("model", """CREATE TABLE dbo.model(
        model_id INT PRIMARY KEY, model_name NVARCHAR(128), load_group INT, is_active BIT)"""),
    ("source_object", """CREATE TABLE dbo.source_object(
        object_id NVARCHAR(128) PRIMARY KEY,
        source_id INT REFERENCES dbo.datasource(source_id),
        source_schema NVARCHAR(128), source_table NVARCHAR(128), target_name NVARCHAR(256),
        load_type NVARCHAR(50), key_columns_json NVARCHAR(MAX), watermark_column NVARCHAR(128),
        watermark_type NVARCHAR(50), is_active BIT, processing_state NVARCHAR(50),
        source_options_json NVARCHAR(MAX), suffix NVARCHAR(128))"""),
    ("dq_rule", """CREATE TABLE dbo.dq_rule(
        rule_id NVARCHAR(128) PRIMARY KEY,
        object_id NVARCHAR(128) REFERENCES dbo.source_object(object_id),
        column_name NVARCHAR(128), rule_type NVARCHAR(50), allowed_values_json NVARCHAR(MAX),
        min_value FLOAT, max_value FLOAT, rule_expression NVARCHAR(MAX),
        severity NVARCHAR(50), is_active BIT)"""),
    ("cleanse_rule", """CREATE TABLE dbo.cleanse_rule(
        rule_id NVARCHAR(128) PRIMARY KEY,
        object_id NVARCHAR(128) REFERENCES dbo.source_object(object_id),
        [function] NVARCHAR(50), [columns] NVARCHAR(MAX), parameters_json NVARCHAR(MAX),
        apply_order INT, is_active BIT)"""),
    ("gold_object", """CREATE TABLE dbo.gold_object(
        gold_object_id NVARCHAR(128) PRIMARY KEY,
        model_id INT REFERENCES dbo.model(model_id),
        gold_type NVARCHAR(50), stage_table NVARCHAR(128), gold_table NVARCHAR(128),
        business_key_columns_json NVARCHAR(MAX), source_query_notebook NVARCHAR(128), is_active BIT)"""),
    ("gold_dependency", """CREATE TABLE dbo.gold_dependency(
        parent_gold_object_id NVARCHAR(128) REFERENCES dbo.gold_object(gold_object_id),
        child_gold_object_id NVARCHAR(128) REFERENCES dbo.gold_object(gold_object_id),
        PRIMARY KEY(parent_gold_object_id, child_gold_object_id))"""),
    ("steps", """CREATE TABLE dbo.steps(
        load_group INT, step_order INT, step_key NVARCHAR(50), child_pipeline NVARCHAR(128),
        is_active BIT, PRIMARY KEY(load_group, step_key))"""),
    ("pbi_dataset", """CREATE TABLE dbo.pbi_dataset(
        dataset_id NVARCHAR(128) PRIMARY KEY, load_group INT, workspace_id NVARCHAR(128),
        dataset_name NVARCHAR(256), is_active BIT)"""),
]
LOAD_ORDER = [name for name, _ in DDL]
COLUMNS = {
    "datasource": ["source_id", "source_name", "source_type", "database_name",
                   "load_group", "ingestion_mode", "is_active", "connector", "connection_json",
                   "secret_name"],
    "model": ["model_id", "model_name", "load_group", "is_active"],
    "source_object": ["object_id", "source_id", "source_schema", "source_table", "target_name",
                      "load_type", "key_columns_json", "watermark_column", "watermark_type",
                      "is_active", "processing_state", "source_options_json", "suffix"],
    "dq_rule": ["rule_id", "object_id", "column_name", "rule_type", "allowed_values_json",
                "min_value", "max_value", "rule_expression", "severity", "is_active"],
    "cleanse_rule": ["rule_id", "object_id", "function", "columns", "parameters_json",
                     "apply_order", "is_active"],
    "gold_object": ["gold_object_id", "model_id", "gold_type", "stage_table", "gold_table",
                    "business_key_columns_json", "source_query_notebook", "is_active"],
    "gold_dependency": ["parent_gold_object_id", "child_gold_object_id"],
    "steps": ["load_group", "step_order", "step_key", "child_pipeline", "is_active"],
    "pbi_dataset": ["dataset_id", "load_group", "workspace_id", "dataset_name", "is_active"],
}
BOOL_COLS = {"is_active"}
# Tables whose PK is an IDENTITY column — cp_config wraps their load in SET IDENTITY_INSERT so the
# ids captured in YAML replay identically across environments (deterministic promotion).
IDENTITY_TABLES = {"datasource"}
# Deterministic export order (stable git diffs) — the primary key of each table.
ORDER_BY = {
    "datasource": "source_id",
    "model": "model_id",
    "source_object": "object_id",
    "dq_rule": "rule_id",
    "cleanse_rule": "object_id, apply_order",
    "gold_object": "gold_object_id",
    "gold_dependency": "parent_gold_object_id, child_gold_object_id",
    "steps": "load_group, step_order",
    "pbi_dataset": "dataset_id",
}


def props():
    """Return (item_id, serverFqdn, databaseName) of the config SQL DB."""
    tok = C.fabric_token()
    h = {"Authorization": f"Bearer {tok}"}
    for d in requests.get(f"{API}/workspaces/{C.WS_ID}/SqlDatabases", headers=h).json().get("value", []):
        if d["displayName"] == CONFIG_DB_NAME:
            p = requests.get(f"{API}/workspaces/{C.WS_ID}/SqlDatabases/{d['id']}", headers=h).json()["properties"]
            return d["id"], p["serverFqdn"], p["databaseName"]
    sys.exit(f"{CONFIG_DB_NAME} not found in workspace {C.WS_NAME}")


def connect(retries=4):
    """Connect to the config SQL DB. A Fabric SQL Database auto-pauses when idle, so the
    first connect after a while must resume it — use a long login timeout and retry."""
    import time
    _id, server, database = props()
    token = C._token("https://database.windows.net/").encode("utf-16-le")
    ts = struct.pack(f"<I{len(token)}s", len(token), token)
    host = server.split(",")[0]
    cs = (f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host};"
          f"DATABASE={database};Encrypt=yes;Connection Timeout=90")
    last = None
    for a in range(retries):
        try:
            return pyodbc.connect(cs, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: ts}, timeout=90)
        except pyodbc.Error as e:
            last = e
            # transient: login timeout (08001/258) or DB resuming from pause (40613)
            if any(s in str(e) for s in ("08001", "timeout", "Timeout", "258",
                                         "40613", "not currently available")):
                time.sleep(20)
                continue
            raise
    raise last


# Additive column migrations — applied to config DBs created before these columns existed.
MIGRATIONS = [
    ("datasource", "connector", "NVARCHAR(50)"),
    ("datasource", "connection_json", "NVARCHAR(MAX)"),
    ("datasource", "secret_name", "NVARCHAR(256)"),
    ("source_object", "source_options_json", "NVARCHAR(MAX)"),
    ("source_object", "suffix", "NVARCHAR(128)"),
]


def _migrate_datasource_identity(cur):
    """Convert datasource.source_id to IDENTITY on config DBs created before it was one.
    SQL Server can't ALTER a column to IDENTITY, so rebuild the (tiny) table, preserving the
    existing ids (via IDENTITY_INSERT) and the source_object FK."""
    if not cur.execute("SELECT OBJECT_ID('dbo.datasource','U')").fetchval():
        return                                          # fresh DB -> CREATE already uses IDENTITY
    if cur.execute("SELECT COLUMNPROPERTY(OBJECT_ID('dbo.datasource'),'source_id','IsIdentity')").fetchval() == 1:
        return                                          # already identity
    cols = ",".join(f"[{c}]" for c in COLUMNS["datasource"])
    fks = [r[0] for r in cur.execute(
        "SELECT name FROM sys.foreign_keys WHERE referenced_object_id=OBJECT_ID('dbo.datasource')").fetchall()]
    cur.execute("SELECT * INTO #ds_bak FROM dbo.datasource")
    for fk in fks:                                      # only source_object references datasource
        cur.execute(f"ALTER TABLE dbo.source_object DROP CONSTRAINT [{fk}]")
    cur.execute("DROP TABLE dbo.datasource")
    cur.execute(dict(DDL)["datasource"])                # recreate with IDENTITY
    cur.execute("SET IDENTITY_INSERT dbo.datasource ON")
    cur.execute(f"INSERT INTO dbo.datasource ({cols}) SELECT {cols} FROM #ds_bak")
    cur.execute("SET IDENTITY_INSERT dbo.datasource OFF")
    cur.execute("DROP TABLE #ds_bak")
    if fks:
        cur.execute("ALTER TABLE dbo.source_object ADD CONSTRAINT FK_source_object_datasource "
                    "FOREIGN KEY(source_id) REFERENCES dbo.datasource(source_id)")
    print("  migrated datasource.source_id -> IDENTITY")


def ensure_schema(cn):
    cur = cn.cursor()
    for name, ddl in DDL:
        if not cur.execute(f"SELECT OBJECT_ID('dbo.{name}','U')").fetchval():
            cur.execute(ddl)
    for tbl, col, coltype in MIGRATIONS:              # add columns to pre-existing tables
        if not cur.execute(f"SELECT COL_LENGTH('dbo.{tbl}','{col}')").fetchval():
            cur.execute(f"ALTER TABLE dbo.{tbl} ADD [{col}] {coltype}")
    _migrate_datasource_identity(cur)                 # source_id -> IDENTITY (rebuild if needed)
    cn.commit()
