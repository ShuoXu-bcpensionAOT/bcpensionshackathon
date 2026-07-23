"""Control/audit tables: run + per-object logging, watermarks, and their explicit schemas.
Appends are idempotent-ish (append-only markers); seed_control_tables pre-creates the empty
tables so parallel ForEach workers don't race to CREATE them on a fresh lakehouse."""
import json

from pyspark.sql import functions as F

from .runtime import spark, tpath
from .storage import delta_exists, read_path, write_path
from .naming import now_ts

# Explicit schemas so all-None columns (e.g. run_completed_at) don't break inference.
SCHEMAS = {
    "ingestion_run": "run_id string, run_started_at timestamp, run_completed_at timestamp, status string, details string",
    "object_load_run": ("run_id string, object_id string, layer string, status string, "
                        "source_count long, target_count long, quarantine_count long, "
                        "started_at timestamp, ended_at timestamp, details string"),
    "watermark_state": "object_id string, watermark_value string, updated_at timestamp",
    "schema_drift_event": ("event_id string, run_id string, object_id string, column_name string, "
                          "drift_type string, severity string, details string, detected_at timestamp"),
    "dq_result": ("run_id string, object_id string, rule_id string, failed_count long, "
                  "passed_count long, status string, evaluated_at timestamp"),
    "parity_result": ("run_id string, object_id string, check_scope string, check_name string, "
                      "source_value string, target_value string, status string, checked_at timestamp"),
    "pipeline_run_log": ("pipeline_name string, run_id string, load_group int, activity string, "
                         "message string, logged_at timestamp"),
    "dropbox_ledger": ("file_key string, content_hash string, schema_name string, "
                       "object_count int, status string, processed_at timestamp"),
}


def _json_safe(d):
    return {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in d.items()}


def _to_int(v):
    return int(v) if v is not None else None


def append_rows(config_table, rows):
    if not rows:
        return
    rows = [_json_safe(r) for r in rows]
    schema = SCHEMAS.get(config_table)
    if schema:
        # order dict values to the schema's column order
        cols = [c.strip().split()[0] for c in schema.split(",")]
        rows = [[r.get(c) for c in cols] for r in rows]
        df = spark.createDataFrame(rows, schema)
    else:
        df = spark.createDataFrame(rows)
    write_path(df, tpath("config", config_table), mode="append")


def seed_control_tables():
    """Pre-create the append-target control tables (empty) so concurrent ForEach workers don't
    race to CREATE them on a fresh lakehouse (Delta 'multiple writers to an empty directory')."""
    for name, schema in SCHEMAS.items():
        p = tpath("config", name)
        if not delta_exists(p):
            write_path(spark.createDataFrame([], schema), p, mode="overwrite")


def start_run(run_id, details=None):
    append_rows("ingestion_run", [{
        "run_id": run_id, "run_started_at": now_ts(), "run_completed_at": None,
        "status": "RUNNING", "details": json.dumps(details or {})}])
    return run_id


def finish_run(run_id, status="SUCCEEDED", details=None):
    # append-only completion marker (avoids UPDATE for simplicity/idempotence)
    append_rows("ingestion_run", [{
        "run_id": run_id, "run_started_at": now_ts(), "run_completed_at": now_ts(),
        "status": status, "details": json.dumps(details or {})}])


def log_object_run(run_id, object_id, layer, status, source_count=None,
                   target_count=None, quarantine_count=None, details=None):
    append_rows("object_load_run", [{
        "run_id": run_id, "object_id": object_id, "layer": layer, "status": status,
        "source_count": _to_int(source_count), "target_count": _to_int(target_count),
        "quarantine_count": _to_int(quarantine_count), "started_at": now_ts(),
        "ended_at": now_ts(), "details": json.dumps(details or {})}])


def get_watermark(object_id):
    p = tpath("config", "watermark_state")
    if not delta_exists(p):
        return None
    rows = (read_path(p).where(F.col("object_id") == object_id)
            .orderBy(F.col("updated_at").desc()).limit(1).collect())
    return rows[0]["watermark_value"] if rows else None


def update_watermark(object_id, value):
    if value is None:
        return
    append_rows("watermark_state", [{
        "object_id": object_id, "watermark_value": str(value), "updated_at": now_ts()}])
