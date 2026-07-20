# PARAMETERS
load_group = 1
plan_type = "objects"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Planner: read config_db (pyodbc/AAD) and return the work-list for a pipeline ForEach.
# plan_type: objects (bronze/silver) | models (gold) | steps (main) | datasets (pbi)
import json

lg = int(load_group)
if plan_type == "objects":
    rows = config_query(
        "SELECT o.object_id,o.source_schema,o.source_table,o.target_name,o.load_type,"
        "o.key_columns_json,o.watermark_column,o.source_options_json,o.suffix,"
        "d.database_name,d.source_name,d.source_type,d.connector,d.connection_json "
        "FROM dbo.source_object o JOIN dbo.datasource d ON o.source_id=d.source_id "
        "WHERE o.is_active=1 AND o.processing_state='ACTIVE' AND d.load_group=? "
        "ORDER BY o.object_id", (lg,))
elif plan_type == "models":
    rows = config_query(
        "SELECT model_id,model_name FROM dbo.model WHERE is_active=1 AND load_group=? "
        "ORDER BY model_id", (lg,))
elif plan_type == "steps":
    rows = config_query(
        "SELECT step_order,step_key,child_pipeline,is_active FROM dbo.steps "
        "WHERE load_group=? ORDER BY step_order", (lg,))
elif plan_type == "datasets":
    rows = config_query(
        "SELECT dataset_id,workspace_id,dataset_name FROM dbo.pbi_dataset "
        "WHERE is_active=1 AND load_group=?", (lg,))
else:
    rows = []

# steps -> keyed object {step_key: is_active} so the main pipeline's If can select by
# property name (Fabric expressions can't filter()/item() outside a ForEach).
if plan_type == "steps":
    out = {r["step_key"]: bool(r["is_active"]) for r in rows}
else:
    for r in rows:
        if "is_active" in r:
            r["is_active"] = bool(r["is_active"])
    out = rows
print(f"plan {plan_type} lg={lg}: {len(rows)} rows")
notebookutils.notebook.exit(json.dumps(out, default=str))
