# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Start the ingestion run and confirm the config-as-code control tables are present.
import traceback
try:
    start_run(run_id, {"engine": "fabric-control-plane"})
    for t in ["datasource", "source_object", "dq_rule", "model", "gold_object", "gold_dependency"]:
        print(f"{t:<16} {read_config(t).count()} rows")
    print("setup complete · run_id =", run_id)
except Exception as e:
    files_put(f"_cp_err_setup_{run_id}.txt", traceback.format_exc())
    raise
