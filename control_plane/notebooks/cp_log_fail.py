# PARAMETERS
pipeline_name = "?"
run_id = "?"
load_group = 0
activity = "?"
message = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Pipeline failure logger: writes one row to metadata lakehouse `pipeline_run_log`.
# Invoked on the Failed path of each pipeline's work activity.
try:
    lg = int(load_group)
except Exception:
    lg = None
append_rows("pipeline_run_log", [{
    "pipeline_name": pipeline_name, "run_id": run_id, "load_group": lg,
    "activity": activity, "message": str(message)[:4000], "logged_at": now_ts()}])
print(f"logged failure: {pipeline_name} / {activity}")
