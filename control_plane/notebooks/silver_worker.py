# PARAMETERS
run_id = "manual"
object_json = "{}"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
workers.silver(run_id=run_id, object_json=object_json)
