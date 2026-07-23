# PARAMETERS
run_id = "manual"
object_json = "{}"
src_user = ""
src_password = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
workers.bronze(run_id=run_id, object_json=object_json, src_user=src_user, src_password=src_password)
