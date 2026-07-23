# PARAMETERS
run_id = "manual"
file_path = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
workers.dropbox(run_id=run_id, file_path=file_path)
