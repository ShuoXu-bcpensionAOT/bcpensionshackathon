# PARAMETERS
run_id = "manual"
load_group = 1
src_user = ""
src_password = ""

# COMMAND ----------
%run cp_framework

# COMMAND ----------
workers.metadata(run_id=run_id, load_group=load_group, src_user=src_user, src_password=src_password)
