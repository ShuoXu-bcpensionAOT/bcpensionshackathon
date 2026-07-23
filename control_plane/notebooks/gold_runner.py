# PARAMETERS
run_id = "manual"
model_id = 1

# COMMAND ----------
%run cp_framework

# COMMAND ----------
workers.gold(run_id=run_id, model_id=model_id)
