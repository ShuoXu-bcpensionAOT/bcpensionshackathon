# PARAMETERS
load_group = 1
plan_type = "objects"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
workers.plan(load_group=load_group, plan_type=plan_type)
