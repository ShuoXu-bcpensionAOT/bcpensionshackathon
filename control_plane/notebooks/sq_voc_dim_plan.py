# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for voc_dim_plan — distinct pension-plan codes across the surveys. plan_name is a slot to
# fill from the plan reference. scd1.
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql(f"""
CREATE OR REPLACE TABLE stage.voc_dim_plan AS
SELECT DISTINCT plan_key, plan_key AS plan_id, CAST(NULL AS STRING) AS plan_name
FROM (
  SELECT CAST(plan_id  AS STRING) plan_key FROM `{silver_lh}`.voc.`2026_2027_q1_employer_survey_worksheet` WHERE plan_id IS NOT NULL
  UNION SELECT CAST(a4__plan AS STRING) FROM `{silver_lh}`.voc.`2026_27_q1_3_2_may_interaction_survey_q2_1_1_june_interaction_survey_worksheet` WHERE a4__plan IS NOT NULL
  UNION SELECT CAST(plan_id  AS STRING) FROM `{silver_lh}`.voc.`2026_27_q2_c14_15_employer_workshop_worksheet` WHERE plan_id IS NOT NULL
  UNION SELECT CAST(plan_id  AS STRING) FROM `{silver_lh}`.voc.`2026_27_q2_c51_57_part_1_member_workshop_worksheet` WHERE plan_id IS NOT NULL
)
""")
