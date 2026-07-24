# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for voc_dim_survey — the four survey instruments (static). scd1.
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql("""
CREATE OR REPLACE TABLE stage.voc_dim_survey AS
SELECT * FROM VALUES
  (1, 'Employer Survey',    'Employer', 'Relationship'),
  (2, 'Interaction Survey', 'Member',   'Interaction'),
  (3, 'Employer Workshop',  'Employer', 'Workshop'),
  (4, 'Member Workshop',    'Member',   'Workshop')
AS t(survey_key, survey_name, audience, survey_type)
""")
