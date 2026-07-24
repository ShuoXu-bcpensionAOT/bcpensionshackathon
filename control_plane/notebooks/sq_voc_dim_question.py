# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for voc_dim_question — the codebook, derived from the gold answer fact: one row per
# (survey, question code) with an inferred question_type. question_text is a slot for the survey
# design docs. Depends on voc_fact_survey_answer. scd1.
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql("""
CREATE OR REPLACE TABLE stage.voc_dim_question AS
SELECT question_key, survey_key, question_code,
       CAST(NULL AS STRING) AS question_text,
       CASE WHEN MAX(LENGTH(answer_text)) > 25 THEN 'free_text'
            WHEN MIN(CASE WHEN answer_text RLIKE '^-?[0-9]+([.][0-9]+)?$' THEN 1 ELSE 0 END) = 1 THEN 'likert'
            ELSE 'choice' END AS question_type,
       COUNT(DISTINCT answer_text) AS distinct_answers,
       CAST(false AS BOOLEAN) AS is_kpi
FROM dbo.voc_fact_survey_answer
GROUP BY question_key, survey_key, question_code
""")
