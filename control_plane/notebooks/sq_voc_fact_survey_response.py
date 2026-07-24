# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for voc_fact_survey_response — one conformed header row per submission, unioned across the
# four surveys (missing attributes are NULL for that survey). response_key = sha2(survey|id). fact.
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql(f"""
CREATE OR REPLACE TABLE stage.voc_fact_survey_response AS
SELECT 1 AS survey_key, CAST(respondent_id AS STRING) AS source_response_id,
       sha2(concat_ws('|','1',CAST(respondent_id AS STRING)),256) AS response_key,
       CAST(plan_id AS STRING) AS plan_key, CAST(start__date AS STRING) AS survey_date,
       CAST(language_code AS STRING) AS language_code,
       CAST(NULL AS STRING) AS segment, CAST(NULL AS STRING) AS sub_segment, CAST(NULL AS STRING) AS gender,
       CAST(sample___subsegment_code__employer_size AS STRING) AS employer_size,
       CAST(NULL AS STRING) AS request_method, CAST(NULL AS STRING) AS business_event,
       CAST(NULL AS STRING) AS topic_id, CAST(NULL AS STRING) AS instructor_name, CAST(NULL AS STRING) AS online
FROM `{silver_lh}`.voc.`2026_2027_q1_employer_survey_worksheet`
UNION ALL
SELECT 2, CAST(id AS STRING), sha2(concat_ws('|','2',CAST(id AS STRING)),256),
       CAST(a4__plan AS STRING), CAST(start__date AS STRING), CAST(language_code AS STRING),
       CAST(a3__segment AS STRING), CAST(a5__sub_segment___career_code AS STRING), CAST(a7__gender AS STRING),
       CAST(NULL AS STRING), CAST(request_method AS STRING), CAST(business_event AS STRING),
       CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)
FROM `{silver_lh}`.voc.`2026_27_q1_3_2_may_interaction_survey_q2_1_1_june_interaction_survey_worksheet`
UNION ALL
SELECT 3, CAST(id AS STRING), sha2(concat_ws('|','3',CAST(id AS STRING)),256),
       CAST(plan_id AS STRING), CAST(seminar_date AS STRING), CAST(NULL AS STRING),
       CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
       CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
       CAST(topic_id AS STRING), CAST(instructor_name AS STRING), CAST(NULL AS STRING)
FROM `{silver_lh}`.voc.`2026_27_q2_c14_15_employer_workshop_worksheet`
UNION ALL
SELECT 4, CAST(id AS STRING), sha2(concat_ws('|','4',CAST(id AS STRING)),256),
       CAST(plan_id AS STRING), CAST(seminar_date AS STRING), CAST(NULL AS STRING),
       CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
       CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
       CAST(topic_id AS STRING), CAST(instructor_name AS STRING), CAST(online AS STRING)
FROM `{silver_lh}`.voc.`2026_27_q2_c51_57_part_1_member_workshop_worksheet`
""")
