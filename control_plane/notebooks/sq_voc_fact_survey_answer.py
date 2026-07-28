# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"
config_lh = "LH_metadata"

# COMMAND ----------
# Stage for voc_fact_survey_answer — the atomic grain: one answer per question, unpivoted from the
# four wide surveys (question columns = business columns minus each survey's metadata). answer_key
# = sha2(response_key|question_code); question_key = sha2(survey_key|question_code). fact.
#
# The transform is written as SparkSQL: a single `sqltext` string is assembled, run with spark.sql,
# then written. Only the per-survey column lists are computed in Python — the set of question columns
# and their ORIGINAL headers ('A2:2', 'B1-1') vary per survey and come from the global column_map, so
# the SELECT/stack() has to be generated. Everything downstream of `long` is plain SQL.

SURVEYS = [
    {"t": "2026_2027_q1_employer_survey_worksheet", "k": 1, "rid": "respondent_id",
     "meta": ["respondent_id", "plan_id", "quarter", "end__date", "end__time", "id", "language_code",
              "start__date", "start__time", "survey_version", "sample___plan",
              "sample___subsegment_code__employer_size", "year", "quarterb"]},
    {"t": "2026_27_q1_3_2_may_interaction_survey_q2_1_1_june_interaction_survey_worksheet", "k": 2, "rid": "id",
     "meta": ["end__date", "end__time", "id", "language_code", "start__date", "start__time", "survey_version",
              "a3__segment", "a4__plan", "a5__sub_segment___career_code", "a6__service__codes", "a7__gender",
              "wave", "request_method", "business_event"]},
    {"t": "2026_27_q2_c14_15_employer_workshop_worksheet", "k": 3, "rid": "id",
     "meta": ["id", "survey_date", "survey_time", "survey_start_date", "survey_start_time", "survey_version",
              "seminar_date", "topic_id", "location", "plan_id", "complete_by_date", "instructor_name",
              "registration_code"]},
    {"t": "2026_27_q2_c51_57_part_1_member_workshop_worksheet", "k": 4, "rid": "id",
     "meta": ["id", "survey_date", "survey_time", "survey_start_date", "survey_start_time", "survey_version",
              "seminar_date", "topic_id", "location", "plan_id", "complete_by_date", "instructor_name",
              "online", "special_event"]},
]


def _colmap(table):
    """physical -> ORIGINAL business header (e.g. 'a2_2' -> 'A2:2'), from the global column_map.
    Only CHANGED columns are stored, so unlisted columns keep their name."""
    try:
        rows = spark.sql(f"SELECT physical_col, original_col FROM `{config_lh}`.dbo.column_map "
                         f"WHERE landed_table = '{table}'").collect()
        return {r["physical_col"]: r["original_col"] for r in rows}
    except Exception:
        return {}


def _unpivot_sql(s):
    """One survey's wide->long unpivot as a SparkSQL SELECT. stack() emits (question_code, answer_text)
    per question column, and _colmap restores each column's ':'/'-' business header as the code."""
    cols = spark.sql(f"SELECT * FROM `{silver_lh}`.voc.`{s['t']}` LIMIT 0").columns
    qcols = [c for c in cols if not c.startswith("_") and c not in s["meta"]]
    cm = _colmap(s["t"])
    pairs = ", ".join(f"'{cm.get(c, c)}', `{c}`" for c in qcols)
    return f"""
    SELECT {s['k']} AS survey_key,
           sha2(concat_ws('|', '{s['k']}', cast(`{s['rid']}` AS string)), 256) AS response_key,
           stack({len(qcols)}, {pairs}) AS (question_code, answer_text)
    FROM `{silver_lh}`.voc.`{s['t']}`"""


union_sql = "\n    UNION ALL".join(_unpivot_sql(s) for s in SURVEYS)

sqltext = f"""
WITH long AS ({union_sql}
)
SELECT
    sha2(concat_ws('|', response_key, question_code), 256)               AS answer_key,
    response_key,
    survey_key,
    question_code,
    sha2(concat_ws('|', cast(survey_key AS string), question_code), 256) AS question_key,
    answer_text,
    CASE WHEN answer_text RLIKE '^-?[0-9]+([.][0-9]+)?$'
         THEN cast(answer_text AS double) END                           AS answer_numeric,
    CASE WHEN length(answer_text) > 25 THEN true ELSE false END          AS is_freetext
FROM long
WHERE answer_text IS NOT NULL
"""

stage = spark.sql(sqltext)
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
stage.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("stage.voc_fact_survey_answer")
