# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for voc_fact_survey_answer — the atomic grain: one answer per question, unpivoted from the
# four wide surveys (question columns = business columns minus each survey's metadata). answer_key
# = sha2(response_key|question_code); question_key = sha2(survey_key|question_code). fact.
from pyspark.sql import functions as F

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

long = None
for s in SURVEYS:
    df = spark.sql(f"SELECT * FROM `{silver_lh}`.voc.`{s['t']}`")
    qcols = [c for c in df.columns if not c.startswith("_") and c not in s["meta"]]
    df = df.withColumn("_rk", F.sha2(F.concat_ws("|", F.lit(str(s["k"])), F.col(s["rid"]).cast("string")), 256))
    pairs = ", ".join(f"'{c}', `{c}`" for c in qcols)
    u = df.selectExpr(f"{s['k']} AS survey_key", "_rk AS response_key",
                      f"stack({len(qcols)}, {pairs}) AS (question_code, answer_text)").where("answer_text IS NOT NULL")
    long = u if long is None else long.unionByName(u)

_NUM = r"^-?[0-9]+([.][0-9]+)?$"
stage = long.select(
    F.sha2(F.concat_ws("|", F.col("response_key"), F.col("question_code")), 256).alias("answer_key"),
    F.col("response_key"), F.col("survey_key"), F.col("question_code"),
    F.sha2(F.concat_ws("|", F.col("survey_key").cast("string"), F.col("question_code")), 256).alias("question_key"),
    F.col("answer_text"),
    F.when(F.col("answer_text").rlike(_NUM), F.col("answer_text").cast("double")).alias("answer_numeric"),
    F.when(F.length("answer_text") > 25, True).otherwise(False).alias("is_freetext"))

spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
stage.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("stage.voc_fact_survey_answer")
