# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Source query: silver product category -> stage_dim_category -> gold dim_category (SCD1).
import traceback
from pyspark.sql import functions as F


def build():
    c = read_path(tpath("silver", "production_productcategory", "adventureworks"))
    stage = c.select(
        c["product_category_id"].alias("category_key"),
        c["product_category_id"].alias("category_id"),
        c["name"].alias("category_name"))
    build_stage_and_gold("dim_category", stage, "scd1", "dim_category",
                         "dim_category", ["category_key"], run_id)


try:
    build()
except Exception:
    files_put(f"_cp_err_sq_dim_category_{run_id}.txt", traceback.format_exc())
    raise
