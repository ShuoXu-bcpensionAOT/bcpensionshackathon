# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Source query: silver subcategory + gold dim_category -> gold dim_subcategory (SCD1).
# Depends on dim_category (reads its gold table for the category surrogate key).
import traceback
from pyspark.sql import functions as F


def build():
    sc = read_path(tpath("silver", "production_productsubcategory", "adventureworks"))
    dc = read_path(tpath("gold", "dim_category")).select("category_key", "category_id", "category_name")
    stage = (sc.join(dc, sc["product_category_id"] == dc["category_id"], "left")
             .select(
                 sc["product_subcategory_id"].alias("subcategory_key"),
                 sc["product_subcategory_id"].alias("subcategory_id"),
                 sc["name"].alias("subcategory_name"),
                 dc["category_key"], dc["category_name"]))
    build_stage_and_gold("dim_subcategory", stage, "scd1", "dim_subcategory",
                         "dim_subcategory", ["subcategory_key"], run_id)


try:
    build()
except Exception:
    files_put(f"_cp_err_sq_dim_subcategory_{run_id}.txt", traceback.format_exc())
    raise
