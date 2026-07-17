# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Source query: silver product + gold dim_subcategory -> gold dim_product (SCD1).
# Depends on dim_subcategory (denormalizes subcategory + category onto the product dim).
import traceback
from pyspark.sql import functions as F


def build():
    p = read_path(tpath("silver", "adventureworks_production_product"))
    ds = read_path(tpath("gold", "dim_subcategory")).select(
        "subcategory_key", "subcategory_id", "subcategory_name", "category_name")
    stage = (p.join(ds, p["product_subcategory_id"] == ds["subcategory_id"], "left")
             .select(
                 p["product_id"].alias("product_key"),
                 p["product_id"],
                 p["name"].alias("product_name"),
                 p["product_number"], p["color"], p["standard_cost"], p["list_price"],
                 ds["subcategory_key"], ds["subcategory_name"], ds["category_name"]))
    build_stage_and_gold("dim_product", stage, "scd1", "dim_product",
                         "dim_product", ["product_key"], run_id)


try:
    build()
except Exception:
    files_put(f"_cp_err_sq_dim_product_{run_id}.txt", traceback.format_exc())
    raise
