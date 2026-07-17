# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Source query: aggregate gold fact_sales_order by territory -> gold fact_sales_by_territory.
# Depends on fact_sales_order (fact-on-fact aggregate) + dim_territory for the label.
import traceback
from pyspark.sql import functions as F


def build():
    f = read_path(tpath("gold", "fact_sales_order"))
    dt = read_path(tpath("gold", "dim_territory")).select("territory_key", "territory_name")
    agg = f.groupBy("territory_key").agg(
        F.sum("line_total").alias("total_sales"),
        F.sum("order_qty").alias("total_qty"),
        F.count(F.lit(1)).alias("order_lines"))
    stage = (agg.join(dt, "territory_key", "left")
             .select("territory_key", "territory_name", "total_sales", "total_qty", "order_lines"))
    build_stage_and_gold("fact_sales_by_territory", stage, "fact", "fact_sales_by_territory",
                         "fact_sales_by_territory", ["territory_key"], run_id)


try:
    build()
except Exception:
    files_put(f"_cp_err_sq_fact_sales_by_territory_{run_id}.txt", traceback.format_exc())
    raise
