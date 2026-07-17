# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Source query: silver sales territory -> gold dim_territory (SCD1). No parent.
import traceback
from pyspark.sql import functions as F


def build():
    t = read_path(tpath("silver", "adventureworks_sales_salesterritory"))
    stage = t.select(
        t["territory_id"].alias("territory_key"),
        t["territory_id"],
        t["name"].alias("territory_name"),
        t["country_region_code"],
        t["group"].alias("territory_group"))
    build_stage_and_gold("dim_territory", stage, "scd1", "dim_territory",
                         "dim_territory", ["territory_key"], run_id)


try:
    build()
except Exception:
    files_put(f"_cp_err_sq_dim_territory_{run_id}.txt", traceback.format_exc())
    raise
