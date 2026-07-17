# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Source query: silver customer + person + gold dim_territory -> gold dim_customer (SCD2).
# Depends on dim_territory (attaches the territory surrogate key). History-tracked.
import traceback
from pyspark.sql import functions as F


def build():
    cu = read_path(tpath("silver", "adventureworks_sales_customer"))
    pe = read_path(tpath("silver", "adventureworks_person_person"))
    dt = read_path(tpath("gold", "dim_territory")).select("territory_key", "territory_id")
    stage = (cu.join(pe, cu["person_id"] == pe["business_entity_id"], "left")
             .join(dt, cu["territory_id"] == dt["territory_id"], "left")
             .select(
                 cu["customer_id"].alias("customer_key"),
                 cu["customer_id"], cu["store_id"],
                 dt["territory_key"],
                 pe["person_type"], pe["first_name"], pe["last_name"],
                 F.concat_ws(" ", pe["first_name"], pe["last_name"]).alias("full_name")))
    build_stage_and_gold("dim_customer", stage, "scd2", "dim_customer",
                         "dim_customer", ["customer_key"], run_id)


try:
    build()
except Exception:
    files_put(f"_cp_err_sq_dim_customer_{run_id}.txt", traceback.format_exc())
    raise
