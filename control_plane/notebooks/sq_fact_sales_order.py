# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Source query: silver order header+detail joined to dims -> gold fact_sales_order (fact).
# Depends on dim_product, dim_customer, dim_territory (surrogate keys + referential integrity).
import traceback
from pyspark.sql import functions as F


def build():
    h = read_path(tpath("silver", "adventureworks_sales_salesorderheader"))
    d = read_path(tpath("silver", "adventureworks_sales_salesorderdetail"))
    dp = read_path(tpath("gold", "dim_product")).select("product_key", "product_id")
    dcu = read_path(tpath("gold", "dim_customer"))
    if "_is_current" in dcu.columns:
        dcu = dcu.where(F.col("_is_current"))
    dcu = dcu.select("customer_key", "customer_id")
    dt = read_path(tpath("gold", "dim_territory")).select("territory_key", "territory_id")

    stage = (d.join(h, "sales_order_id")
             .join(dp, "product_id")                                   # RI: product
             .join(dcu, h["customer_id"] == dcu["customer_id"], "inner")  # RI: customer
             .join(dt, h["territory_id"] == dt["territory_id"], "left")
             .select(
                 h["sales_order_id"], d["sales_order_detail_id"], h["order_date"],
                 dcu["customer_key"], dp["product_key"], dt["territory_key"],
                 d["order_qty"], d["unit_price"], d["line_total"]))
    build_stage_and_gold("fact_sales_order", stage, "fact", "fact_sales_order",
                         "fact_sales_order", ["sales_order_id", "sales_order_detail_id"], run_id)


try:
    build()
except Exception:
    files_put(f"_cp_err_sq_fact_sales_order_{run_id}.txt", traceback.format_exc())
    raise
