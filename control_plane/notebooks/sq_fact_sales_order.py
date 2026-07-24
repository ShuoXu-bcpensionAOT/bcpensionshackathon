# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for fact_sales_order — silver order header+detail joined to the gold dims for surrogate
# keys (current customer version). fact.
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql(f"""
CREATE OR REPLACE TABLE stage.fact_sales_order AS
SELECT h.sales_order_id, d.sales_order_detail_id, h.order_date,
       dcu.customer_key, dp.product_key, dt.territory_key,
       d.order_qty, d.unit_price, d.line_total
FROM      `{silver_lh}`.adventureworks.sales_salesorderdetail d
JOIN      `{silver_lh}`.adventureworks.sales_salesorderheader h ON d.sales_order_id = h.sales_order_id
JOIN      dbo.dim_product  dp  ON d.product_id  = dp.product_id
JOIN      dbo.dim_customer dcu ON h.customer_id = dcu.customer_id AND dcu._is_current
LEFT JOIN dbo.dim_territory dt ON h.territory_id = dt.territory_id
""")
