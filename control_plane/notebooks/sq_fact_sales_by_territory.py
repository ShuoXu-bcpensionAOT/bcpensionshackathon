# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for fact_sales_by_territory — aggregate of the gold fact by territory. fact.
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql(f"""
CREATE OR REPLACE TABLE stage.fact_sales_by_territory AS
SELECT f.territory_key,
       dt.territory_name,
       SUM(f.line_total) AS total_sales,
       SUM(f.order_qty)  AS total_qty,
       COUNT(1)          AS order_lines
FROM      dbo.fact_sales_order f
LEFT JOIN dbo.dim_territory dt ON f.territory_key = dt.territory_key
GROUP BY f.territory_key, dt.territory_name
""")
