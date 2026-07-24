# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for dim_product (denormalizes subcategory + category from the prior gold dim). scd1.
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql(f"""
CREATE OR REPLACE TABLE stage.dim_product AS
SELECT p.product_id AS product_key,
       p.product_id,
       p.name AS product_name,
       p.product_number, p.color, p.standard_cost, p.list_price,
       ds.subcategory_key, ds.subcategory_name, ds.category_name
FROM      `{silver_lh}`.adventureworks.production_product p
LEFT JOIN dbo.dim_subcategory ds ON p.product_subcategory_id = ds.subcategory_id
""")
