# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for dim_subcategory (attaches the category surrogate key from the prior gold dim). scd1.
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql(f"""
CREATE OR REPLACE TABLE stage.dim_subcategory AS
SELECT sc.product_subcategory_id AS subcategory_key,
       sc.product_subcategory_id AS subcategory_id,
       sc.name                   AS subcategory_name,
       dc.category_key,
       dc.category_name
FROM      `{silver_lh}`.adventureworks.production_productsubcategory sc
LEFT JOIN dbo.dim_category dc ON sc.product_category_id = dc.category_id
""")
