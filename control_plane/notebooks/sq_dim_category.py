# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for dim_category. Framework-free: read silver, write the stage table; the gold runner
# applies the scd1 strategy to merge stage -> gold. (gold is the attached default lakehouse.)
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql(f"""
CREATE OR REPLACE TABLE stage.dim_category AS
SELECT product_category_id AS category_key,
       product_category_id AS category_id,
       name               AS category_name
FROM   `{silver_lh}`.adventureworks.production_productcategory
""")
