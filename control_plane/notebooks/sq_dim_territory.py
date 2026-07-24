# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for dim_territory. scd1.
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql(f"""
CREATE OR REPLACE TABLE stage.dim_territory AS
SELECT territory_id AS territory_key,
       territory_id,
       name AS territory_name,
       country_region_code,
       `group` AS territory_group
FROM   `{silver_lh}`.adventureworks.sales_salesterritory
""")
