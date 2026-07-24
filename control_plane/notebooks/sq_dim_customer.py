# PARAMETERS
run_id = "manual"
silver_lh = "LH_silver"

# COMMAND ----------
# Stage for dim_customer — current snapshot of customers + territory key. The gold runner's scd2
# strategy handles history (closes changed rows, inserts new versions).
spark.sql("CREATE SCHEMA IF NOT EXISTS stage")
spark.sql(f"""
CREATE OR REPLACE TABLE stage.dim_customer AS
SELECT cu.customer_id AS customer_key,
       cu.customer_id, cu.store_id,
       dt.territory_key,
       pe.person_type, pe.first_name, pe.last_name,
       concat_ws(' ', pe.first_name, pe.last_name) AS full_name
FROM      `{silver_lh}`.adventureworks.sales_customer cu
LEFT JOIN `{silver_lh}`.adventureworks.person_person pe ON cu.person_id = pe.business_entity_id
LEFT JOIN dbo.dim_territory dt ON cu.territory_id = dt.territory_id
""")
