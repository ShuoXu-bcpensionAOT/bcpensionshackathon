# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Build the gold star schema in dependency order (dims before fact).
# Per-object stage builders read silver; type-based writers apply SCD1 / SCD2 / fact upsert.
import json
import traceback
from pyspark.sql import functions as F

SILVER = {
    "product": "adventureworks_production_product",
    "subcategory": "adventureworks_production_productsubcategory",
    "category": "adventureworks_production_productcategory",
    "customer": "adventureworks_sales_customer",
    "person": "adventureworks_person_person",
    "soh": "adventureworks_sales_salesorderheader",
    "sod": "adventureworks_sales_salesorderdetail",
}


def sread(key):
    return read_path(tpath("silver", SILVER[key]))


# --- stage builders (one per gold object) ---
def stage_dim_product():
    p, sc, c = sread("product"), sread("subcategory"), sread("category")
    return (p.join(sc, "product_subcategory_id", "left").join(c, "product_category_id", "left")
            .select(
                p["product_id"].alias("product_key"),
                p["product_id"],
                p["name"].alias("product_name"),
                p["product_number"],
                p["color"],
                p["standard_cost"],
                p["list_price"],
                sc["name"].alias("subcategory_name"),
                c["name"].alias("category_name")))


def stage_dim_customer():
    cu, pe = sread("customer"), sread("person")
    return (cu.join(pe, cu["person_id"] == pe["business_entity_id"], "left")
            .select(
                cu["customer_id"].alias("customer_key"),
                cu["customer_id"],
                cu["store_id"],
                cu["territory_id"],
                pe["person_type"],
                pe["first_name"],
                pe["last_name"],
                F.concat_ws(" ", pe["first_name"], pe["last_name"]).alias("full_name")))


def stage_fact_sales_order():
    h, d = sread("soh"), sread("sod")
    dimp = read_path(tpath("gold", "dim_product")).select("product_key", "product_id")
    dimc = read_path(tpath("gold", "dim_customer"))
    if "_is_current" in dimc.columns:
        dimc = dimc.where(F.col("_is_current"))
    dimc = dimc.select("customer_key", "customer_id")
    return (d.join(h, "sales_order_id")
            .join(dimp, "product_id")          # RI: product must exist in dim
            .join(dimc, "customer_id")          # RI: customer must exist in dim
            .select(
                h["sales_order_id"], d["sales_order_detail_id"], h["order_date"],
                dimc["customer_key"], dimp["product_key"],
                d["order_qty"], d["unit_price"], d["line_total"], h["total_due"]))


BUILDERS = {
    "dim_product": stage_dim_product,
    "dim_customer": stage_dim_customer,
    "fact_sales_order": stage_fact_sales_order,
}


# --- type-based gold writers ---
def write_scd1(stage, gold_table, keys):
    stage = stage.withColumn("_gold_run_id", F.lit(run_id)) \
                 .withColumn("_gold_updated_at", F.current_timestamp())
    merge_upsert(tpath("gold", gold_table), stage, keys)


def write_fact(stage, gold_table, keys):
    stage = stage.withColumn("_gold_run_id", F.lit(run_id)) \
                 .withColumn("_gold_updated_at", F.current_timestamp())
    merge_upsert(tpath("gold", gold_table), stage, keys)


def write_scd2(stage, gold_table, keys):
    from delta.tables import DeltaTable
    stage = row_hash(stage)
    incoming = (stage.withColumn("_effective_start_ts", F.current_timestamp())
                     .withColumn("_effective_end_ts", F.lit(None).cast("timestamp"))
                     .withColumn("_is_current", F.lit(True))
                     .withColumn("_gold_run_id", F.lit(run_id)))
    path = tpath("gold", gold_table)
    if not delta_exists(path):
        write_path(incoming, path, mode="overwrite")
        return
    tgt = DeltaTable.forPath(spark, path)
    cur = tgt.toDF().where(F.col("_is_current"))
    keycond = [incoming[k] == cur[k] for k in keys]
    changed = (incoming.join(cur, keycond, "inner")
               .where(incoming["_row_hash"] != cur["_row_hash"])
               .select([incoming[k] for k in keys]))
    # expire changed current rows
    if changed.count():
        expire_cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
        (tgt.alias("t").merge(changed.alias("s"), expire_cond)
            .whenMatchedUpdate(set={"_is_current": F.lit(False),
                                    "_effective_end_ts": F.current_timestamp()}).execute())
    # insert new + changed versions (keys not currently current)
    cur_keys = tgt.toDF().where(F.col("_is_current")).select(*keys)
    to_insert = incoming.join(cur_keys, keys, "left_anti")
    if to_insert.count():
        write_path(to_insert, path, mode="append")


WRITERS = {"scd1": write_scd1, "scd2": write_scd2, "fact": write_fact}


def build_gold():
    gobjs = {r["gold_object_id"]: r for r in read_config("gold_object").where(F.col("is_active")).collect()}
    deps = [(r["parent_gold_object_id"], r["child_gold_object_id"])
            for r in read_config("gold_dependency").collect()] \
        if delta_exists(tpath("config", "gold_dependency")) else []
    levels = topo_levels(list(gobjs), deps)
    print("gold DAG levels:", levels)

    summary = []
    for level in levels:
        for gid in level:
            g = gobjs[gid]
            keys = [snake(k) for k in json.loads(g["business_key_columns_json"])]
            stage = BUILDERS[gid]()
            write_path(stage, tpath(STAGE_LH, f"stage_{g['stage_table']}"), mode="overwrite")
            WRITERS[g["gold_type"]](stage, g["gold_table"], keys)
            cnt = read_path(tpath("gold", g["gold_table"])).count()
            log_object_run(run_id, gid, "gold", "SUCCEEDED", target_count=cnt,
                           details={"gold_type": g["gold_type"]})
            summary.append((g["gold_table"], g["gold_type"], cnt))
            print(f"gold {g['gold_table']} ({g['gold_type']}): {cnt} rows")

    print("=== gold summary ===")
    for t, ty, c in summary:
        print(f"  {t:<24} {ty:<6} {c:>8}")


try:
    build_gold()
except Exception:
    files_put(f"_cp_err_gold_{run_id}.txt", traceback.format_exc())
    raise
