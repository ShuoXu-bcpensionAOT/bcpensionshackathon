# PARAMETERS
run_id = "manual"

# COMMAND ----------
%run cp_framework

# COMMAND ----------
# Gold orchestrator: read gold_object + gold_dependency, topo-sort the DAG, and run each
# object's source-query notebook in dependency order. Each notebook builds its stage table
# and merges into gold, so parents exist before children run.
import traceback
from pyspark.sql import functions as F


def build_gold():
    gobjs = {r["gold_object_id"]: r
             for r in read_config("gold_object").where(F.col("is_active")).collect()}
    deps = []
    if delta_exists(tpath("config", "gold_dependency")):
        deps = [(r["parent_gold_object_id"], r["child_gold_object_id"])
                for r in read_config("gold_dependency").collect()]
    levels = topo_levels(list(gobjs), deps)
    print("gold DAG levels:", levels)

    order = []
    for level in levels:
        for gid in level:
            nb = gobjs[gid]["source_query_notebook"]
            print(f">>> {gid}  ->  {nb}")
            res = notebookutils.notebook.run(nb, 1800, {"run_id": run_id})
            print(f"<<< {gid}: {res}")
            order.append(gid)
    print("gold build order:", order)


try:
    build_gold()
except Exception:
    files_put(f"_cp_err_gold_{run_id}.txt", traceback.format_exc())
    raise
