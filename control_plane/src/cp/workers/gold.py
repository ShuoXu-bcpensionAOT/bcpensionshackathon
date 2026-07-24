"""Gold runner: build one model's gold objects in dependency (topo) order. For each object it runs
the source-query notebook — a framework-free SparkSQL/PySpark transform that writes a **stage**
table (gold.stage.<stage_table>) — then applies the gold **strategy** for its type (scd1/scd2/fact)
to merge the stage into gold. The merge logic lives in cp.gold, never in the notebook.

The runner injects the resolved silver lakehouse name (`silver_lh`) so the SQ notebooks reference
it by name without hardcoding — gold is their attached default lakehouse."""
import json
import traceback

from ..runtime import notebookutils, tpath, LAYER_NAMES
from ..config_db import config_query
from ..dag import topo_levels
from ..storage import read_path, files_put
from ..gold import gold_merge
from ..audit import log_object_run


def gold(run_id="manual", model_id=1, **kw):
    mid = int(model_id)
    sq_params = {"run_id": run_id, "silver_lh": LAYER_NAMES["silver"]}

    def _work():
        gobjs = config_query(
            "SELECT gold_object_id, source_query_notebook, gold_type, stage_table, gold_table, "
            "business_key_columns_json FROM dbo.gold_object WHERE model_id=? AND is_active=1", (mid,))
        by_id = {g["gold_object_id"]: g for g in gobjs}
        ids = list(by_id)
        deps = config_query(
            "SELECT parent_gold_object_id, child_gold_object_id FROM dbo.gold_dependency", ())
        edges = [(d["parent_gold_object_id"], d["child_gold_object_id"]) for d in deps
                 if d["parent_gold_object_id"] in ids and d["child_gold_object_id"] in ids]
        levels = topo_levels(ids, edges)
        print(f"model {mid} gold DAG levels:", levels)
        for level in levels:
            for gid in level:
                g = by_id[gid]
                print(f">>> {gid}: stage via {g['source_query_notebook']}")
                notebookutils.notebook.run(g["source_query_notebook"], 1800, sq_params)   # writes stage
                stage = read_path(tpath("gold", g["stage_table"], "stage"))                # read stage
                keys = json.loads(g["business_key_columns_json"])
                cnt = gold_merge(stage, g["gold_type"], g["gold_table"], keys, run_id)      # apply strategy
                log_object_run(run_id, gid, "gold", "SUCCEEDED", target_count=cnt,
                               details={"gold_type": g["gold_type"]})
                print(f"    gold {g['gold_table']} ({g['gold_type']}): {cnt} rows")

    try:
        _work()
    except Exception:
        files_put(f"_cp_err_gold_model{mid}_{run_id}.txt", traceback.format_exc())
        raise
