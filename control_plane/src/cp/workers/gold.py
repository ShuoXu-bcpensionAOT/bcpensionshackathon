"""Gold entrypoint: build ONE model's gold objects in dependency (topo) order by running each
object's source-query notebook. All logic lives here so the gold_runner notebook is a shell."""
import traceback

from ..runtime import notebookutils
from ..config_db import config_query
from ..dag import topo_levels
from ..storage import files_put


def gold(run_id="manual", model_id=1, **kw):
    mid = int(model_id)

    def _work():
        gobjs = config_query(
            "SELECT gold_object_id, source_query_notebook FROM dbo.gold_object "
            "WHERE model_id=? AND is_active=1", (mid,))
        ids = [g["gold_object_id"] for g in gobjs]
        nb_by_id = {g["gold_object_id"]: g["source_query_notebook"] for g in gobjs}
        deps = config_query(
            "SELECT parent_gold_object_id, child_gold_object_id FROM dbo.gold_dependency", ())
        edges = [(d["parent_gold_object_id"], d["child_gold_object_id"]) for d in deps
                 if d["parent_gold_object_id"] in ids and d["child_gold_object_id"] in ids]
        levels = topo_levels(ids, edges)
        print(f"model {mid} gold DAG levels:", levels)
        order = []
        for level in levels:
            for gid in level:
                print(f">>> {gid} -> {nb_by_id[gid]}")
                notebookutils.notebook.run(nb_by_id[gid], 1800, {"run_id": run_id})
                order.append(gid)
        print("gold build order:", order)

    try:
        _work()
    except Exception:
        files_put(f"_cp_err_gold_model{mid}_{run_id}.txt", traceback.format_exc())
        raise
