"""Deploy + run the control-plane Fabric notebooks.

  python scripts/cp_deploy.py deploy [name...]   # upload notebooks (default: all)
  python scripts/cp_deploy.py run <name> [k=v..]  # run one notebook, pass params
"""
import os
import sys
from pathlib import Path

import fabric_nb as FN
import cp_common as C


def _with_source_creds(params):
    """Inject source DB creds from .env for engine notebooks (avoids CLI secrets)."""
    params.setdefault("src_server", os.getenv("SOURCE_DB", ""))
    params.setdefault("src_user", os.getenv("USERNAME", ""))
    params.setdefault("src_password", os.getenv("PASSWORD", ""))
    return params

NB_DIR = C.REPO / "control_plane" / "notebooks"
# deploy order: framework first (others %run it)
SQ = ["sq_dim_category", "sq_dim_subcategory", "sq_dim_product", "sq_dim_territory",
      "sq_dim_customer", "sq_fact_sales_order", "sq_fact_sales_by_territory"]
ORDER = ["cp_framework", "cp_01_setup", "cp_02_ingest_bronze", "cp_03_build_silver",
         *SQ, "cp_04_build_gold", "cp_09_orchestrate"]


def source_cells(name):
    """Split a notebook .py source into cells on '# COMMAND ----------'.
    Framework is a single cell (pure module)."""
    text = (NB_DIR / f"{name}.py").read_text(encoding="utf-8")
    if "# COMMAND ----------" not in text:
        return [text]
    return [c.strip("\n") for c in text.split("# COMMAND ----------") if c.strip()]


def deploy(names):
    tok = FN.token()
    for name in names:
        cells = source_cells(name)
        FN.upsert_notebook(tok, name, FN.build_ipynb(cells))
        print(f"  deployed {name} ({len(cells)} cell(s))")


def run(name, params):
    tok = FN.token()
    print(f"running {name} params={ {k: ('***' if 'pass' in k else v) for k, v in params.items()} }")
    st, info = FN.run_notebook(tok, name, params or None, timeout=1800)
    print("status:", st)
    if st != "Completed":
        import json
        print(json.dumps(info.get("failureReason", info), indent=2)[:1500])
    return st


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "deploy"
    if cmd == "deploy":
        names = sys.argv[2:] or ORDER
        names = [n for n in ORDER if n in names]  # keep dependency order
        deploy(names)
    elif cmd == "run":
        name = sys.argv[2]
        params = dict(kv.split("=", 1) for kv in sys.argv[3:] if "=" in kv)
        if name in ("cp_02_ingest_bronze", "cp_04_build_gold", "cp_09_orchestrate"):
            params = _with_source_creds(params)
        sys.exit(0 if run(name, params) == "Completed" else 1)
