"""Deploy + run the control-plane Fabric notebooks.

  python scripts/cp_deploy.py deploy [name...]   # upload notebooks (default: all)
  python scripts/cp_deploy.py run <name> [k=v..]  # run one notebook, pass params
"""
import os
import sys
from pathlib import Path

import fabric_nb as FN
import cp_common as C
import cp_manifest as MF


def _with_source_creds(params):
    """Inject source DB user/password from .env (server now comes from cp_vars var lib;
    Phase 3 moves creds into a Fabric Connection)."""
    params.setdefault("src_user", os.getenv("USERNAME", ""))
    params.setdefault("src_password", os.getenv("PASSWORD", ""))
    return params

NB_DIR = C.REPO / "control_plane" / "notebooks"
# Deploy order comes from the manifest (framework first; others %run it).
ORDER = MF.NOTEBOOK_ORDER


def source_cells(name):
    """Split a notebook .py source into cells on '# COMMAND ----------'.
    Framework is a single cell (pure module)."""
    text = (NB_DIR / f"{name}.py").read_text(encoding="utf-8")
    if "# COMMAND ----------" not in text:
        return [text]
    return [c.strip("\n") for c in text.split("# COMMAND ----------") if c.strip()]


def deploy(names):
    tok = FN.token()
    # Regenerate the single-cell cp_framework notebook from the modular src/cp package
    # (the bundler validates the public API + registries; fails the deploy if a module broke).
    if "cp_framework" in names:
        import cp_bundle
        cp_bundle.main()
    # Attach the driver Environment (if provisioned) to the notebooks that run connectors,
    # so their libraries / JDBC jars are on the classpath. Set by cp_bootstrap.
    env_id = os.getenv("CP_ENV_ID") or None
    attach = set(filter(None, (os.getenv("CP_ENV_ATTACH") or "").split(","))) if env_id else set()
    # Source-query notebooks are framework-free SparkSQL/PySpark: attach LH_gold (default, where
    # they write the stage) + LH_silver (read) so they reference tables by name. gold_runner gets
    # the same attachment because notebookutils.notebook.run executes the child SQ in the PARENT's
    # lakehouse context. GUIDs from cp_vars.
    lakehouse_attach = set(MF.NB_FOLDERS.get("sourcequery", [])) | {"gold_runner"}
    gold_id, silver_id = C.LH.get("gold"), C.LH.get("silver")
    for name in names:
        cells = source_cells(name)
        eid = env_id if name in attach else None
        if name in lakehouse_attach and gold_id and silver_id:
            ipynb = FN.build_ipynb(cells, default_lakehouse_id=gold_id,
                                   default_lakehouse_name=C.LAYER_NAMES["gold"],
                                   known_lakehouse_ids=[silver_id, gold_id], environment_id=eid)
            tag = "  [lakehouse-attached]"
        else:
            ipynb = FN.build_ipynb(cells, environment_id=eid)
            tag = "  [env-attached]" if eid else ""
        FN.upsert_notebook(tok, name, ipynb)
        print(f"  deployed {name} ({len(cells)} cell(s)){tag}")
    import cp_folders                                   # keep the workspace tidy on every deploy
    cp_folders.organize_notebooks(tok, FN.WS)


def run(name, params):
    tok = FN.token()
    print(f"running {name} params={ {k: ('***' if 'pass' in k else v) for k, v in params.items()} }")
    st, info = FN.run_notebook(tok, name, params or None, timeout=3600)
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
