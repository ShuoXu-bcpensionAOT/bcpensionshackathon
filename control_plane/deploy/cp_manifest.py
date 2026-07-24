"""Loader for the declarative deployment manifest (deploy/manifest.yml).

Provides the deploy inventory (lakehouses, sql db, variable library, notebooks +
folders in order, pipelines, superseded items) to cp_deploy / cp_pipeline / cp_bootstrap.
Standalone (does not import cp_common) so importing it has no side effects.
"""
import json
import os
from pathlib import Path

import yaml

_here = Path(__file__).resolve()
if _here.parents[1].name == "control_plane":            # <repo>/control_plane/deploy/
    REPO = _here.parents[2]
else:
    REPO = Path(os.getenv("CP_REPO", r"C:\Users\Shuo\OneDrive\文档\bcpensionshackathon"))

_M = yaml.safe_load((REPO / "control_plane" / "deploy" / "manifest.yml").read_text(encoding="utf-8"))

LAKEHOUSES = _M["lakehouses"]
SQL_DATABASE = _M["sql_database"]
VARIABLE_LIBRARY = _M["variable_library"]
NOTEBOOKS = _M["notebooks"]                             # [{name, folder}] in deploy order
NOTEBOOK_ORDER = [n["name"] for n in NOTEBOOKS]
PIPELINES = _M["pipelines"]                             # in deploy order
SUPERSEDED_NOTEBOOKS = _M.get("superseded_notebooks", [])
# Optional opt-in driver Environment: {name, pip:[...], jars:[...], set_default:bool}.
# When present, cp_bootstrap provisions+publishes a Fabric Environment (Oracle/DB2 drivers etc.).
ENVIRONMENT = _M.get("environment")

NB_FOLDERS = {}                                         # {folder: [names]}
for _n in NOTEBOOKS:
    NB_FOLDERS.setdefault(_n["folder"], []).append(_n["name"])

# logical layer -> physical lakehouse name, from the cp_vars variable library (config-as-code).
# Deploy tooling that must bind a lakehouse GUID (e.g. Copy-activity sinks) resolves the name here
# so it always tracks the current names — no hardcoded 'bronze' that breaks after a rename.
_VARS = json.loads((REPO / "control_plane" / "variable_library" / "cp_vars.VariableLibrary"
                    / "variables.json").read_text(encoding="utf-8"))
LAKEHOUSE_NAMES = {v["name"][:-len("_lakehouse")]: v["value"]
                   for v in _VARS["variables"] if v["name"].endswith("_lakehouse")}
