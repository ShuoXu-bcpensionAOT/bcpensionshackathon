"""Loader for the declarative deployment manifest (deploy/manifest.yml).

Provides the deploy inventory (lakehouses, sql db, variable library, notebooks +
folders in order, pipelines, superseded items) to cp_deploy / cp_pipeline / cp_bootstrap.
Standalone (does not import cp_common) so importing it has no side effects.
"""
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

NB_FOLDERS = {}                                         # {folder: [names]}
for _n in NOTEBOOKS:
    NB_FOLDERS.setdefault(_n["folder"], []).append(_n["name"])
