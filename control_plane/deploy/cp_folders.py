"""Workspace folder placement — the single source of truth for keeping deployed items tidy.
Used by cp_bootstrap (full provision), cp_deploy (notebooks), and cp_pipeline (pipelines) so
EVERY deploy path organizes items the same way:
    notebook/                 worker notebooks (manifest folder == 'notebook')
    notebook/utility/         framework/planner/utility notebooks
    notebook/sourcequery/     gold source-query notebooks
    pipeline/                 all Data Pipelines
Subfolders are derived from the manifest's per-notebook `folder`. All operations are idempotent.
"""
import time

import requests

import cp_manifest as MF

API = "https://api.fabric.microsoft.com/v1"


def _h(t):
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}


def ensure_folder(t, wid, name, parent=None):
    for f in requests.get(f"{API}/workspaces/{wid}/folders", headers=_h(t)).json().get("value", []):
        if f["displayName"] == name and f.get("parentFolderId") == parent:
            return f["id"]
    body = {"displayName": name}
    if parent:
        body["parentFolderId"] = parent
    return requests.post(f"{API}/workspaces/{wid}/folders", headers=_h(t), json=body).json()["id"]


def _move(t, wid, item_id, folder_id):
    for a in range(6):
        r = requests.post(f"{API}/workspaces/{wid}/items/{item_id}/move",
                          headers=_h(t), json={"targetFolderId": folder_id})
        if r.status_code != 429:
            return
        time.sleep(int(r.headers.get("Retry-After", 8)) + 2 * a)


def organize_notebooks(t, wid):
    """Move every notebook into notebook/ and its manifest subfolder (utility/sourcequery)."""
    nbf = ensure_folder(t, wid, "notebook")
    fmap = {"notebook": nbf}
    for folder in MF.NB_FOLDERS:                       # subfolders derived from the manifest
        if folder != "notebook":
            fmap[folder] = ensure_folder(t, wid, folder, nbf)
    items = {i["displayName"]: i["id"] for i in
             requests.get(f"{API}/workspaces/{wid}/items", headers=_h(t)).json()["value"]
             if i["type"] == "Notebook"}
    n = 0
    for folder, names in MF.NB_FOLDERS.items():
        for name in names:
            if name in items:
                _move(t, wid, items[name], fmap[folder])
                n += 1
    print(f"  organized {n} notebook(s) into notebook/ (+ utility/sourcequery)")


def move_pipelines(t, wid):
    """Move every Data Pipeline into the 'pipeline' folder."""
    fid = ensure_folder(t, wid, "pipeline")
    items = [i for i in requests.get(f"{API}/workspaces/{wid}/items", headers=_h(t)).json()["value"]
             if i["type"] == "DataPipeline"]
    for i in items:
        _move(t, wid, i["id"], fid)
    print(f"  moved {len(items)} pipeline(s) into 'pipeline' folder")
