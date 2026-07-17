"""Reorganize the Fabric workspace into folders.

  notebook/                 engine + legacy transform notebooks
  notebook/utility/         wheel-candidate library code (cp_framework)
  notebook/sourcequery/     silver->stage builders (source, source_v2)
  pipeline/                 Data Pipelines (none yet)
  <root>                    lakehouses stay here

Throwaway spike notebooks are deleted.
"""
import subprocess
import sys

import requests

TEN = "490b5e1f-5a00-41ad-bac3-4704c9c4042c"
WS = "79f30543-1f5c-451f-9ef7-b46b24dc7223"
API = "https://api.fabric.microsoft.com/v1"

DELETE = ["_spike_jdbc", "_spike_diag_nb"]
UTILITY = ["cp_framework"]
SOURCEQUERY = ["source", "source_v2"]
# everything else that is a Notebook goes to notebook/ root
NOTEBOOK_ROOT_EXTRA = []  # computed as remaining notebooks


def tok():
    return subprocess.run(
        ["az", "account", "get-access-token", "--tenant", TEN, "--resource",
         "https://api.fabric.microsoft.com", "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, shell=True).stdout.strip()


def H(t):
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}


def items(t):
    r = requests.get(f"{API}/workspaces/{WS}/items", headers=H(t))
    r.raise_for_status()
    return r.json()["value"]


def create_folder(t, name, parent=None):
    body = {"displayName": name}
    if parent:
        body["parentFolderId"] = parent
    r = requests.post(f"{API}/workspaces/{WS}/folders", headers=H(t), json=body)
    if r.status_code not in (200, 201):
        # maybe already exists — find it
        fr = requests.get(f"{API}/workspaces/{WS}/folders", headers=H(t))
        for f in fr.json().get("value", []):
            if f["displayName"] == name and f.get("parentFolderId") == parent:
                return f["id"]
        sys.exit(f"create folder {name} failed: {r.status_code} {r.text}")
    return r.json()["id"]


def move_item(t, item_id, folder_id, tries=6):
    import time
    for a in range(tries):
        r = requests.post(f"{API}/workspaces/{WS}/items/{item_id}/move",
                          headers=H(t), json={"targetFolderId": folder_id})
        if r.status_code != 429:
            return r.status_code, r.text
        time.sleep(int(r.headers.get("Retry-After", 8)) + 2 * a)
    return 429, "gave up"


def delete_item(t, item_id):
    return requests.delete(f"{API}/workspaces/{WS}/items/{item_id}", headers=H(t)).status_code


def main():
    t = tok()
    by_name = {i["displayName"]: i for i in items(t) if i["type"] == "Notebook"}

    # 1. delete spikes
    for n in DELETE:
        if n in by_name:
            print(f"delete {n}: {delete_item(t, by_name[n]['id'])}")

    # 2. folders
    nb = create_folder(t, "notebook")
    util = create_folder(t, "utility", nb)
    sq = create_folder(t, "sourcequery", nb)
    create_folder(t, "pipeline")  # top-level, for future Data Pipelines
    print(f"folders: notebook={nb} utility={util} sourcequery={sq}")

    # 3. move notebooks
    placed = set(DELETE)
    for n in UTILITY:
        if n in by_name:
            print(f"  utility     <- {n}: {move_item(t, by_name[n]['id'], util)[0]}")
            placed.add(n)
    for n in SOURCEQUERY:
        if n in by_name:
            print(f"  sourcequery <- {n}: {move_item(t, by_name[n]['id'], sq)[0]}")
            placed.add(n)
    for n, i in by_name.items():
        if n not in placed:
            print(f"  notebook    <- {n}: {move_item(t, i['id'], nb)[0]}")


if __name__ == "__main__":
    main()
