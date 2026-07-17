"""Reset data + run-state tables for a clean end-to-end run. Keeps config tables.

Drops every table in bronze/silver/gold lakehouses (incl. stage_/quarantine_),
and the state/audit tables in metadata. Preserves datasource/source_object/
dq_rule/model/gold_object/gold_dependency.
"""
import json

import requests

import cp_common as C

KEEP_CONFIG = {"datasource", "source_object", "dq_rule", "model",
               "gold_object", "gold_dependency"}


def list_tables(guid, tok):
    r = requests.get(
        f"https://onelake.dfs.fabric.microsoft.com/{C.WS_ID}?resource=filesystem"
        f"&recursive=false&directory={guid}/Tables",
        headers={"Authorization": f"Bearer {tok}"})
    return [p["name"].split("/")[-1] for p in json.loads(r.text).get("paths", [])] if r.ok else []


def drop(guid, table, tok):
    return requests.delete(
        f"https://onelake.dfs.fabric.microsoft.com/{C.WS_ID}/{guid}/Tables/{table}?recursive=true",
        headers={"Authorization": f"Bearer {tok}"}).status_code


def main():
    tok = C.storage_token()
    for layer in ("bronze", "silver", "gold"):
        for t in list_tables(C.LH[layer], tok):
            print(f"drop {layer}.{t}: {drop(C.LH[layer], t, tok)}")
    for t in list_tables(C.LH["config"], tok):
        if t not in KEEP_CONFIG:
            print(f"drop config.{t}: {drop(C.LH['config'], t, tok)}")
    print("reset complete (config tables preserved)")


if __name__ == "__main__":
    main()
