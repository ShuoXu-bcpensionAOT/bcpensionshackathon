"""Helpers to deploy + run Fabric notebooks via the REST API, and read Delta back.

Reusable library for the control-plane build. Auth via az CLI (MFA-friendly).
`python scripts/fabric_nb.py spike` runs a JDBC connectivity test.
"""

import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

import cp_auth

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

# CP_TARGET_WORKSPACE_ID lets the bootstrap point deploy/run at another workspace (e.g. UAT).
WS = os.getenv("CP_TARGET_WORKSPACE_ID") or os.getenv("FABRIC_WORKSPACE_ID")
WS_NAME = os.getenv("CP_TARGET_WORKSPACE") or os.getenv("FABRIC_WORKSPACE_NAME", "HackathonShuo-DEV")
TENANT = os.getenv("AZURE_TENANT_ID")
API = "https://api.fabric.microsoft.com/v1"


def token(resource="https://api.fabric.microsoft.com"):
    # SP (client-credentials) when configured, else personal az login — see cp_auth.
    return cp_auth.get_token(resource)


def _headers(tok):
    return {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}


def _poll_lro(resp, tok):
    if resp.status_code == 202:
        loc = resp.headers.get("Location")
        retry = int(resp.headers.get("Retry-After", 3))
        while loc:
            time.sleep(retry)
            p = requests.get(loc, headers={"Authorization": f"Bearer {tok}"})
            st = p.json().get("status") if p.content else None
            if st in ("Succeeded", "Completed"):
                rurl = p.headers.get("Location") or f"{loc}/result"
                return requests.get(rurl, headers={"Authorization": f"Bearer {tok}"})
            if st == "Failed":
                sys.exit(f"LRO failed: {p.text}")
    return resp


def build_ipynb(cells, default_lakehouse_id=None, default_lakehouse_name=None, environment_id=None,
                known_lakehouse_ids=None):
    """cells: list of code strings (or ('md', text) for markdown).
    A code cell whose first line is '# PARAMETERS' is tagged as a Fabric
    parameters cell. environment_id attaches the notebook to a Fabric Environment
    (so its libraries/driver jars are on the classpath). known_lakehouse_ids attaches extra
    lakehouses so SparkSQL can reference them by name (e.g. source-query notebooks that read
    LH_silver and write LH_gold)."""
    nb_cells = []
    for c in cells:
        if isinstance(c, tuple) and c[0] == "md":
            nb_cells.append({"cell_type": "markdown", "metadata": {},
                             "source": (c[1] + "\n").splitlines(keepends=True)})
        else:
            meta = {}
            if c.lstrip().startswith("# PARAMETERS"):
                meta = {"tags": ["parameters"]}
            nb_cells.append({"cell_type": "code", "metadata": meta, "execution_count": None,
                             "outputs": [], "source": (c + "\n").splitlines(keepends=True)})
    meta = {
        "language_info": {"name": "python"},
        "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
    }
    deps = {}
    if default_lakehouse_id:
        deps["lakehouse"] = {
            "default_lakehouse": default_lakehouse_id,
            "default_lakehouse_name": default_lakehouse_name,
            "default_lakehouse_workspace_id": WS,
        }
        if known_lakehouse_ids:
            deps["lakehouse"]["known_lakehouses"] = [{"id": g} for g in known_lakehouse_ids]
    if environment_id:
        deps["environment"] = {"environmentId": environment_id, "workspaceId": WS}
    if deps:
        meta["dependencies"] = deps
    nb = {"cells": nb_cells, "metadata": meta, "nbformat": 4, "nbformat_minor": 5}
    return json.dumps(nb)


def find_item(tok, display_name, item_type="Notebook"):
    r = requests.get(f"{API}/workspaces/{WS}/items", headers=_headers(tok))
    r.raise_for_status()
    for it in r.json().get("value", []):
        if it["displayName"] == display_name and it["type"] == item_type:
            return it["id"]
    return None


def upsert_notebook(tok, name, ipynb_json):
    payload = base64.b64encode(ipynb_json.encode()).decode()
    definition = {"format": "ipynb", "parts": [
        {"path": "notebook-content.ipynb", "payload": payload, "payloadType": "InlineBase64"}]}
    iid = find_item(tok, name)
    if iid:
        r = requests.post(f"{API}/workspaces/{WS}/items/{iid}/updateDefinition",
                          headers=_headers(tok), json={"definition": definition})
        _poll_lro(r, tok)
        return iid
    r = requests.post(f"{API}/workspaces/{WS}/notebooks", headers=_headers(tok),
                      json={"displayName": name, "definition": definition})
    r = _poll_lro(r, tok)
    return find_item(tok, name)


def run_notebook(tok, name, params=None, timeout=1200):
    iid = find_item(tok, name)
    if not iid:
        sys.exit(f"notebook not found: {name}")
    body = {}
    if params:
        body = {"executionData": {"parameters": {
            k: {"value": str(v), "type": "string"} for k, v in params.items()}}}
    r = requests.post(f"{API}/workspaces/{WS}/items/{iid}/jobs/instances?jobType=RunNotebook",
                      headers=_headers(tok), json=body)
    if r.status_code not in (200, 202):
        sys.exit(f"run start failed [{r.status_code}]: {r.text}")
    loc = r.headers.get("Location")
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(int(r.headers.get("Retry-After", 8)) if r.headers.get("Retry-After") else 10)
        p = requests.get(loc, headers={"Authorization": f"Bearer {tok}"})
        st = p.json().get("status")
        if st in ("Completed", "Failed", "Cancelled", "Deduped"):
            return st, p.json()
    return "Timeout", {}


# --------------------------------------------------------------------------- #
def _spike():
    tok = token()
    pwd = os.getenv("PASSWORD")
    out_uri = (f"abfss://{WS_NAME}@onelake.dfs.fabric.microsoft.com/"
               f"metadata.Lakehouse/Tables/_spike_result")
    cell = f'''
out = "{out_uri}"
status = "?"; cnt = -1; err = ""
try:
    url = "jdbc:sqlserver://{os.getenv('SOURCE_DB')}:1433;database=AdventureWorks2025;encrypt=true;trustServerCertificate=true;loginTimeout=30"
    df = (spark.read.format("jdbc")
          .option("url", url).option("dbtable", "Production.ProductCategory")
          .option("user", "{os.getenv('USERNAME')}").option("password", "{pwd}")
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load())
    cnt = int(df.count()); status = "JDBC_OK"
except Exception as e:
    import traceback
    status = "JDBC_FAIL"; err = (repr(e) + " || " + traceback.format_exc())[:3500]
spark.createDataFrame([(cnt, status, err)], "n int, status string, err string") \\
     .write.mode("overwrite").format("delta").save(out)
print(status, cnt)
'''
    print("Deploying spike notebook...")
    upsert_notebook(tok, "_spike_jdbc", build_ipynb([cell]))
    print("Running (Spark session cold-start can take 1-3 min)...")
    st, info = run_notebook(tok, "_spike_jdbc", timeout=900)
    print("Run status:", st)
    if st != "Completed":
        print(json.dumps(info, indent=2)[:1500])


def _diag():
    tok = token()
    host, user, pwd = os.getenv("SOURCE_DB"), os.getenv("USERNAME"), os.getenv("PASSWORD")
    META_ID = "35cd447c-1572-4726-afd6-54e01062659b"
    cell = f'''
import json
res = {{"steps": []}}
def log(s): res["steps"].append(str(s))
try: log("spark_ok rows=" + str(spark.range(3).count()))
except Exception as e: log("spark_fail " + repr(e)[:400])
try:
    import notebookutils; log("notebookutils_ok")
except Exception as e: log("notebookutils_missing " + repr(e)[:200])
try:
    mssparkutils; log("mssparkutils_ok")
except Exception as e: log("mssparkutils_missing " + repr(e)[:200])
try:
    url = "jdbc:sqlserver://{host}:1433;database=AdventureWorks2025;encrypt=true;trustServerCertificate=true;loginTimeout=30"
    df = (spark.read.format("jdbc").option("url", url).option("dbtable", "Production.ProductCategory")
          .option("user", "{user}").option("password", "{pwd}")
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load())
    log("jdbc_ok cnt=" + str(df.count()))
except Exception as e: log("jdbc_fail " + repr(e)[:1200])
try:
    spark.createDataFrame([(1,)], "x int").write.mode("overwrite").format("delta").saveAsTable("_spike_default")
    log("write_default_ok")
except Exception as e: log("write_default_fail " + repr(e)[:500])
try:
    spark.createDataFrame([(1,)], "x int").write.mode("overwrite").format("delta").save("abfss://{WS_NAME}@onelake.dfs.fabric.microsoft.com/metadata.Lakehouse/Tables/_spike_abfss_name")
    log("write_abfss_name_ok")
except Exception as e: log("write_abfss_name_fail " + repr(e)[:500])
try:
    spark.createDataFrame([(1,)], "x int").write.mode("overwrite").format("delta").save("abfss://{WS}@onelake.dfs.fabric.microsoft.com/{META_ID}/Tables/_spike_abfss_guid")
    log("write_abfss_guid_ok")
except Exception as e: log("write_abfss_guid_fail " + repr(e)[:500])
try:
    rows = [(i, s) for i, s in enumerate(res["steps"])]
    spark.createDataFrame(rows, "i int, step string").write.mode("overwrite").format("delta").saveAsTable("_spike_diag")
    log("diag_written")
except Exception as e: log("diag_fail " + repr(e)[:400])
try:
    notebookutils.fs.put("Files/_spike_diag.json", json.dumps(res), True)
    log("files_put_ok")
except Exception as e:
    log("files_put_fail " + repr(e)[:400])
try:
    notebookutils.fs.put("Files/_spike_diag.json", json.dumps(res), True)
except Exception: pass
print(json.dumps(res))
try: notebookutils.notebook.exit(json.dumps(res))
except Exception: pass
'''
    print("Deploying diag notebook (metadata as default lakehouse)...")
    upsert_notebook(tok, "_spike_diag_nb",
                    build_ipynb([cell], default_lakehouse_id=META_ID, default_lakehouse_name="metadata"))
    print("Running diagnostic...")
    st, info = run_notebook(tok, "_spike_diag_nb", timeout=900)
    print("Run status:", st)
    if st != "Completed":
        print(json.dumps(info.get("failureReason", info), indent=2)[:1200])


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "spike":
        _spike()
    elif mode == "diag":
        _diag()
