"""Author + deploy Fabric Data Pipelines (main + children) via REST API.

Planner-notebook pattern (config read via pyodbc in cp_plan). Notebook workers are
param-driven. Switch to native SQL Lookups once a service principal exists.
"""
import base64
import json
import sys
import time

import requests

import cp_common as C
import fabric_nb as FN
import cp_manifest as MF

API = FN.API


# --- expression / activity helpers ---
def expr(e):
    return {"value": e, "type": "Expression"}


def pexpr(e, typ="string"):
    return {"value": expr(e), "type": typ}


def plit(v, typ="string"):
    return {"value": v, "type": typ}


def dep(names, cond="Succeeded"):
    return [{"activity": n, "dependencyConditions": [cond]} for n in (names or [])]


def nb(name, notebook_id, params, depends=None, cond="Succeeded"):
    return {"name": name, "type": "TridentNotebook",
            "dependsOn": dep(depends, cond),
            "policy": {"timeout": "0.12:00:00", "retry": 0},
            "typeProperties": {"notebookId": notebook_id, "workspaceId": FN.WS, "parameters": params}}


def foreach(name, items_expr, inner, depends=None, batch=3):
    return {"name": name, "type": "ForEach", "dependsOn": dep(depends),
            "typeProperties": {"items": expr(items_expr), "isSequential": False,
                               "batchCount": batch, "activities": inner}}


def if_cond(name, condition_expr, if_true, depends=None):
    return {"name": name, "type": "IfCondition", "dependsOn": dep(depends),
            "typeProperties": {"expression": expr(condition_expr), "ifTrueActivities": if_true}}


def pipeline(parameters, activities):
    return {"properties": {"parameters": parameters, "activities": activities}}


# --- deploy / run ---
def _headers(tok):
    return {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}


def _wait(r, tok):
    if r.status_code == 202:
        loc = r.headers.get("Location")
        while loc:
            time.sleep(int(r.headers.get("Retry-After", 3)))
            p = requests.get(loc, headers={"Authorization": f"Bearer {tok}"})
            st = p.json().get("status") if p.content else None
            if st in ("Succeeded", "Completed"):
                return
            if st == "Failed":
                sys.exit(f"LRO failed: {p.text}")
    elif r.status_code not in (200, 201):
        sys.exit(f"[{r.status_code}] {r.text}")


def deploy_pipeline(tok, name, content):
    payload = base64.b64encode(json.dumps(content).encode()).decode()
    definition = {"parts": [{"path": "pipeline-content.json", "payload": payload,
                             "payloadType": "InlineBase64"}]}
    iid = FN.find_item(tok, name, "DataPipeline")
    if iid:
        _wait(requests.post(f"{API}/workspaces/{FN.WS}/items/{iid}/updateDefinition",
                            headers=_headers(tok), json={"definition": definition}), tok)
    else:
        _wait(requests.post(f"{API}/workspaces/{FN.WS}/items", headers=_headers(tok),
                            json={"displayName": name, "type": "DataPipeline",
                                  "definition": definition}), tok)
    return FN.find_item(tok, name, "DataPipeline")


def run_pipeline(tok, name, params=None, timeout=3600):
    pid = FN.find_item(tok, name, "DataPipeline")
    body = {"executionData": {"parameters": params}} if params else {}
    r = requests.post(f"{API}/workspaces/{FN.WS}/items/{pid}/jobs/instances?jobType=Pipeline",
                      headers=_headers(tok), json=body)
    if r.status_code not in (200, 202):
        return f"start failed [{r.status_code}]: {r.text}", {}
    loc = r.headers.get("Location")
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(15)
        p = requests.get(loc, headers={"Authorization": f"Bearer {tok}"})
        st = p.json().get("status")
        if st in ("Completed", "Failed", "Cancelled", "Deduped"):
            return st, p.json()
    return "Timeout", {}


# --- child pipeline builders ---
def nbid(tok, name):
    i = FN.find_item(tok, name, "Notebook")
    if not i:
        sys.exit(f"notebook not found: {name}")
    return i


def fail_handler(tok, pipeline_name, work_activity):
    """On <work_activity> failure: log to metadata lakehouse, then re-fail the pipeline
    (so parents/fail-fast propagate)."""
    log_act = nb("LogFailure", nbid(tok, "cp_log_fail"), {
        "pipeline_name": plit(pipeline_name),
        "run_id": pexpr("@pipeline().parameters.run_id"),
        "load_group": pexpr("@pipeline().parameters.load_group", "int"),
        "activity": plit(work_activity),
        "message": pexpr(f"@string(activity('{work_activity}').error)")},
        depends=[work_activity], cond="Failed")
    fail_act = {"name": "FailPipeline", "type": "Fail", "dependsOn": dep(["LogFailure"]),
                "typeProperties": {"message": f"{pipeline_name} failed - see pipeline_run_log",
                                   "errorCode": "CP_FAIL"}}
    return [log_act, fail_act]


def build_bronze(tok):
    plan = nbid(tok, "cp_plan")
    worker = nbid(tok, "bronze_worker")
    params = {"load_group": plit(1, "int"), "run_id": plit("manual"),
              "src_user": plit(""), "src_password": plit("")}
    plan_act = nb("Plan", plan, {
        "load_group": pexpr("@pipeline().parameters.load_group", "int"),
        "plan_type": plit("objects")})
    worker_act = nb("BronzeWorker", worker, {
        "run_id": pexpr("@pipeline().parameters.run_id"),
        "object_json": pexpr("@string(item())"),
        "src_user": pexpr("@pipeline().parameters.src_user"),
        "src_password": pexpr("@pipeline().parameters.src_password")})
    fe = foreach("ForEachObject", "@json(activity('Plan').output.result.exitValue)",
                 [worker_act], depends=["Plan"])
    return pipeline(params, [plan_act, fe] + fail_handler(tok, "cp_pl_bronze", "ForEachObject"))


def build_silver(tok):
    plan, worker = nbid(tok, "cp_plan"), nbid(tok, "silver_worker")
    params = {"load_group": plit(1, "int"), "run_id": plit("manual")}
    plan_act = nb("Plan", plan, {
        "load_group": pexpr("@pipeline().parameters.load_group", "int"),
        "plan_type": plit("objects")})
    worker_act = nb("SilverWorker", worker, {
        "run_id": pexpr("@pipeline().parameters.run_id"),
        "object_json": pexpr("@string(item())")})
    return pipeline(params, [plan_act, foreach(
        "ForEachObject", "@json(activity('Plan').output.result.exitValue)",
        [worker_act], depends=["Plan"])] + fail_handler(tok, "cp_pl_silver", "ForEachObject"))


def build_gold(tok):
    plan, worker = nbid(tok, "cp_plan"), nbid(tok, "gold_runner")
    params = {"load_group": plit(1, "int"), "run_id": plit("manual")}
    plan_act = nb("Plan", plan, {
        "load_group": pexpr("@pipeline().parameters.load_group", "int"),
        "plan_type": plit("models")})
    worker_act = nb("GoldRunner", worker, {
        "run_id": pexpr("@pipeline().parameters.run_id"),
        "model_id": pexpr("@item().model_id", "int")})
    # models are independent; run sequentially to keep the gold DAG stable
    return pipeline(params, [plan_act, foreach(
        "ForEachModel", "@json(activity('Plan').output.result.exitValue)",
        [worker_act], depends=["Plan"], batch=1)] + fail_handler(tok, "cp_pl_gold", "ForEachModel"))


def build_metadata(tok):
    worker = nbid(tok, "metadata_worker")
    params = {"load_group": plit(1, "int"), "run_id": plit("manual"),
              "src_user": plit(""), "src_password": plit("")}
    worker_act = nb("MetadataWorker", worker, {
        "run_id": pexpr("@pipeline().parameters.run_id"),
        "load_group": pexpr("@pipeline().parameters.load_group", "int"),
        "src_user": pexpr("@pipeline().parameters.src_user"),
        "src_password": pexpr("@pipeline().parameters.src_password")})
    return pipeline(params, [worker_act] + fail_handler(tok, "cp_pl_metadata", "MetadataWorker"))


def build_pbi(tok):
    plan = nbid(tok, "cp_plan")
    params = {"load_group": plit(1, "int"), "run_id": plit("manual")}
    plan_act = nb("Plan", plan, {
        "load_group": pexpr("@pipeline().parameters.load_group", "int"),
        "plan_type": plit("datasets")})
    web = {"name": "RefreshDataset", "type": "WebActivity",
           "typeProperties": {
               "url": expr("@concat('https://api.powerbi.com/v1.0/myorg/groups/', item().workspace_id, "
                           "'/datasets/', item().dataset_id, '/refreshes')"),
               "method": "POST", "body": "{}",
               "authentication": {"type": "MSI", "resource": "https://analysis.windows.net/powerbi/api"}}}
    return pipeline(params, [plan_act, foreach(
        "ForEachDataset", "@json(activity('Plan').output.result.exitValue)",
        [web], depends=["Plan"], batch=1)] + fail_handler(tok, "cp_pl_pbi", "ForEachDataset"))


def execpl(name, pipeline_id, params, depends=None):
    return {"name": name, "type": "ExecutePipeline", "dependsOn": dep(depends),
            "typeProperties": {"pipeline": {"referenceName": pipeline_id, "type": "PipelineReference"},
                               "waitOnCompletion": True, "parameters": params}}


def step_active(step_key):
    # planner returns {step_key: is_active}; select by property name (no filter/item())
    return f"@bool(json(activity('PlanSteps').output.result.exitValue).{step_key})"


def build_main(tok):
    plan = nbid(tok, "cp_plan")
    ids = {k: FN.find_item(tok, k, "DataPipeline") for k in
           ["cp_pl_metadata", "cp_pl_bronze", "cp_pl_silver", "cp_pl_gold", "cp_pl_pbi"]}
    params = {"load_group": plit(1, "int"), "run_id": plit("manual"),
              "src_user": plit(""), "src_password": plit("")}

    def cparams(creds=False):
        p = {"load_group": expr("@pipeline().parameters.load_group"),
             "run_id": expr("@pipeline().parameters.run_id")}
        if creds:
            p["src_user"] = expr("@pipeline().parameters.src_user")
            p["src_password"] = expr("@pipeline().parameters.src_password")
        return p

    plan_act = nb("PlanSteps", plan, {
        "load_group": pexpr("@pipeline().parameters.load_group", "int"),
        "plan_type": plit("steps")})
    # (step_key, child pipeline, needs source creds) — sequential, fail-fast, is_active-gated
    steps = [("load_metadata", "cp_pl_metadata", True), ("load_bronze", "cp_pl_bronze", True),
             ("load_silver", "cp_pl_silver", False), ("load_gold", "cp_pl_gold", False),
             ("refresh_pbi", "cp_pl_pbi", False)]
    activities, prev = [plan_act], "PlanSteps"
    for step_key, pl, creds in steps:
        ifname = "If_" + step_key
        inv = execpl("Invoke_" + step_key, ids[pl], cparams(creds))
        activities.append(if_cond(ifname, step_active(step_key), [inv], depends=[prev]))
        prev = ifname
    return pipeline(params, activities)


BUILDERS = {
    "cp_pl_metadata": build_metadata,
    "cp_pl_bronze": build_bronze,
    "cp_pl_silver": build_silver,
    "cp_pl_gold": build_gold,
    "cp_pl_pbi": build_pbi,
    "cp_pl_main": build_main,
}


if __name__ == "__main__":
    tok = FN.token()
    names = sys.argv[1:] or MF.PIPELINES          # manifest order (children before main)
    for name in names:
        if name in BUILDERS:
            print(f"deployed {name}:", deploy_pipeline(tok, name, BUILDERS[name](tok)))
