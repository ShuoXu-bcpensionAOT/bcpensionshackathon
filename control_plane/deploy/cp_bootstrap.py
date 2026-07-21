"""Promote the control plane to a target environment.

Inputs: workspace base name + environment name (e.g. HackathonShuo UAT).
Finds-or-creates the `<base>-<env>` workspace on the trial capacity, creates the
lakehouses, then drives the existing tooling (var lib, deploy, config, run) against
it. Auth via cp_auth: a service principal (SPN_CLIENT_ID/SECRET or AZURE_CLIENT_ID/
SECRET) when configured, else the personal az login. Source DB password is injected
at run time from .env (KV later).

    python cp_bootstrap.py HackathonShuo UAT
"""
import os
import subprocess
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

import cp_auth
import cp_environment as CE
import cp_manifest as MF

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

TENANT = os.getenv("AZURE_TENANT_ID")
API = "https://api.fabric.microsoft.com/v1"
CAPACITY_ID = os.getenv("CP_CAPACITY_ID", "42092329-66a5-4754-93df-fb5cb58fa305")
SCRIPTS = Path(__file__).resolve().parent
LAKEHOUSES = MF.LAKEHOUSES


def token(resource="https://api.fabric.microsoft.com"):
    # SP (client-credentials) when configured, else personal az login — see cp_auth.
    return cp_auth.get_token(resource)


def H(t):
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}


def wait_lro(resp, t):
    if resp.status_code == 202:
        loc = resp.headers.get("Location")
        while loc:
            time.sleep(int(resp.headers.get("Retry-After", 3)))
            p = requests.get(loc, headers={"Authorization": f"Bearer {t}"})
            st = p.json().get("status") if p.content else None
            if st in ("Succeeded", "Completed"):
                return p
            if st == "Failed":
                sys.exit(f"LRO failed: {p.text}")
    elif resp.status_code not in (200, 201):
        sys.exit(f"[{resp.status_code}] {resp.text}")
    return resp


# Shown when the SP is not permitted to create a workspace (tenant setting off).
WORKSPACE_CREATE_HELP = """
  ------------------------------------------------------------------------------
  The service principal is NOT permitted to CREATE workspaces (Fabric returns 401).
  This is a tenant-level setting, separate from workspace access. To fix, EITHER:
    A) A Fabric/Power BI admin enables the tenant setting
       'Service principals can create workspaces, connections, and deployment
       pipelines' (Admin portal > Tenant settings > Developer settings) and adds
       this SP to the allowed security group — then the SP self-provisions. This
       same setting also unblocks creating the config_db connection (native SQL
       Lookups).  [preferred, revisit once permission allows]
    B) Pre-create the workspace as a user: re-run with CP_PROVISION_AS_USER=1 so
       the workspace + capacity assignment + SP-admin grant happen under your az
       login, then the SP takes over the rest of the deploy.
  ------------------------------------------------------------------------------"""


def _user_token(resource="https://api.fabric.microsoft.com"):
    """Personal az (human) token — bypasses cp_auth's SP path. Used only to seed a
    workspace when the SP itself may not create one (CP_PROVISION_AS_USER)."""
    cmd = ["az", "account", "get-access-token", "--resource", resource,
           "--query", "accessToken", "-o", "tsv"]
    if TENANT:
        cmd[3:3] = ["--tenant", TENANT]
    out = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    if out.returncode or not out.stdout.strip():
        sys.exit(f"az user token error: {out.stderr.strip()}")
    return out.stdout.strip()


def ensure_workspace(t, name):
    for w in requests.get(f"{API}/workspaces", headers=H(t)).json()["value"]:
        if w["displayName"] == name:
            print(f"workspace exists: {name} ({w['id']})")
            return w["id"]
    ct = t                                              # token used for create + capacity
    r = requests.post(f"{API}/workspaces", headers=H(t), json={"displayName": name})
    if r.status_code in (401, 403):                     # SP not allowed to create workspaces
        print(WORKSPACE_CREATE_HELP)
        if os.getenv("CP_PROVISION_AS_USER", "").lower() not in ("1", "true", "yes"):
            sys.exit("create workspace failed: SP not permitted. Set CP_PROVISION_AS_USER=1 "
                     "to provision as your az user, or enable the tenant setting (A above).")
        print("  CP_PROVISION_AS_USER set — creating workspace with your az login")
        ct = _user_token()
        r = requests.post(f"{API}/workspaces", headers=H(ct), json={"displayName": name})
    if r.status_code not in (200, 201):
        sys.exit(f"create workspace failed: {r.text}")
    wid = r.json()["id"]
    a = requests.post(f"{API}/workspaces/{wid}/assignToCapacity", headers=H(ct),
                      json={"capacityId": CAPACITY_ID})
    wait_lro(a, ct)
    print(f"created workspace {name} ({wid}) on trial capacity")
    if ct is not t:                                     # user-provisioned: grant SP access now
        ensure_sp_admin(ct, wid)                        # under the human token
        print("  waiting for SP role to propagate…")
        time.sleep(20)
    return wid


def ensure_sp_admin(t, wid):
    """Grant the deploy service principal the workspace Admin role (idempotent).
    Lets a human (or another principal) provision a workspace that the SP will then
    deploy to / run. No-op when SPN_OBJECT_ID is unset or the SP is already assigned
    (e.g. it created the workspace itself). Warns rather than fails so provisioning
    continues even if the caller lacks role-assignment rights."""
    sp_obj = os.getenv("SPN_OBJECT_ID")
    if not sp_obj:
        print("  (SPN_OBJECT_ID unset — skipping SP admin grant)")
        return
    existing = requests.get(f"{API}/workspaces/{wid}/roleAssignments", headers=H(t))
    if existing.status_code == 200:
        for a in existing.json().get("value", []):
            if a.get("principal", {}).get("id") == sp_obj:
                print(f"  SP already {a.get('role')} on workspace")
                return
    r = requests.post(f"{API}/workspaces/{wid}/roleAssignments", headers=H(t),
                      json={"principal": {"id": sp_obj, "type": "ServicePrincipal"},
                            "role": "Admin"})
    if r.status_code in (200, 201):
        print("  granted SP the workspace Admin role")
    elif r.status_code in (400, 409) and "already" in r.text.lower():
        print("  SP already has a workspace role")
    else:
        print(f"  WARN: could not grant SP admin [{r.status_code}] {r.text[:200]}")


def ensure_lakehouses(t, wid):
    existing = {i["displayName"] for i in
                requests.get(f"{API}/workspaces/{wid}/items", headers=H(t)).json()["value"]
                if i["type"] == "Lakehouse"}
    for n in LAKEHOUSES:
        if n in existing:
            print(f"  lakehouse {n} exists")
            continue
        r = requests.post(f"{API}/workspaces/{wid}/lakehouses", headers=H(t),
                          json={"displayName": n, "creationPayload": {"enableSchemas": True}})
        wait_lro(r, t)
        print(f"  created lakehouse {n} (schema-enabled)")


def ensure_sqldb(t, wid, name="config_db"):
    h = H(t)
    for d in requests.get(f"{API}/workspaces/{wid}/SqlDatabases", headers=h).json().get("value", []):
        if d["displayName"] == name:
            print(f"  sqldb {name} exists")
            return d["id"]
    r = requests.post(f"{API}/workspaces/{wid}/items", headers=h,
                      json={"displayName": name, "type": "SQLDatabase",
                            "creationPayload": {"creationMode": "new"}})
    wait_lro(r, t)
    for _ in range(20):
        for d in requests.get(f"{API}/workspaces/{wid}/SqlDatabases", headers=h).json().get("value", []):
            if d["displayName"] == name:
                print(f"  created sqldb {name}")
                return d["id"]
        time.sleep(15)
    sys.exit("sqldb not found after create")


def wait_for_mirror(wid, sqldb_id, tables, timeout=420):
    """Poll the SQL DB's OneLake mirror until the config tables have replicated."""
    stok = token("https://storage.azure.com")
    h = {"Authorization": f"Bearer {stok}"}
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(requests.get(
                f"https://onelake.dfs.fabric.microsoft.com/{wid}/{sqldb_id}/Tables/dbo/{t}"
                f"/_delta_log/00000000000000000000.json", headers=h).status_code == 200
               for t in tables):
            print("  config mirror ready")
            return
        time.sleep(15)
    print("  WARN: config mirror not confirmed within timeout")


def ensure_folder(t, wid, name, parent=None):
    for f in requests.get(f"{API}/workspaces/{wid}/folders", headers=H(t)).json().get("value", []):
        if f["displayName"] == name and f.get("parentFolderId") == parent:
            return f["id"]
    body = {"displayName": name}
    if parent:
        body["parentFolderId"] = parent
    return requests.post(f"{API}/workspaces/{wid}/folders", headers=H(t), json=body).json()["id"]


def _move(t, wid, item_id, folder_id):
    for a in range(6):
        r = requests.post(f"{API}/workspaces/{wid}/items/{item_id}/move",
                          headers=H(t), json={"targetFolderId": folder_id})
        if r.status_code != 429:
            return
        time.sleep(int(r.headers.get("Retry-After", 8)) + 2 * a)


def move_pipelines(t, wid):
    fid = ensure_folder(t, wid, "pipeline")
    items = [i for i in requests.get(f"{API}/workspaces/{wid}/items", headers=H(t)).json()["value"]
             if i["type"] == "DataPipeline"]
    for i in items:
        _move(t, wid, i["id"], fid)
    print(f"  moved {len(items)} pipeline(s) into 'pipeline' folder")


# Superseded notebooks (from the manifest) — removed from every environment on deploy.
SUPERSEDED_NOTEBOOKS = MF.SUPERSEDED_NOTEBOOKS


def remove_superseded(t, wid):
    items = {i["displayName"]: i for i in
             requests.get(f"{API}/workspaces/{wid}/items", headers=H(t)).json()["value"]}
    for n in SUPERSEDED_NOTEBOOKS:
        if n in items and items[n]["type"] == "Notebook":
            requests.delete(f"{API}/workspaces/{wid}/items/{items[n]['id']}",
                            headers={"Authorization": f"Bearer {t}"})
            print(f"  removed superseded notebook: {n}")


# notebook -> subfolder placement (from the manifest; keeps deploys tidy across envs)
NB_FOLDERS = MF.NB_FOLDERS


def organize_notebooks(t, wid):
    nbf = ensure_folder(t, wid, "notebook")
    fmap = {"notebook": nbf}
    for folder in NB_FOLDERS:                       # subfolders derived from the manifest
        if folder != "notebook":
            fmap[folder] = ensure_folder(t, wid, folder, nbf)
    items = {i["displayName"]: i["id"] for i in
             requests.get(f"{API}/workspaces/{wid}/items", headers=H(t)).json()["value"]
             if i["type"] == "Notebook"}
    n = 0
    for folder, names in NB_FOLDERS.items():
        for name in names:
            if name in items:
                _move(t, wid, items[name], fmap[folder])
                n += 1
    print(f"  organized {n} notebook(s) into notebook/utility/sourcequery")


def step(envset, *args):
    e = dict(os.environ)
    e.update(envset)
    print(f"\n>>> {' '.join(args)}")
    if subprocess.run([sys.executable, *args], cwd=str(SCRIPTS), env=e).returncode != 0:
        sys.exit(f"step failed: {args}")


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else "HackathonShuo"
    env_name = sys.argv[2] if len(sys.argv) > 2 else "UAT"
    name = f"{base}-{env_name}"

    t = token()
    wid = ensure_workspace(t, name)
    ensure_sp_admin(t, wid)                            # grant deploy SP admin (idempotent)
    ensure_lakehouses(t, wid)
    # ORDERING INVARIANT: the driver Environment must be created AND fully published BEFORE any
    # notebook that binds to it is deployed — a notebook bound to an unpublished/absent env fails
    # at run time. CE.provision blocks until publish is confirmed (and aborts the deploy if it is
    # not), so it runs here, ahead of cp_deploy below.
    env_id = None
    if MF.ENVIRONMENT:                                 # opt-in driver Environment (manifest block)
        print("provisioning driver Environment (publish takes minutes; blocks until ready)...")
        env_id = CE.provision(wid, MF.ENVIRONMENT)
    sqldb_id = ensure_sqldb(t, wid, MF.SQL_DATABASE)
    time.sleep(10)  # let OneLake endpoints settle

    envset = {"CP_TARGET_WORKSPACE": name, "CP_TARGET_WORKSPACE_ID": wid}
    if env_id:                                         # attach the driver env to connector notebooks
        envset["CP_ENV_ID"] = env_id
        envset["CP_ENV_ATTACH"] = ",".join(MF.ENVIRONMENT.get("attach", ["bronze_worker"]))
    step(envset, "cp_varlib.py")                       # variable library
    step(envset, "cp_deploy.py", "deploy")             # framework + worker notebooks
    remove_superseded(t, wid)                          # prune old orchestrator notebooks
    organize_notebooks(t, wid)                         # -> notebook/utility/sourcequery
    step(envset, "cp_pipeline.py")                     # main + child data pipelines
    move_pipelines(t, wid)                             # -> 'pipeline' folder
    step(envset, "cp_config.py")                       # config-as-code -> config SQL DB
    wait_for_mirror(wid, sqldb_id, ["datasource", "gold_dependency"])
    print(f"\nBOOTSTRAP COMPLETE (deploy-only): {name} ({wid})")
    print("Run the pipeline with: cp_pl_main (load_group, run_id, src_user, src_password)")


if __name__ == "__main__":
    main()
