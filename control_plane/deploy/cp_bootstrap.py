"""Promote the control plane to a target environment.

Inputs: workspace base name + environment name (e.g. HackathonShuo UAT).
Finds-or-creates the `<base>-<env>` workspace on the trial capacity, creates the
lakehouses, then drives the existing tooling (var lib, deploy, config, run) against
it. Uses the personal az login now; swap in a service principal later with no
other change. Source DB password is injected at run time from .env (KV later).

    python cp_bootstrap.py HackathonShuo UAT
"""
import os
import subprocess
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

TENANT = os.getenv("AZURE_TENANT_ID")
API = "https://api.fabric.microsoft.com/v1"
TRIAL_CAPACITY = "42092329-66a5-4754-93df-fb5cb58fa305"
SCRIPTS = Path(__file__).resolve().parent
LAKEHOUSES = ["metadata", "bronze", "silver", "gold"]


def token():
    return subprocess.run(
        ["az", "account", "get-access-token", "--tenant", TENANT, "--resource",
         "https://api.fabric.microsoft.com", "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, shell=True).stdout.strip()


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


def ensure_workspace(t, name):
    for w in requests.get(f"{API}/workspaces", headers=H(t)).json()["value"]:
        if w["displayName"] == name:
            print(f"workspace exists: {name} ({w['id']})")
            return w["id"]
    r = requests.post(f"{API}/workspaces", headers=H(t), json={"displayName": name})
    if r.status_code not in (200, 201):
        sys.exit(f"create workspace failed: {r.text}")
    wid = r.json()["id"]
    a = requests.post(f"{API}/workspaces/{wid}/assignToCapacity", headers=H(t),
                      json={"capacityId": TRIAL_CAPACITY})
    wait_lro(a, t)
    print(f"created workspace {name} ({wid}) on trial capacity")
    return wid


def ensure_lakehouses(t, wid):
    existing = {i["displayName"] for i in
                requests.get(f"{API}/workspaces/{wid}/items", headers=H(t)).json()["value"]
                if i["type"] == "Lakehouse"}
    for n in LAKEHOUSES:
        if n in existing:
            print(f"  lakehouse {n} exists")
            continue
        r = requests.post(f"{API}/workspaces/{wid}/lakehouses", headers=H(t),
                          json={"displayName": n})
        wait_lro(r, t)
        print(f"  created lakehouse {n}")


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
    ensure_lakehouses(t, wid)
    time.sleep(10)  # let OneLake endpoints settle

    envset = {"CP_TARGET_WORKSPACE": name, "CP_TARGET_WORKSPACE_ID": wid}
    step(envset, "cp_varlib.py")
    step(envset, "cp_deploy.py", "deploy")
    step(envset, "cp_config.py")
    step(envset, "cp_deploy.py", "run", "cp_09_orchestrate", f"run_id={env_name.lower()}_e2e")
    print(f"\nBOOTSTRAP COMPLETE: {name} ({wid})")


if __name__ == "__main__":
    main()
