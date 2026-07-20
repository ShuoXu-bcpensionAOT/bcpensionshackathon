"""Provision a Fabric Environment (OPT-IN) that pre-loads extra ingestion drivers.

Why: Postgres/MySQL/SQL Server JDBC jars are already bundled in the Fabric runtime, and the
pure-Python Oracle/DB2 connectors self-install on demand — so NO environment is required for
plug-and-play. This is only for teams who want (a) the Python drivers pre-installed (zero
per-session pip) or (b) the Oracle/DB2 JDBC jars for DISTRIBUTED Spark JDBC on large tables
(connection_json.mode='jdbc'). Those jars are proprietary — the operator supplies them.

Config via the manifest `environment` block (see cp_manifest); cp_bootstrap calls provision()
when present. `set_default` is off by default — a custom workspace default env adds session
startup time for ALL notebooks.

Endpoints verified against the Fabric REST API:
  POST   /workspaces/{wid}/environments                          create
  POST   /workspaces/{wid}/environments/{eid}/staging/libraries  upload (multipart): environment.yml | *.jar/*.whl
  POST   /workspaces/{wid}/environments/{eid}/staging/publish    publish (build image; minutes)
  PATCH  /workspaces/{wid}/spark/settings                        {environment:{name}} -> workspace default
"""
import os
import sys
import time

import requests

import cp_auth

API = "https://api.fabric.microsoft.com/v1"


def _t():
    return cp_auth.get_token("https://api.fabric.microsoft.com")


def _H(t):
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}


def _Ha(t):
    return {"Authorization": f"Bearer {t}"}


def find_env(t, wid, name):
    for e in requests.get(f"{API}/workspaces/{wid}/environments", headers=_H(t)).json().get("value", []):
        if e["displayName"] == name:
            return e["id"]
    return None


def ensure_environment(t, wid, name):
    eid = find_env(t, wid, name)
    if eid:
        print(f"  environment exists: {name}")
        return eid
    r = requests.post(f"{API}/workspaces/{wid}/environments", headers=_H(t), json={"displayName": name})
    if r.status_code not in (200, 201):
        sys.exit(f"create environment failed: {r.text}")
    print(f"  created environment: {name}")
    return r.json()["id"]


def stage_public_libs(t, wid, eid, pip_pkgs):
    """Stage PyPI packages (pip) as an environment.yml public-library spec."""
    if not pip_pkgs:
        return
    yml = "dependencies:\n  - pip:\n" + "".join(f"    - {p}\n" for p in pip_pkgs)
    r = requests.post(f"{API}/workspaces/{wid}/environments/{eid}/staging/libraries", headers=_Ha(t),
                      files={"file": ("environment.yml", yml.encode(), "application/x-yaml")})
    if r.status_code not in (200, 201):
        sys.exit(f"stage public libs failed: {r.text}")
    print(f"  staged pip libs: {pip_pkgs}")


def stage_jars(t, wid, eid, jar_paths):
    """Stage custom JDBC driver jars (operator-supplied, e.g. ojdbc11.jar, db2jcc4.jar)."""
    for p in jar_paths:
        if not os.path.exists(p):
            print(f"  WARN: jar not found, skipping: {p}")
            continue
        with open(p, "rb") as f:
            r = requests.post(f"{API}/workspaces/{wid}/environments/{eid}/staging/libraries",
                              headers=_Ha(t),
                              files={"file": (os.path.basename(p), f.read(), "application/java-archive")})
        if r.status_code not in (200, 201):
            sys.exit(f"stage jar {p} failed: {r.text}")
        print(f"  staged jar: {os.path.basename(p)}")


def publish(t, wid, eid, timeout=1800):
    r = requests.post(f"{API}/workspaces/{wid}/environments/{eid}/staging/publish", headers=_H(t))
    if r.status_code not in (200, 201, 202):
        sys.exit(f"publish failed: {r.text}")
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(20)
        e = requests.get(f"{API}/workspaces/{wid}/environments/{eid}", headers=_H(t)).json()
        state = ((e.get("properties", {}) or {}).get("publishDetails", {}) or {}).get("state")
        if state in ("Success", "Succeeded"):
            print("  published")
            return
        if state in ("Failed", "Cancelled"):
            sys.exit(f"publish ended in state {state}: {e}")
    print("  WARN: publish not confirmed within timeout")


def set_workspace_default(t, wid, name):
    r = requests.patch(f"{API}/workspaces/{wid}/spark/settings", headers=_H(t),
                       json={"environment": {"name": name}})
    print(f"  set workspace default environment -> {name}: [{r.status_code}]")


def provision(wid, conf):
    """conf: {name, pip:[...], jars:[...paths...], set_default:bool}. Returns the environment id."""
    t = _t()
    name = conf["name"]
    eid = ensure_environment(t, wid, name)
    stage_public_libs(t, wid, eid, conf.get("pip", []))
    stage_jars(t, wid, eid, [os.path.expandvars(p) for p in conf.get("jars", [])])
    publish(t, wid, eid)
    if conf.get("set_default"):
        set_workspace_default(t, wid, name)
    return eid


if __name__ == "__main__":
    # ad-hoc: python cp_environment.py <workspace_id> <env_name> [pip1,pip2] [jar1,jar2] [set_default]
    wid = sys.argv[1]
    conf = {
        "name": sys.argv[2] if len(sys.argv) > 2 else "cp_ingest_drivers",
        "pip": (sys.argv[3].split(",") if len(sys.argv) > 3 and sys.argv[3] else []),
        "jars": (sys.argv[4].split(",") if len(sys.argv) > 4 and sys.argv[4] else []),
        "set_default": (len(sys.argv) > 5 and sys.argv[5].lower() in ("1", "true", "yes")),
    }
    print("provisioned env:", provision(wid, conf))
