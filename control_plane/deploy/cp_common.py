"""Shared helpers for the control plane (local tooling). No hardcoded IDs.

Workspace + lakehouse IDs are resolved by NAME via the Fabric API against the
workspace named by FABRIC_WORKSPACE_NAME (default HackathonShuo-DEV). OneLake is
always addressed by GUID (name paths are unreliable) — but those GUIDs are
discovered at runtime, never hardcoded.
"""

import os
import subprocess
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

TENANT = os.getenv("AZURE_TENANT_ID")
# CP_TARGET_WORKSPACE lets the bootstrap point all tooling at another workspace (e.g. UAT).
WS_NAME = os.getenv("CP_TARGET_WORKSPACE") or os.getenv("FABRIC_WORKSPACE_NAME", "HackathonShuo-DEV")
FABRIC_API = "https://api.fabric.microsoft.com/v1"

# logical layer -> physical lakehouse name
LAYER_NAMES = {"config": "metadata", "bronze": "bronze", "silver": "silver", "gold": "gold"}

REPO = Path(r"C:\Users\Shuo\OneDrive\文档\bcpensionshackathon")
CONFIG_DIR = REPO / "control_plane" / "config"


def _token(resource):
    cmd = ["az", "account", "get-access-token", "--resource", resource,
           "--query", "accessToken", "-o", "tsv"]
    if TENANT:
        cmd[3:3] = ["--tenant", TENANT]
    out = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    if out.returncode or not out.stdout.strip():
        sys.exit(f"az token error: {out.stderr.strip()}")
    return out.stdout.strip()


def storage_token():
    return _token("https://storage.azure.com")


def fabric_token():
    return _token("https://api.fabric.microsoft.com")


def _resolve():
    """Resolve WS_ID + lakehouse GUIDs by name. Cached at import."""
    tok = fabric_token()
    h = {"Authorization": f"Bearer {tok}"}
    wss = requests.get(f"{FABRIC_API}/workspaces", headers=h).json()["value"]
    ws = next((w for w in wss if w["displayName"] == WS_NAME), None)
    if not ws:
        sys.exit(f"workspace not found: {WS_NAME}")
    ws_id = ws["id"]
    lhs = requests.get(f"{FABRIC_API}/workspaces/{ws_id}/lakehouses", headers=h).json()["value"]
    by_name = {l["displayName"]: l["id"] for l in lhs}
    lh = {logical: by_name[name] for logical, name in LAYER_NAMES.items() if name in by_name}
    return ws_id, lh


WS_ID, LH = _resolve()
STAGE_LH, QUAR_LH = LH.get("gold"), LH.get("silver")


def so(token):
    return {"bearer_token": token, "account_name": "onelake", "use_fabric_endpoint": "true"}


def path(lh_guid, table):
    return f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{lh_guid}/Tables/{table}"


def write_delta(lh_guid, table, df, token, mode="overwrite"):
    from deltalake import write_deltalake
    write_deltalake(path(lh_guid, table), df, mode=mode,
                    storage_options=so(token), schema_mode="overwrite")
    return len(df)


def read_delta(lh_guid, table, token):
    from deltalake import DeltaTable
    return DeltaTable(path(lh_guid, table), storage_options=so(token)).to_pandas()
