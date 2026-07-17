"""Shared constants + OneLake helpers for the control plane (GUID paths only).

OneLake workspace-name paths are unreliable; always address by GUID.
"""

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

TENANT = os.getenv("AZURE_TENANT_ID")
WS_ID = os.getenv("FABRIC_WORKSPACE_ID", "79f30543-1f5c-451f-9ef7-b46b24dc7223")

# Lakehouse GUIDs in the Hackathon-DEV workspace.
LH = {
    "config":  "35cd447c-1572-4726-afd6-54e01062659b",  # metadata
    "bronze":  "f0154c3a-4e5e-4245-8ee9-2c3e565f8ff6",
    "silver":  "d586e0f5-5b37-4eaa-b5a6-62dfdba65765",
    "gold":    "16b4141e-62ef-4cbc-a424-88e1a0f963bf",
}
# stage tables live in gold, quarantine tables live in silver (name-prefixed).
STAGE_LH, QUAR_LH = LH["gold"], LH["silver"]

REPO = Path(r"C:\Users\Shuo\OneDrive\文档\bcpensionshackathon")
CONFIG_DIR = REPO / "control_plane" / "config"


def storage_token():
    cmd = ["az", "account", "get-access-token", "--resource",
           "https://storage.azure.com", "--query", "accessToken", "-o", "tsv"]
    if TENANT:
        cmd[3:3] = ["--tenant", TENANT]
    out = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    if out.returncode or not out.stdout.strip():
        sys.exit(f"az token error: {out.stderr.strip()}")
    return out.stdout.strip()


def so(token):
    return {"bearer_token": token, "account_name": "onelake", "use_fabric_endpoint": "true"}


def path(lh_guid, table):
    return (f"abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{lh_guid}/Tables/{table}")


def write_delta(lh_guid, table, df, token, mode="overwrite"):
    from deltalake import write_deltalake
    write_deltalake(path(lh_guid, table), df, mode=mode,
                    storage_options=so(token), schema_mode="overwrite")
    return len(df)


def read_delta(lh_guid, table, token):
    from deltalake import DeltaTable
    return DeltaTable(path(lh_guid, table), storage_options=so(token)).to_pandas()


def table_exists(lh_guid, table, token):
    import requests
    r = requests.get(
        f"https://onelake.dfs.fabric.microsoft.com/{WS_ID}/{lh_guid}/Tables/{table}"
        f"/_delta_log/00000000000000000000.json",
        headers={"Authorization": f"Bearer {token}"})
    return r.status_code == 200
