"""Create/update the cp_vars Variable Library from the repo definition folder."""
import base64
import sys
import time
from pathlib import Path

import requests

import cp_common as C

VL_DIR = C.REPO / "control_plane" / "variable_library" / "cp_vars.VariableLibrary"
API = "https://api.fabric.microsoft.com/v1"


def _part(rel):
    p = VL_DIR / rel
    payload = base64.b64encode(p.read_bytes()).decode()
    return {"path": rel.replace("\\", "/"), "payload": payload, "payloadType": "InlineBase64"}


def build_definition():
    parts = [_part("variables.json"), _part("settings.json"), _part(".platform")]
    for vs in (VL_DIR / "valueSets").glob("*.json"):
        parts.append(_part(f"valueSets/{vs.name}"))
    return {"parts": parts}


def find(tok, name):
    h = {"Authorization": f"Bearer {tok}"}
    for i in requests.get(f"{API}/workspaces/{C.WS_ID}/items", headers=h).json()["value"]:
        if i["displayName"] == name and i["type"] == "VariableLibrary":
            return i["id"]
    return None


def _wait(resp, tok):
    if resp.status_code == 202:
        loc = resp.headers.get("Location")
        while loc:
            time.sleep(int(resp.headers.get("Retry-After", 3)))
            p = requests.get(loc, headers={"Authorization": f"Bearer {tok}"})
            if p.json().get("status") in ("Succeeded", "Completed"):
                return
            if p.json().get("status") == "Failed":
                sys.exit(f"LRO failed: {p.text}")
    elif resp.status_code not in (200, 201):
        sys.exit(f"[{resp.status_code}] {resp.text}")


def main():
    tok = C.fabric_token()
    h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    definition = build_definition()
    iid = find(tok, "cp_vars")
    if iid:
        r = requests.post(f"{API}/workspaces/{C.WS_ID}/items/{iid}/updateDefinition",
                          headers=h, json={"definition": definition})
        _wait(r, tok)
        print("updated cp_vars", iid)
    else:
        r = requests.post(f"{API}/workspaces/{C.WS_ID}/items", headers=h,
                          json={"displayName": "cp_vars", "type": "VariableLibrary",
                                "definition": definition})
        _wait(r, tok)
        print("created cp_vars:", find(tok, "cp_vars"))


if __name__ == "__main__":
    main()
