"""Unified token acquisition for the control-plane tooling.

Prefers a **service principal** (client-credentials OAuth) when SPN_CLIENT_ID /
SPN_CLIENT_SECRET (or AZURE_CLIENT_ID / AZURE_CLIENT_SECRET) are set — no az CLI,
so it works headless in CI. Falls back to `az account get-access-token` (personal
MFA login) when no SP is configured. Same resource convention as az: callers pass a
bare resource (e.g. https://api.fabric.microsoft.com or https://database.windows.net/);
the OAuth path appends '/.default'.
"""
import os
import subprocess
import sys
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
    for _p in (Path(__file__).resolve().parent.parent / ".env",
               Path(__file__).resolve().parent.parent.parent / ".env",
               Path(os.getenv("CP_REPO", "")) / ".env"):
        if _p and _p.exists():
            load_dotenv(_p, override=False)
except Exception:
    pass


def _tenant():
    return os.getenv("AZURE_TENANT_ID")


def _sp_creds():
    cid = os.getenv("SPN_CLIENT_ID") or os.getenv("AZURE_CLIENT_ID")
    sec = os.getenv("SPN_CLIENT_SECRET") or os.getenv("AZURE_CLIENT_SECRET")
    return (cid, sec) if (cid and sec and _tenant()) else (None, None)


def use_sp():
    """True when a service principal will be used for auth."""
    return _sp_creds()[0] is not None


def auth_mode():
    return "service-principal" if use_sp() else "az-cli"


def _scope(resource):
    return resource if resource.endswith("/.default") else resource.rstrip("/") + "/.default"


def get_token(resource="https://api.fabric.microsoft.com"):
    cid, sec = _sp_creds()
    if cid:
        r = requests.post(
            f"https://login.microsoftonline.com/{_tenant()}/oauth2/v2.0/token",
            data={"grant_type": "client_credentials", "client_id": cid,
                  "client_secret": sec, "scope": _scope(resource)})
        if r.status_code != 200:
            sys.exit(f"SP token error [{r.status_code}]: {r.text[:300]}")
        return r.json()["access_token"]
    # fall back to the personal az login
    cmd = ["az", "account", "get-access-token", "--resource", resource,
           "--query", "accessToken", "-o", "tsv"]
    if _tenant():
        cmd[3:3] = ["--tenant", _tenant()]
    out = subprocess.run(cmd, capture_output=True, text=True, shell=True)
    if out.returncode or not out.stdout.strip():
        sys.exit(f"az token error: {out.stderr.strip()}")
    return out.stdout.strip()
