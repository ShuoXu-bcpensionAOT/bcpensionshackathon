"""Key Vault access for the local deploy tooling (mirror of cp_framework.get_secret, which uses
notebookutils inside Fabric). Reads via the cp_auth token (SPN or personal az login). Writing a
secret needs a principal with KV 'set' — pass a personal-account token (the SPN is read-only)."""
import os

import requests

import cp_auth

API_VERSION = "7.4"


def vault_url():
    url = os.getenv("Key_Vault_URL") or os.getenv("KEY_VAULT_URL")
    return url.rstrip("/") if url else None


def get_secret(name, vault=None, token=None):
    vault = (vault or vault_url())
    if not vault:
        raise SystemExit("Key_Vault_URL not set in .env")
    tok = token or cp_auth.get_token("https://vault.azure.net")
    r = requests.get(f"{vault}/secrets/{name}?api-version={API_VERSION}",
                     headers={"Authorization": f"Bearer {tok}"})
    if r.status_code != 200:
        raise SystemExit(f"get secret '{name}' failed [{r.status_code}]: {r.text[:200]}")
    return r.json()["value"]


def set_secret(name, value, vault=None, token=None):
    """Write a secret. token must belong to a principal with KV 'set' (personal account)."""
    vault = (vault or vault_url())
    tok = token or cp_auth.get_token("https://vault.azure.net")
    r = requests.put(f"{vault}/secrets/{name}?api-version={API_VERSION}",
                     headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
                     json={"value": value})
    if r.status_code not in (200, 201):
        raise SystemExit(f"set secret '{name}' failed [{r.status_code}]: {r.text[:200]}")
    return r.json()["id"]
