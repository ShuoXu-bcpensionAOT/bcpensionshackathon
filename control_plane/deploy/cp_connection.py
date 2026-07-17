"""Create (or reuse) a Fabric cloud Connection for the source SQL Server.

The password is read from .env and stored in the connection — never committed.
allowUsageInUserControlledCode=true so notebooks may use it.
Usage: python cp_connection.py [connection_name] [database]
"""
import os
import sys

import requests

import cp_common as C

API = "https://api.fabric.microsoft.com/v1"


def find(tok, name):
    h = {"Authorization": f"Bearer {tok}"}
    r = requests.get(f"{API}/connections", headers=h)
    for c in r.json().get("value", []):
        if c.get("displayName") == name:
            return c["id"]
    return None


def create(name, database):
    tok = C.fabric_token()
    h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    existing = find(tok, name)
    if existing:
        print(f"connection '{name}' exists: {existing}")
        return existing
    body = {
        "connectivityType": "ShareableCloud",
        "displayName": name,
        "connectionDetails": {
            "type": "SQL", "creationMethod": "SQL",
            "parameters": [
                {"dataType": "Text", "name": "server", "value": os.getenv("SOURCE_DB")},
                {"dataType": "Text", "name": "database", "value": database},
            ],
        },
        "privacyLevel": "Organizational",
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "credentials": {
                "credentialType": "Basic",
                "username": os.getenv("USERNAME"),
                "password": os.getenv("PASSWORD"),
            },
        },
        "allowUsageInUserControlledCode": True,
    }
    r = requests.post(f"{API}/connections", headers=h, json=body)
    if r.status_code not in (200, 201):
        sys.exit(f"create connection failed [{r.status_code}]: {r.text}")
    cid = r.json()["id"]
    print(f"created connection '{name}': {cid}")
    return cid


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "src_adventureworks"
    database = sys.argv[2] if len(sys.argv) > 2 else "AdventureWorks2025"
    create(name, database)
