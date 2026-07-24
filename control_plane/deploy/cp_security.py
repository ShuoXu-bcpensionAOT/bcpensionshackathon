"""Apply data-security / governance policies per environment — CODE-DRIVEN & PROMOTABLE.

Reads the config-as-code table `dbo.security_policy` (promoted like dq_rule/cleanse_rule) and
applies each method to the target workspace:

  onelake_cls  -> OneLake data access role, column whitelist (hidden cross-engine incl. Spark)
  onelake_rls  -> OneLake data access role, T-SQL row predicate (filtered cross-engine)
  ddm          -> Dynamic Data Masking via T-SQL on the lakehouse SQL analytics endpoint

Run against a target env:  CP_TARGET_WORKSPACE=<name> python cp_security.py [apply|show]

OneLake roles: PUT /workspaces/{wid}/items/{lakehouseId}/dataAccessRoles (Preview). DDM: ALTER
TABLE … ADD MASKED WITH on the SQL endpoint. Static masking is handled in silver via the `mask`
cleanse function (cleanse_rule) — stored masked, enforced everywhere.
"""
import json
import os
import struct
import sys

import pyodbc
import requests

import cp_auth
import cp_common as C
import cp_manifest as MF

API = "https://api.fabric.microsoft.com/v1"


def _H(t):
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}


def _lakehouse(t, wid, name):
    # security_policy.lakehouse stores the LOGICAL layer (e.g. 'silver'); resolve it to the current
    # physical name via cp_vars so a lakehouse rename needs no policy edits. A physical name passes through.
    name = MF.LAKEHOUSE_NAMES.get(name, name)
    for lh in requests.get(f"{API}/workspaces/{wid}/lakehouses", headers=_H(t)).json()["value"]:
        if lh["displayName"] == name:
            return lh
    sys.exit(f"lakehouse '{name}' not found")


def policies():
    import cp_sqldb as S
    cn = S.connect()
    cur = cn.cursor()
    cur.execute("SELECT * FROM dbo.security_policy WHERE is_active=1 ORDER BY method, policy_id")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cn.close()
    return rows


def _members(p):
    """members_json: JSON array of {objectId, objectType} (or bare id strings -> Group)."""
    out = []
    for m in (json.loads(p["members_json"]) if p.get("members_json") else []):
        m = m if isinstance(m, dict) else {"objectId": m, "objectType": "Group"}
        out.append({"objectId": m["objectId"], "objectType": m.get("objectType", "Group"),
                    "tenantId": os.getenv("AZURE_TENANT_ID")})
    return {"microsoftEntraMembers": out}


def _cols(p):
    return [c.strip() for c in str(p.get("columns") or "").split(";") if c.strip()]


def apply_onelake_roles(t, wid, pols):
    by_lh = {}
    for p in pols:
        if p["method"] in ("onelake_cls", "onelake_rls"):
            by_lh.setdefault(p["lakehouse"], []).append(p)
    for lh_name, ps in by_lh.items():
        lid = _lakehouse(t, wid, lh_name)["id"]
        r = requests.get(f"{API}/workspaces/{wid}/items/{lid}/dataAccessRoles", headers=_H(t))
        etag = r.headers.get("ETag")
        roles = {role["name"]: role for role in r.json().get("value", [])} if r.status_code == 200 else {}
        for p in ps:
            tp = f"/Tables/{p['target_schema']}/{p['target_table']}"
            perm = [{"attributeName": "Path", "attributeValueIncludedIn": [tp]},
                    {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}]
            constraints = {}
            if p["method"] == "onelake_cls":            # columns = VISIBLE whitelist (rest -> null)
                constraints["columns"] = [{"tablePath": tp, "columnNames": _cols(p),
                                           "columnEffect": "Permit", "columnAction": ["Read"]}]
            else:                                        # onelake_rls: T-SQL row predicate
                constraints["rows"] = [{"tablePath": tp, "value": p["predicate"]}]
            roles[p["role_name"]] = {
                "name": p["role_name"],
                "decisionRules": [{"effect": "Permit", "permission": perm, "constraints": constraints}],
                "members": _members(p)}
        h = _H(t)
        if etag:
            h["If-Match"] = etag
        pr = requests.put(f"{API}/workspaces/{wid}/items/{lid}/dataAccessRoles", headers=h,
                          json={"value": list(roles.values())})
        ok = pr.status_code in (200, 201)
        print(f"  OneLake roles on '{lh_name}': [{pr.status_code}] -> {list(roles)}"
              + ("" if ok else f"\n    {pr.text[:300]}"))


def apply_ddm(t, wid, pols):
    by_lh = {}
    for p in pols:
        if p["method"] == "ddm":
            by_lh.setdefault(p["lakehouse"], []).append(p)
    for lh_name, ps in by_lh.items():
        props = _lakehouse(t, wid, lh_name)["properties"]
        host = props["sqlEndpointProperties"]["connectionString"]
        # DDM targets the SQL endpoint — nudge it to sync new tables before ALTER (endpoint lags).
        try:
            requests.post(f"{API}/workspaces/{wid}/sqlEndpoints/"
                          f"{props['sqlEndpointProperties']['id']}/refreshMetadata", headers=_H(t), json={})
        except Exception:
            pass
        tok = cp_auth.get_token("https://database.windows.net/").encode("utf-16-le")
        ts = struct.pack(f"<I{len(tok)}s", len(tok), tok)
        cn = pyodbc.connect(f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host};"
                            f"DATABASE={lh_name};Encrypt=yes", attrs_before={1256: ts})
        cur = cn.cursor()
        for p in ps:
            for col in _cols(p):
                sql = (f"ALTER TABLE [{p['target_schema']}].[{p['target_table']}] "
                       f"ALTER COLUMN [{col}] ADD MASKED WITH (FUNCTION = '{p['mask_function']}')")
                try:
                    cur.execute(sql)
                    cn.commit()
                    print(f"  DDM {lh_name}.{p['target_schema']}.{p['target_table']}.{col} -> {p['mask_function']}")
                except Exception as e:
                    msg = str(e)
                    if "already" in msg.lower() or "masked" in msg.lower():
                        print(f"  DDM {col}: already masked")
                    else:
                        print(f"  DDM {col} FAILED: {msg[:200]}")
        cn.close()


def show(t, wid, pols):
    print(f"\n=== security_policy ({C.WS_NAME}) — {len(pols)} active ===")
    for p in pols:
        detail = (p.get("predicate") if p["method"] == "onelake_rls"
                  else (p.get("mask_function") if p["method"] == "ddm" else "keep: " + (p.get("columns") or "")))
        print(f"  [{p['method']:11}] {p['lakehouse']}.{p['target_schema']}.{p['target_table']}"
              f"  cols={p.get('columns') or '-'}  {detail or ''}  role={p.get('role_name') or '-'}")
    for lh_name in sorted({p["lakehouse"] for p in pols if p["method"].startswith("onelake")}):
        lid = _lakehouse(t, wid, lh_name)["id"]
        r = requests.get(f"{API}/workspaces/{wid}/items/{lid}/dataAccessRoles", headers=_H(t))
        names = [x["name"] for x in r.json().get("value", [])] if r.status_code == 200 else []
        print(f"  applied OneLake roles on '{lh_name}': {names}")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "apply"
    t = cp_auth.get_token("https://api.fabric.microsoft.com")
    wid = C.WS_ID
    pols = policies()
    print(f"security policies for {C.WS_NAME}: {len(pols)} active")
    if cmd in ("apply", "all"):
        apply_onelake_roles(t, wid, pols)
        apply_ddm(t, wid, pols)
    show(t, wid, pols)


if __name__ == "__main__":
    main()
