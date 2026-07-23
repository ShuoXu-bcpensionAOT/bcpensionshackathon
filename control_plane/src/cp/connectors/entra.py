"""Microsoft Entra ID (Azure AD) connector via Microsoft Graph. Loads directory objects from the
tenant named in the KV secret. Client-credentials auth (app registration with the relevant Graph
*Application* permissions + admin consent); the secret holds {tenant_id, client_id, client_secret}.

source_options_json:
    entity        Graph collection path, e.g. "users", "groups", "servicePrincipals",
                  "applications", "directoryRoles", "roleManagement/directory/roleAssignments"
    select        list of $select fields (or a string)
    filter        $filter expression
    expand        $expand (list or string) — e.g. ["principal","roleDefinition"]
    top           $top page size (optional; Graph paginates via @odata.nextLink regardless)
    api           "v1.0" (default) or "beta"
    subresource   RELATIONAL mode: for each item in `entity`, fetch /entity/{id}/<subresource>
                  (e.g. entity="groups", subresource="members") and tag each row with the parent's
                  fields as <singular>_<field> (e.g. group_id, group_displayName).
    parent_fields fields to read from each parent for tagging (default ["id"])
"""
import json
import re

from ..runtime import spark
from . import ingest_connector
from .base import _resolve_conn, _opts

GRAPH = "https://graph.microsoft.com"


def _graph_token(c):
    import requests
    tid = c.get("tenant_id") or c.get("tenant")
    r = requests.post(f"https://login.microsoftonline.com/{tid}/oauth2/v2.0/token",
                      data={"grant_type": "client_credentials", "client_id": c.get("client_id"),
                            "client_secret": c.get("client_secret"), "scope": f"{GRAPH}/.default"},
                      timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def _get_all(url, headers):
    """GET a Graph collection, following @odata.nextLink to the end."""
    import requests
    rows = []
    while url:
        r = requests.get(url, headers=headers, timeout=120)
        r.raise_for_status()
        j = r.json()
        rows += j.get("value", [j] if "id" in j else [])
        url = j.get("@odata.nextLink")
    return rows


def _qs(opts):
    parts = []
    sel = opts.get("select")
    if sel:
        parts.append("$select=" + (",".join(sel) if isinstance(sel, list) else str(sel)))
    if opts.get("filter"):
        parts.append("$filter=" + opts["filter"])
    exp = opts.get("expand")
    if exp:
        parts.append("$expand=" + (",".join(exp) if isinstance(exp, list) else str(exp)))
    if opts.get("top"):
        parts.append("$top=" + str(opts["top"]))
    return "&".join(parts)


def _flatten(rows):
    import pandas as pd
    if not rows:
        return None
    pdf = pd.json_normalize(rows)
    for col in pdf.columns:                                   # nested list/dict -> JSON string
        if pdf[col].apply(lambda v: isinstance(v, (list, dict))).any():
            pdf[col] = pdf[col].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)
    pdf.columns = [re.sub(r"[^A-Za-z0-9_]", "_", str(c)).strip("_") for c in pdf.columns]
    return pdf.where(pd.notnull(pdf), None)


@ingest_connector("entra", "msgraph", "graph")
def entra(o, user, password):
    c, opts = _resolve_conn(o), _opts(o)
    base = f"{GRAPH}/{opts.get('api', 'v1.0')}"
    headers = {"Authorization": f"Bearer {_graph_token(c)}", "ConsistencyLevel": "eventual"}
    entity = opts.get("entity") or o.get("source_table")

    if opts.get("subresource"):                              # relational: parent -> sub-collection
        sub = opts["subresource"]
        pfields = opts.get("parent_fields", ["id"])
        singular = re.sub(r"s$", "", entity.split("/")[-1])  # groups -> group
        parents = _get_all(f"{base}/{entity}?$select={','.join(pfields)}&$top=999", headers)
        subqs = _qs({k: opts.get(k) for k in ("select", "filter", "expand", "top")})
        rows = []
        for p in parents:
            for row in _get_all(f"{base}/{entity}/{p['id']}/{sub}?{subqs}", headers):
                tagged = {f"{singular}_{k}": p.get(k) for k in pfields}
                tagged.update(row)
                rows.append(tagged)
    else:
        rows = _get_all(f"{base}/{entity}?{_qs(opts)}", headers)

    pdf = _flatten(rows)
    if pdf is None or pdf.empty:
        return spark.createDataFrame([], "id string")
    return spark.createDataFrame(pdf)
