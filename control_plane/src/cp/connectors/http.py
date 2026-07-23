"""Generalized HTTP/API connector — one connector for every API (like ADF's HTTP linked
service + dataset). Differs per source only by source_options_json. Also serves statcan_wds
(a zip_csv profile)."""
import re

from ..runtime import spark
from . import ingest_connector
from .base import _resolve_conn, _opts, _dig


@ingest_connector("http", "rest_api", "api", "statcan_wds")
def _ic_http(o, user, password):
    """Generalized HTTP/API connector. Connection (KV secret / connection_json): base_url,
    headers/auth. Request (source_options_json):
       url | path        endpoint; may template {name} from `params` (e.g. .../{table_id}/{language})
       method            GET (default) / POST / ...
       params            values for URL templating AND query string
       query             extra query params (dict)
       body              JSON request body
       headers           per-request headers (merged over connection headers)
       response          how to turn the response into rows:
           {type:"json", record_path:"a.b"}          JSON -> list of records
           {type:"csv"}                               response body IS a CSV
           {type:"zip_csv", url_field:"object",       response JSON has a URL field pointing to a
                            member:<regex>|null,       ZIP; download it and read the CSV member
                            exclude:"_Meta"}           (StatCan WDS pattern)
       filters           {col:value} equality filters at ingest (ORIGINAL column names)
    """
    import requests
    import io
    import pandas as pd
    c, opts = _resolve_conn(o), _opts(o)
    params = opts.get("params", {})
    url = (opts.get("url") or (c.get("base_url", "") + opts.get("path", ""))).format(**params)
    headers = {**c.get("headers", {}), **opts.get("headers", {})}
    resp = requests.request((opts.get("method") or "GET").upper(), url, headers=headers,
                            params=opts.get("query"), json=opts.get("body"), timeout=120)
    resp.raise_for_status()

    r = opts.get("response") or {"type": "json"}
    rtype = r.get("type", "json")
    if rtype == "csv":
        pdf = pd.read_csv(io.BytesIO(resp.content), dtype=str, low_memory=False)
    elif rtype == "zip_csv":                            # follow a JSON pointer to a zip of CSVs
        import zipfile
        zurl = _dig(resp.json(), r.get("url_field", "object"))
        zf = zipfile.ZipFile(io.BytesIO(requests.get(zurl, timeout=600).content))
        member, excl = r.get("member"), r.get("exclude", "_Meta")
        names = [f for f in zf.namelist() if f.lower().endswith(".csv")
                 and (re.search(member, f) if member else (not excl or excl not in f))]
        pdf = pd.read_csv(zf.open(names[0]), dtype=str, low_memory=False)
    else:                                               # json
        data = _dig(resp.json(), r.get("record_path"))
        pdf = pd.json_normalize([data] if isinstance(data, dict) else data)

    for col, val in (opts.get("filters") or {}).items():
        if col in pdf.columns:
            pdf = pdf[pdf[col].astype(str).str.strip() == str(val)]
    pdf.columns = [re.sub(r"[^A-Za-z0-9_]", "_", str(x)) for x in pdf.columns]
    pdf = pdf.where(pd.notnull(pdf), None)
    return spark.createDataFrame(pdf)
