# COMMAND ----------
# cp_connection_builder — interactive wizard to build a source CONNECTION SECRET.
# Pick a source type, fill the fields it needs, and it generates the exact JSON our connectors
# parse (cp_framework._resolve_conn), then optionally writes it to Key Vault. Point the datasource
# at it with:  UPDATE dbo.datasource SET secret_name='<name>' WHERE source_name='<...>'.
import json
import base64

# Field spec per connector: (key, label, default, kind)  kind = text | password | number | choice:a,b
SPECS = {
    "sqlserver":  [("host", "Host / IP", "", "text"), ("port", "Port", "1433", "number"),
                   ("database", "Database", "", "text"), ("user", "User", "", "text"),
                   ("password", "Password", "", "password")],
    "postgresql": [("host", "Host / IP", "", "text"), ("port", "Port", "5432", "number"),
                   ("database", "Database", "", "text"), ("user", "User", "", "text"),
                   ("password", "Password", "", "password")],
    "mysql":      [("host", "Host / IP", "", "text"), ("port", "Port", "3306", "number"),
                   ("database", "Database", "", "text"), ("user", "User", "", "text"),
                   ("password", "Password", "", "password")],
    "oracle":     [("host", "Host / IP", "", "text"), ("port", "Port", "1521", "number"),
                   ("service", "Service / DB", "", "text"), ("user", "User", "", "text"),
                   ("password", "Password", "", "password")],
    "db2":        [("host", "Host / IP", "", "text"), ("port", "Port", "50000", "number"),
                   ("database", "Database", "", "text"), ("user", "User", "", "text"),
                   ("password", "Password", "", "password")],
    "odbc":       [("odbc", "ODBC connection string ({user}/{password} placeholders)",
                    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=host,1433;DATABASE=db;"
                    "UID={user};PWD={password};Encrypt=yes", "text")],
    "http":       [("base_url", "Base URL (optional; can also be per-object)", "", "text"),
                   ("auth_type", "Auth", "none", "choice:none,bearer,api_key,basic"),
                   ("token", "Token / API key", "", "password"),
                   ("api_key_header", "API-key header name", "x-api-key", "text"),
                   ("user", "User (basic)", "", "text"), ("password", "Password (basic)", "", "password")],
}


def build_secret(connector, v):
    """Build the connection-secret dict exactly as cp_framework._resolve_conn / the connectors read it."""
    if connector in ("sqlserver", "postgresql", "mysql", "oracle", "db2"):
        out = {}
        for k in ("host", "port", "database", "service", "user", "password"):
            if v.get(k) not in (None, ""):
                out[k] = int(v[k]) if k == "port" else v[k]
        return out
    if connector == "odbc":
        return {"odbc": v.get("odbc", "")}
    if connector == "http":
        headers, at = {}, v.get("auth_type", "none")
        if at == "bearer" and v.get("token"):
            headers["Authorization"] = "Bearer " + v["token"]
        elif at == "api_key" and v.get("token"):
            headers[v.get("api_key_header") or "x-api-key"] = v["token"]
        elif at == "basic" and v.get("user"):
            headers["Authorization"] = "Basic " + base64.b64encode(
                f"{v['user']}:{v.get('password','')}".encode()).decode()
        out = {}
        if v.get("base_url"):
            out["base_url"] = v["base_url"]
        if headers:
            out["headers"] = headers
        return out
    return {}


# COMMAND ----------
# --- interactive UI (ipywidgets) ---
import ipywidgets as W                                     # noqa: E402
from IPython.display import display, clear_output          # noqa: E402
import notebookutils                                       # noqa: F821,E402
import requests                                            # noqa: E402

try:
    KV_URL = (getattr(notebookutils.variableLibrary.getLibrary("cp_vars"), "key_vault_url", "") or "").rstrip("/")
except Exception:
    KV_URL = ""

_STYLE = {"description_width": "220px"}
_LAYOUT = W.Layout(width="620px")


def _mk(key, label, default, kind):
    if kind.startswith("choice:"):
        opts = kind.split(":", 1)[1].split(",")
        return W.Dropdown(options=opts, value=default or opts[0], description=label, style=_STYLE, layout=_LAYOUT)
    cls = W.Password if kind == "password" else W.Text
    return cls(value=default, description=label, style=_STYLE, layout=_LAYOUT)


connector_dd = W.Dropdown(options=list(SPECS), description="Source type", style=_STYLE, layout=_LAYOUT)
secret_name = W.Text(description="Secret name", placeholder="e.g. source-adventureworks", style=_STYLE, layout=_LAYOUT)
kv_url_w = W.Text(value=KV_URL, description="Key Vault URL", style=_STYLE, layout=_LAYOUT)
fields_box = W.VBox()
out = W.Output()
_widgets = {}


def _render(*_):
    global _widgets
    _widgets = {k: _mk(k, label, default, kind) for (k, label, default, kind) in SPECS[connector_dd.value]}
    fields_box.children = list(_widgets.values())


connector_dd.observe(_render, names="value")
_render()


def _current():
    return connector_dd.value, {k: w.value for k, w in _widgets.items()}


def _on_generate(_):
    with out:
        clear_output()
        conn, vals = _current()
        secret = build_secret(conn, vals)
        print(f"connector = {conn}   (set this in datasource.connector)")
        print("Key Vault secret VALUE (store this):\n")
        print(json.dumps(secret, indent=2))
        print(f"\nThen point the datasource at it:\n"
              f"  UPDATE dbo.datasource SET secret_name='{secret_name.value or '<secret-name>'}' "
              f"WHERE source_name='<your source>';")


def _on_write(_):
    with out:
        clear_output()
        conn, vals = _current()
        if not secret_name.value:
            print("Set a secret name first."); return
        if not kv_url_w.value:
            print("Set the Key Vault URL (or cp_vars.key_vault_url)."); return
        secret = build_secret(conn, vals)
        tok = notebookutils.credentials.getToken("https://vault.azure.net")  # running identity needs KV 'set'
        r = requests.put(f"{kv_url_w.value.rstrip('/')}/secrets/{secret_name.value}?api-version=7.4",
                         headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
                         json={"value": json.dumps(secret)})
        if r.status_code in (200, 201):
            print(f"✅ wrote secret '{secret_name.value}' to Key Vault.\n"
                  f"Now: UPDATE dbo.datasource SET secret_name='{secret_name.value}' WHERE source_name='<...>';")
        else:
            print(f"❌ KV write failed [{r.status_code}] {r.text[:300]}\n"
                  "(Your identity needs KV 'set' on this vault — the SPN is read-only; use your account.)")


gen_btn = W.Button(description="Generate JSON", button_style="primary")
write_btn = W.Button(description="Write to Key Vault", button_style="success")
gen_btn.on_click(_on_generate)
write_btn.on_click(_on_write)

display(W.VBox([W.HTML("<b>Build a source connection secret</b> — pick a type, fill the fields, "
                       "Generate (copy to Key Vault) or Write directly."),
                connector_dd, secret_name, kv_url_w, fields_box, W.HBox([gen_btn, write_btn]), out]))
