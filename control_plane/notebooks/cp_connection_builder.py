# COMMAND ----------
%run cp_framework

# COMMAND ----------
# cp_connection_builder — interactive wizard to onboard a source in ONE action:
#   1) build the CONNECTION SECRET (the JSON our connectors parse) and write it to Key Vault, and
#   2) register/update the matching dbo.datasource row (connector + secret_name) in config_db,
# so the vault and config stay in sync. Then run cp_pl_metadata to discover the source's objects.
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
    "http":       [("base_url", "Base URL", "", "text"),
                   ("auth_type", "Auth", "none", "choice:none,bearer,api_key,basic"),
                   ("token", "Token / API key", "", "password"),
                   ("api_key_header", "API-key header name", "x-api-key", "text"),
                   ("user", "User (basic)", "", "text"), ("password", "Password (basic)", "", "password")],
}


def build_secret(connector, v):
    """The connection-secret dict our connectors parse (cp_framework._resolve_conn)."""
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


def register_datasource(source_name, connector, secret_name, load_group):
    """Upsert the dbo.datasource row (config_db) — INSERT (IDENTITY source_id) or UPDATE by name."""
    src_type = "API" if connector == "http" else "SQL"
    ingest = "api" if connector == "http" else "custom_jdbc"
    exists = config_query("SELECT source_id FROM dbo.datasource WHERE source_name=?", (source_name,))
    if exists:
        config_exec("UPDATE dbo.datasource SET source_type=?, load_group=?, ingestion_mode=?, "
                    "connector=?, secret_name=?, is_active=1 WHERE source_name=?",
                    (src_type, int(load_group), ingest, connector, secret_name, source_name))
        return "updated", exists[0]["source_id"]
    config_exec("INSERT INTO dbo.datasource (source_name, source_type, load_group, ingestion_mode, "
                "is_active, connector, secret_name) VALUES (?,?,?,?,1,?,?)",
                (source_name, src_type, int(load_group), ingest, connector, secret_name))
    sid = config_query("SELECT source_id FROM dbo.datasource WHERE source_name=?", (source_name,))
    return "inserted", sid[0]["source_id"]


# COMMAND ----------
# --- interactive UI (ipywidgets) ---
import ipywidgets as W                                     # noqa: E402
from IPython.display import display, clear_output          # noqa: E402
import requests                                            # noqa: F821,E402

_STYLE = {"description_width": "210px"}
_LAYOUT = W.Layout(width="640px")


def _mk(key, label, default, kind):
    if kind.startswith("choice:"):
        opts = kind.split(":", 1)[1].split(",")
        return W.Dropdown(options=opts, value=default or opts[0], description=label, style=_STYLE, layout=_LAYOUT)
    cls = W.Password if kind == "password" else W.Text
    return cls(value=default, description=label, style=_STYLE, layout=_LAYOUT)


connector_dd = W.Dropdown(options=list(SPECS), description="Source type (connector)", style=_STYLE, layout=_LAYOUT)
source_name = W.Text(description="Source name", placeholder="e.g. stats_can", style=_STYLE, layout=_LAYOUT)
load_group = W.IntText(value=1, description="Load group", style=_STYLE, layout=_LAYOUT)
secret_name = W.Text(description="Key Vault secret name", placeholder="e.g. conn-stats_can", style=_STYLE, layout=_LAYOUT)
kv_url_w = W.Text(value=str(KEY_VAULT_URL or "").rstrip("/"), description="Key Vault URL", style=_STYLE, layout=_LAYOUT)
fields_box = W.VBox()
out = W.Output()
_widgets = {}


def _render(*_):
    global _widgets
    _widgets = {k: _mk(k, label, default, kind) for (k, label, default, kind) in SPECS[connector_dd.value]}
    fields_box.children = list(_widgets.values())
    if not secret_name.value and source_name.value:
        secret_name.value = "conn-" + source_name.value


connector_dd.observe(_render, names="value")
source_name.observe(lambda *_: _render(), names="value")
_render()


def _vals():
    return connector_dd.value, {k: w.value for k, w in _widgets.items()}


def _on_generate(_):
    with out:
        clear_output()
        conn, vals = _vals()
        print(f"connector = {conn}")
        print("Key Vault secret VALUE:\n" + json.dumps(build_secret(conn, vals), indent=2))
        print(f"\nWould register datasource '{source_name.value}' (load_group={load_group.value}, "
              f"secret_name='{secret_name.value}'). Click 'Write + register' to apply.")


def _on_write(_):
    with out:
        clear_output()
        conn, vals = _vals()
        if not source_name.value or not secret_name.value:
            print("Set a source name and a secret name first."); return
        # 1) write the connection secret to Key Vault (running identity needs KV 'set')
        tok = notebookutils.credentials.getToken("https://vault.azure.net")   # noqa: F821
        r = requests.put(f"{kv_url_w.value.rstrip('/')}/secrets/{secret_name.value}?api-version=7.4",
                         headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
                         json={"value": json.dumps(build_secret(conn, vals))})
        if r.status_code not in (200, 201):
            print(f"❌ Key Vault write failed [{r.status_code}] {r.text[:300]}\n"
                  "(Your identity needs KV 'set' — the SPN is read-only; use your account.)"); return
        print(f"✅ wrote Key Vault secret '{secret_name.value}'")
        # 2) upsert the datasource row so config stays in sync
        action, sid = register_datasource(source_name.value, conn, secret_name.value, load_group.value)
        print(f"✅ datasource '{source_name.value}' {action} (source_id={sid}, connector={conn})")
        print("\nNext: run cp_pl_metadata to discover this source's objects, then tweak + activate them.")


gen_btn = W.Button(description="Generate (preview)", button_style="primary")
write_btn = W.Button(description="Write to KV + register datasource", button_style="success",
                     layout=W.Layout(width="280px"))
gen_btn.on_click(_on_generate)
write_btn.on_click(_on_write)

display(W.VBox([W.HTML("<b>Onboard a source</b> — pick a type, fill the connection + name, then "
                       "write the secret to Key Vault AND register the datasource in one step."),
                connector_dd, source_name, load_group, secret_name, kv_url_w, fields_box,
                W.HBox([gen_btn, write_btn]), out]))
