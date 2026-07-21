# Runbook — landing Statistics Canada data (labour force) to silver

A complete, reproducible walkthrough of how we loaded StatCan table **14100287**
(Labour force characteristics) into the lakehouse as a **British-Columbia subset**, and
how to redo it in a **brand-new environment**. Everything here is **config only** — no
notebook or pipeline code changes.

---

## 1. What actually happened (end to end)

```
config_db (SQL)                 cp_pl_main (load_group = 2)
  datasource: stats_can  ─────►  ├─ cp_pl_metadata → metadata_worker  (start run; schema discovery SKIPPED for API source)
  source_object: statcan_...     ├─ cp_pl_bronze   → cp_plan → ForEach → bronze_worker
  dq_rule x2                     │                     connector = statcan_wds
  steps (lg2)                    │                     download 14100287 → filter to BC slice → select 11 cols, VALUE→double
                                 │                     land → bronze.stats_can_dbo_labour_force_bc   (32,724 rows)
                                 ├─ cp_pl_silver   → cp_plan → ForEach → silver_worker
                                 │                     snake_case → dedupe by (ref_date, vector) → DQ (not-null) 
                                 │                     merge → silver.stats_can_dbo_labour_force_bc  (32,724 rows)
                                 ├─ cp_pl_gold     (skipped — inactive)
                                 └─ cp_pl_pbi      (skipped — inactive)
```

- **Source**: StatCan WDS REST API. `getFullTableDownloadCSV/14100287/en` returns JSON
  `{status:"SUCCESS", object:"<zip url>"}`; the connector downloads the zip and reads the
  non-`_Meta` CSV. Full table ≈ **5.4M rows**; the filter lands the **32,724-row** BC slice.
- **Connector**: the **generalized `http` connector** (there is no StatCan-specific connector) —
  driven by parameters; StatCan is just `response.type = zip_csv`. No credentials, no driver — just
  outbound HTTPS to `www150.statcan.gc.ca`.
- **Landed table name** is *derived* (we left `target_name` NULL):
  `{source_name}_{source_schema|dbo}_{source_table}_{suffix}` → **`stats_can_dbo_labour_force_bc`**.

---

## 2. Prerequisites (one-time, per environment)

The control plane must be deployed to the target workspace (lakehouses, `config_db`, variable
library, notebooks, pipelines). If it isn't yet, run the bootstrap once:

```bash
# from the repo root; CP_PROVISION_AS_USER=1 only if the service principal can't create workspaces
CP_PROVISION_AS_USER=1 python control_plane/deploy/cp_bootstrap.py <WorkspaceBase> <ENV>
# e.g.  CP_PROVISION_AS_USER=1 python control_plane/deploy/cp_bootstrap.py HackathonShuo DEV
```

Bootstrap also runs `cp_config`, which loads `control_plane/config/*.yml` into `config_db`. **The
StatCan config is already in those YAML files**, so a fresh bootstrap creates the StatCan source
automatically — see §6 (Path A). §3–§5 below show the underlying SQL for understanding or for
adding it by hand (Path B).

The config schema carries four columns this feature relies on; `cp_sqldb.ensure_schema` adds them
automatically on any existing `config_db` (idempotent `ALTER … ADD`):
`datasource.connector`, `datasource.connection_json`, `source_object.source_options_json`,
`source_object.suffix`.

---

## 3. The config we added (exact SQL)

Run these in the Fabric **`config_db`** SQL query editor (or via `cp_config` from YAML). Idempotent:
the deletes let you re-run safely.

> **The connection lives in Key Vault — for APIs too.** Every source (DB *or* API) puts its
> connection in a KV secret and the datasource points at it with `secret_name`. StatCan uses the
> **generalized `http` connector** (not a StatCan-specific connector), and `source_options` holds
> the *request* (path/params/response). `source_id` is auto-assigned (`IDENTITY`) — never pick it.

**Step 3.1 — put the API connection in Key Vault** (the base URL; add `headers` for a private API).
Build it with `cp_connection_builder` (pick **http**) or the CLI:
```bash
az keyvault secret set --vault-name kv-fabric-cc --name statcan-http \
  --value '{"base_url":"https://www150.statcan.gc.ca/t1/wds/rest"}'
```
> **Shortcut:** the `cp_connection_builder` wizard does Steps 3.1–3.2 in one action — it writes the
> secret *and* registers the `datasource` row (connector + `secret_name`). Then skip to Step 3.3.

**Step 3.2 — register the datasource** — generalized `http` connector, connection **by secret name**
(no `connection_json`, no custom connector):
```sql
DELETE FROM dbo.datasource WHERE source_name = 'stats_can';   -- safe re-run
INSERT INTO dbo.datasource
  (source_name, source_type, load_group, ingestion_mode, is_active, connector, secret_name)
VALUES
  ('stats_can', 'API', 2, 'api', 1, 'http', 'statcan-http');
```

**Step 3.3 — register the object** — the *request* in `source_options_json`. `path` is relative to
the secret's `base_url`; `params` fill the `{…}` templates; `response:zip_csv` handles StatCan's
"JSON pointer → ZIP → CSV"; `filters` land the subset; `select` shapes the schema. `target_name`
NULL → derived name (§5).
```sql
DECLARE @source_id INT = (SELECT source_id FROM dbo.datasource WHERE source_name='stats_can');
DELETE FROM dbo.source_object WHERE object_id='statcan_labour_force';
INSERT INTO dbo.source_object
  (object_id, source_id, source_table, load_type, key_columns_json, is_active, processing_state, suffix, source_options_json)
VALUES ('statcan_labour_force', @source_id, 'labour_force', 'full', '["REF_DATE","VECTOR"]', 1, 'ACTIVE', 'bc',
  '{"path":"/getFullTableDownloadCSV/{table_id}/{language}",
    "params":{"table_id":"14100287","language":"en"},
    "response":{"type":"zip_csv","url_field":"object","exclude":"_Meta"},
    "filters":{"GEO":"British Columbia","Gender":"Total - Gender","Statistics":"Estimate","Data type":"Seasonally adjusted"},
    "select":{"columns":["REF_DATE","GEO","Labour_force_characteristics","Age_group","Gender","Statistics","Data_type","VECTOR","COORDINATE","VALUE","UOM"],
              "cast":{"VALUE":"double"}}}');
```
> **Discovery vs. declaration:** a **SQL Server** datasource auto-discovers *every* table (the
> metadata step registers them `is_active=0`; you then activate the ones you want). An **API** has
> no enumerable catalog, so you *declare* the object's request as above — still no hardcoded
> connection, still promoted as code.
> Only objects you activate here load. Everything discovery finds stays `is_active=0` until you
> approve it — so an API/DB source can't silently pull tables you didn't intend.

**Step 3.4 — DQ rules + orchestration steps for the load group:**
```sql
-- dq_rule — error-severity not-null on the keys (failing rows are quarantined on silver)
INSERT INTO dbo.dq_rule (rule_id, object_id, column_name, rule_type, severity, is_active) VALUES
  ('statcan_ref_date_nn', 'statcan_labour_force', 'ref_date', 'not_null', 'error', 1),
  ('statcan_vector_nn',   'statcan_labour_force', 'vector',   'not_null', 'error', 1);

-- steps — orchestration for load group 2 (metadata/bronze/silver on; gold/pbi off)
INSERT INTO dbo.steps (load_group, step_order, step_key, child_pipeline, is_active) VALUES
  (2, 1, 'load_metadata', 'cp_pl_metadata', 1),
  (2, 2, 'load_bronze',   'cp_pl_bronze',   1),
  (2, 3, 'load_silver',   'cp_pl_silver',   1),
  (2, 4, 'load_gold',     'cp_pl_gold',     0),
  (2, 5, 'refresh_pbi',   'cp_pl_pbi',      0);
```

> **Why load group 2?** Load groups are the run unit — `cp_pl_main` runs exactly one. Putting
> StatCan in group 2 keeps it independent of AdventureWorks (group 1), so you run just StatCan.

---

## 4. `source_options_json` explained (the important part)

StatCan is loaded by the **generalized `http` connector** (one connector for every API — there is
no StatCan-specific connector). `source_options_json` is the *request definition*, exactly like a
Data Factory HTTP dataset:

| Key | Meaning |
|-----|---------|
| `url` (or `path`) | Endpoint, with `{name}` placeholders filled from `params`. Here `…/getFullTableDownloadCSV/{table_id}/{language}`. (A base URL + auth headers can instead live in the KV secret — see §10.) |
| `params` | Values for URL templating (and query string) — `{table_id, language}`. |
| `method` / `query` / `body` / `headers` | Standard HTTP request parts (default `GET`). |
| `response` | **How to turn the response into rows.** `{type:"json", record_path}` for normal APIs; `{type:"csv"}`; or **`{type:"zip_csv", url_field:"object", exclude:"_Meta"}`** — the StatCan pattern: the response JSON has a field (`object`) pointing to a ZIP; download it and read the non-`_Meta` CSV. |
| `filters` | **Subset loading.** Equality filters applied **at ingest**, on the **ORIGINAL** column names (note the space in `"Data type"`). Turns the 5.4M-row table into the 32,724-row BC slice *before* anything is written. |
| `select` | **Schema selection.** `columns` = which columns land (names are the connector's *cleaned* output — non-alphanumeric → `_`, e.g. `Labour force characteristics` → `Labour_force_characteristics`); `cast` = per-column type (`VALUE`→`double`). Omit to land the full schema. |

**Same connector, different parameters.** A plain JSON API is just `{"url":…, "response":{"type":"json","record_path":"data.records"}}`; StatCan swaps `response.type` to `zip_csv`. Next API → same `http` connector, new params.

**Filters use original names; select uses cleaned names** — the connector fetches, applies `filters`,
cleans column names, then applies `select`. Silver later snake_cases everything
(`REF_DATE` → `ref_date`, `VALUE` → `value`).

---

## 5. How the table name is derived

We left `target_name` NULL, so `landed_table()` builds it:

```
{source_name}_{source_schema | dbo}_{source_table}_{suffix}
  stats_can  _        dbo        _  labour_force _  bc     =  stats_can_dbo_labour_force_bc
```

(lowercased, non-alphanumeric → `_`). To pin an explicit name instead, set `target_name` — it
always wins. This is a **flat name** (our lakehouses aren't schema-enabled); it namespaces every
source's tables consistently.

---

## 6. Run it — which pipeline, what parameters

**One pipeline runs everything: `cp_pl_main`.** It reads the `steps` for the load group and invokes
the active child pipelines in order.

Parameters:

| Parameter | Value for StatCan | Notes |
|-----------|-------------------|-------|
| `load_group` | `2` | selects the StatCan config |
| `run_id` | any label, e.g. `statcan1` | stamped into bronze/silver + logs |
| `src_user` | `` (empty) | StatCan needs no credentials |
| `src_password` | `` (empty) | " |

**Option A — Fabric UI:** open the **`cp_pl_main`** pipeline → **Run** → set
`load_group=2, run_id=statcan1, src_user=, src_password=` → Run. (~5–6 min.)

**Option B — programmatic (what we ran):**
```python
import cp_pipeline as P, fabric_nb as FN
tok = FN.token()
st, info = P.run_pipeline(tok, "cp_pl_main",
    {"load_group": 2, "run_id": "statcan1", "src_user": "", "src_password": ""}, timeout=2400)
print(st)   # -> Completed
```

What each stage does at run time:
1. **cp_pl_metadata → metadata_worker**: starts the run; **skips** schema discovery because the
   connector isn't SQL Server (API sources define columns at ingest).
2. **cp_pl_bronze → cp_plan(objects, lg=2) → ForEach → bronze_worker**: `cp_plan` reads `config_db`
   and returns the work-list (the one StatCan object, with its `source_options_json`). `bronze_worker`
   dispatches to the `statcan_wds` connector → download → filter → `select` → add control columns →
   write `bronze.stats_can_dbo_labour_force_bc` (overwrite, `full` load).
3. **cp_pl_silver → silver_worker**: reads bronze, snake_cases, dedupes by `(ref_date, vector)`,
   runs cleanse (none configured) then DQ (`ref_date`/`vector` not-null; failures quarantined),
   merges into `silver.stats_can_dbo_labour_force_bc`.
4. **cp_pl_gold / cp_pl_pbi**: skipped (inactive in `steps`).

---

## 7. Verify

Query the **silver** lakehouse SQL endpoint:
```sql
SELECT COUNT(*) FROM stats_can_dbo_labour_force_bc;                                  -- 32724
SELECT COUNT(DISTINCT CONCAT(ref_date,'|',vector)) FROM stats_can_dbo_labour_force_bc; -- 32724 (key is unique)
SELECT TOP 5 ref_date, geo, labour_force_characteristics, age_group, vector, value
FROM stats_can_dbo_labour_force_bc ORDER BY ref_date, vector;
```
Run logs (in the **metadata** lakehouse): `object_load_run` rows for your `run_id` show
`bronze SUCCEEDED 32724` and `silver SUCCEEDED 32724`.

---

## 8. Reproduce in a BRAND-NEW environment

### Path A — it comes for free with bootstrap (recommended)
The StatCan config is already committed to `control_plane/config/*.yml`. So:
```bash
# 1) deploy the whole control plane to the new workspace (also loads all config from YAML)
CP_PROVISION_AS_USER=1 python control_plane/deploy/cp_bootstrap.py <WorkspaceBase> <ENV>
# 2) run the load
#    Fabric UI: run cp_pl_main with load_group=2, run_id=<label>, src_user=, src_password=
```
Nothing else — bootstrap's `cp_config` step inserts the StatCan `datasource`/`source_object`/
`dq_rule`/`steps` into the new `config_db`, and `cp_pl_main(load_group=2)` lands the data.

### Path B — add it by hand (new table, or without re-bootstrapping)
1. Ensure the schema columns exist (any deploy runs `ensure_schema`; or run it once).
2. Paste the SQL from **§3** into the new environment's `config_db` (adjust ids/filters as needed).
3. Run `cp_pl_main` with `load_group=2` (§6).

### Promotion note
Config is promoted as code: edit `config_db` (source of truth) → `cp_export_config` (SQL→YAML) →
commit → `cp_config` applies it to UAT/PROD. That's why Path A works — we already exported and
committed the StatCan rows.

---

## 9. Load a DIFFERENT StatCan table / a different slice
Copy the §3 pattern with a new `object_id` and edit `source_options_json`:
- `table_id` → the new product id.
- `filters` → your slice (original StatCan column names). Remove keys to widen; drop `filters`
  entirely to load the whole table (watch volume).
- `select.columns` → the columns you want (cleaned names); `cast` as needed.
- `suffix` / `source_table` / `source_schema` → drive the landed name (§5).
- Add `dq_rule` rows and, if it's a new load group, `steps` rows for that group.
Then run `cp_pl_main` with the matching `load_group`.
```

---

## 10. Connections via Key Vault (`datasource.secret_name`)
Connection info (especially credentials) lives in **Key Vault**, not in config. A datasource points
at a secret by name; the secret's value is the **complete connection payload** for the source.
```sql
-- e.g. a SQL Server source: the secret holds the whole connection, config holds only its NAME
UPDATE dbo.datasource SET secret_name = 'source-adventureworks' WHERE source_name = 'AdventureWorks';
```
Examples of the secret **value** per source type (built by `cp_connection_builder`):
```json
// SQL Server / Postgres / MySQL / Oracle / DB2 (secret 'source-adventureworks')
{"host":"20.63.101.180","port":1433,"database":"AdventureWorks2025","user":"dbadmin","password":"…"}

// HTTP / API (secret 'statcan-http') — base URL, plus headers/auth for a PRIVATE API
{"base_url":"https://www150.statcan.gc.ca/t1/wds/rest"}
{"base_url":"https://api.example.com/v2", "headers":{"Authorization":"Bearer <token>"}}
```
- **Every source type — DB and API — puts its connection in KV.** For HTTP the secret holds
  `base_url` (+ `headers` for auth); the per-request `path`/`params`/`response` live in the object's
  `source_options`. The value may also be a raw connection string/url. `connection_json` only layers
  **non-secret** overrides (e.g. `{"mode":"jdbc"}`).
- At run time the connector resolves it via `notebookutils.credentials.getSecret` using the **running
  identity** — so automated pipeline runs must execute **as the service principal** (which has KV
  *get*). The vault URL is the `cp_vars` variable `key_vault_url`.
- **No secret value ever lands in git or config** — only the secret *name*. Writing a secret needs a
  principal with KV *set* (your personal account; the SPN is read-only). Use `cp_connection_builder`
  to generate + write the secret in the right format.
