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
- **Connector**: `statcan_wds` (registered in `cp_framework`). No credentials, no driver — just
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

```sql
-- clean prior StatCan rows (safe re-run)
DELETE FROM dbo.dq_rule       WHERE object_id = 'statcan_labour_force';
DELETE FROM dbo.source_object WHERE object_id = 'statcan_labour_force';
DELETE FROM dbo.steps         WHERE load_group = 2;
DELETE FROM dbo.datasource    WHERE source_id = 2;

-- 3.1  datasource — the source system + which connector loads it
INSERT INTO dbo.datasource
  (source_id, source_name, source_type, database_name, load_group, ingestion_mode, is_active, connector, connection_json)
VALUES
  (2, 'stats_can', 'API', NULL, 2, 'api', 1, 'statcan_wds', NULL);

-- 3.2  source_object — the object to ingest (one StatCan table)
--   target_name NULL  -> name is derived (see §5): stats_can_dbo_labour_force_bc
--   key_columns_json  -> silver dedupe/merge key (snake_cased internally: ref_date, vector)
--   source_options_json -> connector params: which table, filters (SUBSET), select (SCHEMA)
--   suffix 'bc'       -> appended to the derived table name
INSERT INTO dbo.source_object
  (object_id, source_id, source_schema, source_table, target_name, load_type,
   key_columns_json, watermark_column, watermark_type, is_active, processing_state,
   source_options_json, suffix)
VALUES
  ('statcan_labour_force', 2, NULL, 'labour_force', NULL, 'full',
   '["REF_DATE","VECTOR"]', NULL, NULL, 1, 'ACTIVE',
   '{"table_id":"14100287","language":"en",
     "filters":{"GEO":"British Columbia","Gender":"Total - Gender","Statistics":"Estimate","Data type":"Seasonally adjusted"},
     "select":{"columns":["REF_DATE","GEO","Labour_force_characteristics","Age_group","Gender","Statistics","Data_type","VECTOR","COORDINATE","VALUE","UOM"],
               "cast":{"VALUE":"double"}}}',
   'bc');

-- 3.3  dq_rule — error-severity not-null on the keys (failing rows are quarantined on silver)
INSERT INTO dbo.dq_rule (rule_id, object_id, column_name, rule_type, severity, is_active) VALUES
  ('statcan_ref_date_nn', 'statcan_labour_force', 'ref_date', 'not_null', 'error', 1),
  ('statcan_vector_nn',   'statcan_labour_force', 'vector',   'not_null', 'error', 1);

-- 3.4  steps — orchestration for load group 2 (metadata/bronze/silver on; gold/pbi off)
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

| Key | Meaning |
|-----|---------|
| `table_id` | StatCan product id — `14100287` (Labour force characteristics). |
| `language` | `en` / `fr`. |
| `filters` | **Subset loading.** Equality filters applied **at ingest**, on the **ORIGINAL** StatCan column names (note the space in `"Data type"`). This turns the 5.4M-row table into the 32,724-row BC slice *before* anything is written. Add/remove keys to change the slice. |
| `select` | **Schema selection.** `columns` = exactly which columns land (names are the connector's *cleaned* output — non-alphanumeric → `_`, e.g. `Labour force characteristics` → `Labour_force_characteristics`), in order; `cast` = per-column type (here `VALUE` → `double`). Omit `select` to land the full schema. |

**Filters use original names; select uses cleaned names** — because the connector filters first,
then cleans column names, then applies `select`. Silver later snake_cases everything
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
