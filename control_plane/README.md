# Fabric Control Plane

A **metadata-driven lakehouse control plane** for Microsoft Fabric. You declare *what* to load and
*how* to govern it as **config-as-code**; the framework does the rest — connect, discover, ingest,
cleanse, quality-check, model, and secure — across **DEV / UAT / PROD** from git.

Config is authored in a **Fabric SQL Database** (`config_db`, T-SQL) and promoted as YAML.
Orchestration runs as **Fabric Data Pipelines** driving param-driven **Spark notebooks**. Nothing
is hand-wired: sources, objects, rules, models, and security policies are all rows in config.

---

## What it does

| Capability | How |
|---|---|
| **Pluggable source connectors** | One registry — SQL Server / PostgreSQL / MySQL (bundled JDBC), Oracle / DB2 (self-installing pure-Python), ODBC, and one **generalized HTTP/API** connector (JSON / CSV / zip-CSV via parameters). Add a source = config, no code. |
| **Connections in Key Vault** | `datasource.secret_name` → a KV secret holding the full connection (DB creds or HTTP base-url/auth). Only the *name* is in git. Build secrets with the `cp_connection_builder` wizard notebook. |
| **Auto-discovery** | The metadata step enumerates a datasource (all SQL Server tables + PK keys, or declared API resources) and **registers `source_object` rows as `is_active=0`** — you never hand-author objects; you review, tweak, activate. |
| **Subset & schema selection** | Per-object `filters` (land only the rows you want) and `select` (control which columns land, order, names, casts). |
| **Data quality & cleansing** | `cleanse_rule` fixes rows (trim/normalize/mask/…); `dq_rule` validates (not_null/min/max/allowed/expr) — error-severity failures are **quarantined** off silver. |
| **Schema-enabled medallion** | Bronze/silver land at `Tables/<datasource>/<sourceschema>_<table>` (e.g. `stats_can.dbo_labour_force_bc`); gold star schema (SCD1/SCD2/fact) built in DAG order by `sq_*` source-query notebooks. |
| **Code-driven data security** | `security_policy` + `cp_security.py` apply OneLake **CLS** (hide columns, cross-engine), OneLake **RLS** (row predicate), **Dynamic Data Masking** (SQL endpoint), and static **mask** (cleanse) — declared in config, promoted per environment. |
| **Promotion & CI/CD** | `cp_bootstrap` provisions a whole environment idempotently (workspace, lakehouses, `config_db`, variable library, notebooks, pipelines, config). Auth via a service principal (`cp_auth`); GitHub Actions workflow included. |

---

## Layout

```
control_plane/
├── config/                    config-as-code (YAML) — promotion snapshot of config_db
│   ├── datasource.yml         source systems + connector + secret_name
│   ├── source_object.yml      objects: load_type, keys, filters/select, suffix (mostly discovered)
│   ├── dq_rule.yml            data-quality rules            cleanse_rule.yml  row-fix rules
│   ├── gold_model.yml         model + gold_object + gold_dependency (the gold DAG)
│   ├── steps.yml              orchestration steps per load group
│   └── security_policy.yml    CLS / RLS / DDM / mask policies
├── notebooks/                 Fabric notebooks (deployed to the workspace)
│   ├── cp_framework.py        shared engine: connectors, discovery, cleanse/DQ, gold, security, KV
│   ├── cp_plan.py             planner — reads config_db, returns the work-list for a ForEach
│   ├── metadata_worker.py     start run + discover objects + schema-drift snapshot
│   ├── bronze_worker.py       load ONE object via its connector → bronze
│   ├── silver_worker.py       dedupe + cleanse + DQ/quarantine → silver
│   ├── gold_runner.py + sq_*  gold DAG: source-query notebooks build stage → gold
│   ├── cp_log_fail.py         on-failure logger
│   ├── cp_connection_builder.py   interactive wizard → Key Vault connection secret
│   └── cp_seed_demo.py        synthetic sensitive table for the security demo
├── deploy/                    local / CI tooling (Fabric REST + pyodbc)
│   ├── cp_bootstrap.py        provision + deploy a whole environment (idempotent)
│   ├── cp_auth.py             service-principal or az-login token minting
│   ├── cp_manifest.py         reads deploy/manifest.yml (the resource bundle)
│   ├── cp_sqldb.py            config_db schema (DDL + additive migrations)
│   ├── cp_config.py / cp_export_config.py   YAML ⇄ config_db (promotion)
│   ├── cp_pipeline.py         author/deploy the data pipelines
│   ├── cp_deploy.py           deploy/run notebooks
│   ├── cp_security.py         apply security_policy per environment
│   ├── cp_environment.py      (opt-in) provision a driver Environment
│   ├── cp_secrets.py          Key Vault get/set for the tooling
│   └── fabric_nb.py, cp_common.py, cp_varlib.py
├── environments/              per-env deploy params (dev/uat/prod .yml — no secrets)
├── variable_library/          cp_vars (lakehouse names, source server, key_vault_url; per-env sets)
├── deploy/manifest.yml        the resource bundle — what gets deployed (names only)
└── docs/                      WORKING_GUIDE (reference), RUNBOOK_statcan, CICD, portability-design
```

---

## Deploy an environment

```bash
# service principal in .env (SPN_CLIENT_ID/SECRET) or an az login; CP_PROVISION_AS_USER=1 if the
# SP can't create workspaces (see docs). One command provisions everything + loads config-as-code:
python control_plane/deploy/cp_bootstrap.py HackathonShuo DEV        # or UAT / PROD
```

This creates/updates the `<base>-<env>` workspace, the **schema-enabled** lakehouses, `config_db`,
the `cp_vars` variable library, all notebooks (foldered) + pipelines, and loads `config/*.yml`.
Deploy is idempotent and **deploy-only** (deploy ≠ run). CI/CD: the *Deploy Control Plane* GitHub
Action does the same headlessly.

## Run a load

```
cp_pl_main(load_group, run_id, src_user, src_password)
```
`cp_pl_main` reads the `steps` table for the load group and runs
`load_metadata → load_bronze → load_silver → load_gold → refresh_pbi` in order, skipping inactive
steps, fail-fast. Each child pipeline logs failures to `pipeline_run_log` and re-fails.

## Apply security (per environment, after a load)

```bash
CP_TARGET_WORKSPACE=<workspace> python control_plane/deploy/cp_security.py apply   # or: show
```

## Author & promote config

Config in `config_db` is the **source of truth** (edit via T-SQL, or use auto-discovery). Snapshot
to git with `cp_export_config.py` (SQL → YAML); promote with `cp_config.py` (YAML → target env).
Runtime state/logs live in the lakehouse and are never promoted.

---

## Add a source (config only)

1. **Connection → Key Vault**: run `cp_connection_builder`, pick the source type, fill the fields,
   Write to Key Vault. Set `datasource.connector` + `datasource.secret_name`.
2. **Discover**: run `cp_pl_metadata` — objects register as `is_active=0`.
3. **Tweak + activate**: set filters/select/keys, `is_active=1`.
4. **Run**: `cp_pl_main(load_group=…)`.

Worked end-to-end example (a Statistics Canada API subset): **`docs/RUNBOOK_statcan.md`**.
Full reference (every config table, connector, and the data-security model): **`docs/WORKING_GUIDE.md`**.

---

## Notes

- **Secrets** never touch git or Delta — connections resolve from Key Vault at run time via the
  running identity (the service principal has KV *get*). `src_user`/`src_password` are a fallback.
- **OneLake** is addressed by **GUID** (workspace + lakehouse), resolved at runtime — no hardcoded ids.
- **Enforcement note:** OneLake CLS/RLS and DDM only restrict **non-privileged (Viewer)** users;
  admins/owners see clear data.
