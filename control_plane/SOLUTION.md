# Fabric Control Plane — Packaged Solution

A metadata-driven lakehouse platform for Microsoft Fabric, deployable to any
environment from git. Config is authored in a **Fabric SQL Database** (T-SQL) and
promoted as code; orchestration runs as **Fabric Data Pipelines**.

## Package layout

```
control_plane/
├── environments/          per-env deploy parameters (dev/uat/prod .yml)
├── config/                config-as-code (YAML) — promotion snapshot of config_db
├── variable_library/      cp_vars Variable Library (env value sets)
├── notebooks/             Fabric notebooks (framework, planner, workers, source-queries)
├── deploy/                deployment tooling (bootstrap, config loader, pipeline authoring)
├── docs/                  design + ALM docs
├── requirements.txt       deploy-tooling Python deps
└── SOLUTION.md            this file
../.github/workflows/deploy.yml   CI/CD pipeline
```

## What gets deployed (per environment)

| Layer | Items |
|-------|-------|
| Workspace | `<workspace_base>-<environment>` on the configured capacity (created if missing) |
| Lakehouses | `metadata`, `bronze`, `silver`, `gold` |
| Config store | `config_db` (Fabric SQL Database) — authored config tables |
| Variable Library | `cp_vars` (lakehouse names, source server; per-env value sets) |
| Notebooks | `cp_framework`, `cp_plan`, `cp_log_fail`, `*_worker`, `gold_runner`, `sq_*` |
| Data Pipelines | `cp_pl_main` + `cp_pl_{metadata,bronze,silver,gold,pbi}` (in the `pipeline` folder) |

## Deploy

**CI/CD (recommended):** run the *Deploy Control Plane* workflow (`workflow_dispatch`,
pick the environment), or push to `main` to deploy DEV. Requires repo secrets
`AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` for a service principal that can use Fabric
APIs, is workspace admin, has capacity rights, and is granted access to `config_db`.

**Local:**
```bash
az login --tenant <tenant> --scope https://api.fabric.microsoft.com/.default --allow-no-subscriptions
export CP_CAPACITY_ID=<capacity-id>            # optional; defaults to the trial capacity
python control_plane/deploy/cp_bootstrap.py HackathonShuo DEV
```

Deploy is idempotent and **deploy-only** (deploy ≠ run). It provisions/updates items,
loads config-as-code into `config_db`, and waits for the OneLake mirror.

## Run

Pipelines execute on schedule or on demand, parameterized per **load group**:
```
cp_pl_main(load_group, run_id, src_user, src_password)
```
`cp_pl_main` reads the `steps` table and runs `load_metadata → load_bronze → load_silver
→ load_gold → refresh_pbi` in order, skipping inactive steps, fail-fast. Each pipeline
logs failures to `pipeline_run_log` in the metadata lakehouse and re-fails.

## Config authoring & promotion

Config tables in `config_db` are the **source of truth** (edit via T-SQL). Sync to git
with `cp_export_config.py` (SQL → YAML); promotion applies YAML → target `config_db`
with `cp_config.py`. Runtime state/logs stay in the lakehouse and are never promoted.

## Notes / roadmap

- **Config read:** pipeline planners (`cp_plan`) read `config_db` via pyodbc today.
  When a service principal is available, switch to native pipeline **SQL Lookups**.
- **Secrets:** source DB password is passed at run time (from a CI/secret store).
  Key Vault integration is the planned target.
- **Items promotion:** the bootstrap deploys all items via REST API; **fabric-cicd**
  can replace the notebook/pipeline deploy step later if desired.
