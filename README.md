# BCPensions — Fabric Control Plane

A **metadata-driven lakehouse control plane** for Microsoft Fabric: declare sources, rules, models,
and security as **config-as-code**, and deploy/run/promote them across **DEV / UAT / PROD** from git.
Config is authored in a Fabric SQL Database and promoted as YAML; orchestration runs as Fabric Data
Pipelines over param-driven Spark notebooks.

## Where to look

| | |
|---|---|
| **[`control_plane/README.md`](control_plane/README.md)** | Start here — overview, layout, deploy/run/promote, add-a-source. |
| **[`control_plane/docs/WORKING_GUIDE.md`](control_plane/docs/WORKING_GUIDE.md)** | Full reference: every config table, all connectors, DQ/cleanse, gold DAG, and data security (`.docx` alongside). |
| **[`control_plane/docs/RUNBOOK_statcan.md`](control_plane/docs/RUNBOOK_statcan.md)** | Worked example: land a Statistics Canada API subset end-to-end. |
| **[`control_plane/docs/GOVERNANCE_SECURITY.md`](control_plane/docs/GOVERNANCE_SECURITY.md)** | Governance & security: what we've implemented, the full Lakehouse surface, and what a Warehouse adds. |
| **[`control_plane/docs/DESIGN.md`](control_plane/docs/DESIGN.md)** | Engine internals: the modular `src/cp/` package, how to add a connector, the bundler, the wheel. |
| **[`control_plane/docs/CICD.md`](control_plane/docs/CICD.md)** | CI/CD (GitHub Actions + Azure DevOps), service-principal auth, Key Vault. |
| **[`control_plane/SOLUTION.md`](control_plane/SOLUTION.md)** | Packaged-solution summary + roadmap. |

## Highlights

- **Modular engine** — the runtime is a package (`control_plane/src/cp/`): one file per connector,
  auto-registered; the pipeline notebooks are 3-cell shells. A bundler flattens it into the
  `%run cp_framework` cell (and builds a wheel). Add a connector = drop a file. See `docs/DESIGN.md`.
- **Pluggable connectors** — SQL Server / Postgres / MySQL / Oracle / DB2 / ODBC, a generalized
  **HTTP/API** connector, **Microsoft Entra** (Graph), and an **ad-hoc file** connector; add a source with config only.
- **Event-driven file dropbox** — drop csv/txt/xlsx into `LH_Filedrop/Files/newfile/<schema>/` and a
  OneLake event trigger loads each (one table per file / Excel tab) → bronze append → silver
  row-hash dedup → archive by date; idempotent. See `control_plane/docs/DESIGN.md` §8.
- **Connections in Key Vault** — `datasource.secret_name`; the `cp_connection_builder` wizard writes
  the secret *and* registers the datasource. No secrets (or hosts) in git or the variable library.
- **Auto-discovery** — the metadata step registers source objects (`is_active=0`); you review + activate.
- **DQ + cleansing + masking**, **schema-enabled medallion** (`datasource.sourceschema_table`), and a
  **gold star schema** (SCD1/2/fact) built in DAG order.
- **Code-driven data security** — OneLake CLS/RLS, Dynamic Data Masking, and static masking declared
  in `security_policy` and applied per environment by `cp_security.py`.
- **One-command environment provisioning** (`cp_bootstrap`) with service-principal auth and a GitHub
  Actions workflow.

## Quick start

```bash
# auth: service principal in .env, or `az login`
python control_plane/deploy/cp_bootstrap.py HackathonShuo DEV   # provision + deploy + load config
# then run cp_pl_main(load_group, run_id, src_user, src_password) in the workspace
```

> The legacy MXData/Databricks accelerator this evolved from lives separately; this repo is the
> Fabric-native control plane.
