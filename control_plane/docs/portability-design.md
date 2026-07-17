# Control Plane Portability Design

Goal: **zero hardcoded values**; the control plane deploys and runs in any environment
given only a service principal, a `workspaceName`, and an `environmentName`.

## Principles

1. **Notebooks self-configure at runtime.** No workspace ID, lakehouse ID, or
   environment baked into code.
2. **Name-based, known values live in the repo** (Variable Library value sets,
   `parameter.yml`) and are swapped by CICD per environment.
3. **ID-based values are never stored** — they are resolved at runtime from the
   workspace the notebook is running in.
4. **Secrets live in a Fabric Connection**, never in repo/.env/variable library.

## Confirmed decisions

| Fork | Decision |
|------|----------|
| Lakehouse IDs | **Runtime name→ID** via `notebookutils.lakehouse.list()` |
| Source credentials | **Fabric Connection** (managed; referenced by name) |
| Deploy tooling | **fabric-cicd** for items + `parameter.yml`, wrapped by our SP bootstrap |

## Verified runtime capabilities (spike 2026-07-17)

- `notebookutils.runtime.context["currentWorkspaceId"]` / `currentWorkspaceName`
- `notebookutils.lakehouse.list()` → `[{name, id}]`
- `notebookutils.variableLibrary` present
- `notebookutils.connections`, `notebookutils.credentials` present
- (Observed: the workspace was renamed `Hackathon-DEV` → `HackathonShuo-DEV`;
  GUIDs stayed stable — this is exactly why name paths are unreliable and runtime
  resolution is required.)

## Architecture

### Framework bootstrap (`cp_framework`)
```python
ctx   = notebookutils.runtime.context
WS_ID = ctx["currentWorkspaceId"]
_lh   = {l["name"]: l["id"] for l in notebookutils.lakehouse.list()}
LH    = {logical: _lh[name] for logical, name in LAYER_NAMES.items()}
```
`LAYER_NAMES` (logical layer → physical lakehouse name) comes from the Variable
Library, defaulting to identity (`bronze→bronze`, …) when names are conventional.

### Variable Library (`cp_vars`) — repo-tracked, CICD-swapped
Per-environment value set holds only name-based/known values:
- `layer_names` — logical→physical lakehouse name map (for env-prefixed names)
- `source_connection` — Fabric Connection name for the source DB
- `source_database`, `load_groups`, and any env toggles

Notebooks read via `notebookutils.variableLibrary.get("$(/**/cp_vars/<name>)")`.

### Source credentials — Fabric Connection
A managed Connection (e.g. `src_adventureworks`) holds server + database + login.
Notebooks obtain the connection at runtime (no password in code). The SP/workspace
identity is granted use of the connection. `.env` source creds are removed.

### Promotion bootstrap (SP-driven, one entry point)
Inputs: **SP id+secret, tenant, `workspaceName`, `environmentName`**.
1. `target = f"{workspaceName}_{environmentName}"` → find-or-create workspace, bind capacity
2. Create lakehouses (from `layer_names`) if missing
3. Ensure the source Connection exists (or bind an existing one)
4. `fabric-cicd` deploy notebooks + `parameter.yml` find/replace for the env
5. Create/update the `cp_vars` value set for this environment
6. Load config-as-code into `metadata`
7. Runnable — no manual ID edits (IDs resolve at runtime)

## Phased implementation plan

| Phase | Scope | Risk | Verifiable by |
|-------|-------|------|---------------|
| **1. De-hardcode framework** | WS_ID from context, LH from name resolution; drop GUID dict | low | re-run e2e in current workspace, unchanged results |
| **2. Variable Library** | create `cp_vars`; move source server/db + layer names there; framework/notebooks read it | med | e2e reads config from var lib |
| **3. Secrets** | ~~Fabric Connection~~ **PARKED** | — | see note below |
| **4. Bootstrap + promote** ✅ | `cp_bootstrap.py`: find-or-create workspace on trial capacity, create lakehouses, var lib, deploy notebooks, load config, run e2e | high | **DONE — promoted to fresh `HackathonShuo-UAT`, e2e green (9/9/7, DQ pass, gold ordered), zero manual ID edits** |

Each phase is independently shippable. Phase 1 removes every GUID from the code and
is the biggest immediate win.

## Promotion (validated)

```bash
az login --tenant <t> --scope https://api.fabric.microsoft.com/.default --allow-no-subscriptions
python scripts/cp_bootstrap.py HackathonShuo UAT     # or PROD
```
Inputs today: personal login + source password from `.env`. To switch to a service
principal, log in as the SP (same `az`/token path) — no code change. `cp_bootstrap.py`
targets any workspace via `CP_TARGET_WORKSPACE` / `CP_TARGET_WORKSPACE_ID`, which the
tooling honors over `.env`. fabric-cicd can replace the `cp_deploy.py deploy` step for
notebook deployment when desired; the bootstrap already handles workspace/lakehouse/
var-lib/config which fabric-cicd does not.

## Phase 3 finding — secrets (PARKED)

Empirically confirmed (2026-07-17):
- A Fabric **Connection** was created + test-connected, but a **Spark notebook cannot
  extract its Basic password** — `notebookutils.credentials.getSecretWithConnection`
  and `connections.getCredential` fail with *"Artifact Connection does not exist"*
  (the connection must be bound to the notebook artifact, which isn't exposed via a
  public API; Fabric guards connection creds from user code).
- **Key Vault** (the clean alternative) needs an Azure subscription; the `pensionsbc`
  tenant currently has none.

**Decision: park it.** Current secret handling = **runtime injection**: the source
password is passed as a notebook run-time parameter (from `.env` locally, from a
CI secret store in automation). Nothing secret lives in the repo, Variable Library,
or Delta. **Key Vault is the agreed target** once a subscription is available
(`notebookutils.credentials.getSecret(kvUri, secret)` works from Spark). The
`source_connection` variable + `cp_connection.py` are retained for that future work.

## ALM model — items vs data (decided)

The meaningful boundary is **Fabric items vs data**, not "infra code vs content code".

**Items** (engine notebooks, `sq_*` source-query notebooks, `cp_vars` var-lib, lakehouse
shells) travel together as one unit:
- **Authoring:** DEV workspace **git-integrated** to the repo (feature branch → PR → main).
- **Promotion:** **fabric-cicd** deploys `main` → UAT → PROD; per-env values come from the
  var-lib **value sets**. (Our `cp_bootstrap`/API tooling remains for initial provisioning.)

**Data** takes its own route, in two classes:
- **Runtime state / logs** (`watermark_state`, `object_load_run`, `ingestion_run`,
  `dq_result`, `schema_drift_event`, `source_column`) → Delta in the lakehouse.
  Per-environment, **never promoted**.
- **Authored config** (`datasource`, `source_object`, `dq_rule`, `model`, `gold_object`,
  `gold_dependency`) → lives in a **Fabric SQL Database (`config_db`)**, because the users
  are SQL-native and the lakehouse SQL endpoint is read-only. **The SQL tables are the
  source of truth** — users edit them via **T-SQL** (INSERT/UPDATE/MERGE) with enforced
  PK/FK constraints. Promotion is **SQL → YAML → SQL**:

  ```
  DEV: edit config tables in config_db (T-SQL)
       ->  cp_export_config (SQL -> YAML)  ->  git PR
  Promote: fabric-cicd (items)  +  cp_config (YAML -> UAT/PROD config_db)
  ```

  - **Engine reads** config from `config_db`'s **OneLake mirror** (Delta) — zero-cred,
    resolved at runtime (`getToken('pbi')` → find `config_db` GUID → read
    `abfss://…/{sqldb}/Tables/dbo/{table}`). Spark handles the mirror's `deletionVectors`.
  - **Local tooling** (`cp_config` / `cp_export_config`) uses **pyodbc + AAD token**
    (`cp_sqldb.py`); delta-rs can't read the mirror, which is fine.
  - **Mirror lag** is near-real-time; the bootstrap waits for the mirror after a config
    load before running.
  - Round-trip verified (SQL → YAML → SQL) and DEV e2e green reading config from the SQL DB.

## Open items

- Variable Library create/update + value-set switch via REST/SP (for CICD).
- fabric-cicd support for Variable Library items (else bootstrap handles it — as
  `cp_varlib.py` already does).
- Key Vault wiring (deferred until an Azure subscription is attached).
