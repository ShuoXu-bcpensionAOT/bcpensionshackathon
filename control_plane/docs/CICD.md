# CI/CD Guide — Deploying the Fabric Control Plane

How to host, build, and securely deploy this solution across environments, in **two
streams**: **GitHub Actions** and **Azure DevOps Pipelines**. Both call the same
`cp_bootstrap.py` (idempotent, deploy-only), so the logic is identical — only the
platform wiring differs.

## Two kinds of secret (know the difference)

| Context | What | Where it belongs |
|---------|------|------------------|
| **Deploy-time** | Identity that provisions/deploys Fabric items | CI identity — an Entra **service principal** via **OIDC / Workload Identity Federation** (no stored secret, preferred) or a client secret in the CI secret store |
| **Run-time** | The **source connection** used when `cp_pl_main` actually runs | **Azure Key Vault** — `datasource.secret_name` → a secret the worker reads at run time via `notebookutils.credentials.getSecret` (the service principal has KV *get*). *Not* needed for deploy (deploy ≠ run). |

> The CI/CD pipelines here only **deploy** (provision items + load config). They do **not**
> need the source password. The source password is a *run-time* secret consumed when the
> Fabric pipeline executes (schedule/trigger) — see [§6 Vault](#6-vault--secure-storage).

## The service principal (shared prerequisite)

Create one Entra app (service principal) and grant it:
1. **Fabric APIs** — tenant setting *"Service principals can use Fabric APIs"* (Admin portal),
   and *"Service principals can create workspaces, connections, and deployment pipelines"*.
2. **Workspace admin** — add the SP as Admin on each target workspace (or let it create them
   and it becomes admin).
3. **Capacity** — Contributor/assignment rights on the Fabric capacity.
4. **config_db** — grant the SP access to the SQL database (`CREATE USER [<sp>] FROM EXTERNAL
   PROVIDER; ALTER ROLE db_owner ADD MEMBER [<sp>];`).

Prefer **federated credentials (OIDC)** over a client secret — no secret to store or rotate.

---

## 1. Repository layout (both platforms)

Commit the package as-is; the CI file lives where each platform expects it:

```
<repo>/
├── .github/workflows/deploy.yml          # GitHub Actions
├── control_plane/
│   ├── cicd/azure-pipelines.yml          # Azure DevOps (point the pipeline at this path)
│   ├── environments/{dev,uat,prod}.yml   # per-env params (workspace_base, tenant, capacity)
│   ├── config/  variable_library/  notebooks/  deploy/  docs/
│   └── requirements.txt
```

Both CI files: install ODBC Driver 18 + `requirements.txt`, read `environments/<env>.yml`,
authenticate to Azure, then run `cp_bootstrap.py <workspace_base> <env>`.

---

## 2. Stream A — GitHub Actions

### 2.1 Put the asset on GitHub
```bash
git remote add origin https://github.com/<org>/<repo>.git
git push -u origin main
```
The workflow is `.github/workflows/deploy.yml` (already in the package).

### 2.2 Environments & approvals
Repo **Settings → Environments** → create **DEV**, **UAT**, **PROD**. On UAT/PROD add
**Required reviewers** (deployment approvals) and, if desired, branch restrictions. Secrets
can be scoped **per environment** (a PROD secret is only visible to a PROD-targeted run).

### 2.3 Secure auth — OIDC (recommended, no stored secret)
1. On the Entra app, add a **Federated credential**:
   *Issuer* `https://token.actions.githubusercontent.com`, *Subject*
   `repo:<org>/<repo>:environment:PROD` (one per environment), *Audience* `api://AzureADTokenExchange`.
2. Store only the **non-secret** `AZURE_CLIENT_ID` (and reuse `tenant_id` from the env file).
3. The workflow logs in via OIDC:
```yaml
permissions:
  id-token: write
  contents: read
# ...
      - uses: azure/login@v2
        with:
          client-id: ${{ secrets.AZURE_CLIENT_ID }}
          tenant-id: ${{ env.AZURE_TENANT_ID }}
          allow-no-subscriptions: true
```

**Fallback (client secret):** set repo/environment secrets `AZURE_CLIENT_ID` +
`AZURE_CLIENT_SECRET`; the shipped workflow uses
`az login --service-principal -u … -p … --tenant … --allow-no-subscriptions`.

### 2.4 Run it
**Actions → Deploy Control Plane → Run workflow**, pick the environment. (Push-to-main
auto-deploy is available — uncomment the `push:` block — but keep it off until secrets exist;
a preflight step fails early with a clear message if they're missing.)

---

## 3. Stream B — Azure DevOps Pipelines

### 3.1 Put the asset on Azure Repos (or use GitHub)
Push the repo to Azure Repos, **or** connect the pipeline to a GitHub repo. Then **Pipelines
→ New pipeline → Existing YAML** → select `control_plane/cicd/azure-pipelines.yml`.

### 3.2 Secure auth — service connection (WIF, recommended)
**Project Settings → Service connections → New → Azure Resource Manager → Workload Identity
federation**. Name it `fabric-sp` (matches the YAML). This creates/uses an Entra app with a
federated credential — no secret stored in DevOps. Grant that app the Fabric permissions
above. The `AzureCLI@2` task runs `az login` through this connection automatically.

### 3.3 Secrets from Key Vault — variable group
**Pipelines → Library → Variable group** `fabric-control-plane` → **Link secrets from an
Azure key vault**. Select your Key Vault and the secrets (e.g. `SOURCE-PASSWORD`). Grant the
pipeline's service connection **Get/List** on the vault. (For deploy-only you may not need
any — it's for the run-time path.)

### 3.4 Environments & approvals
**Pipelines → Environments** → create `fabric-dev`, `fabric-uat`, `fabric-prod`. Add
**Approvals and checks** (required approvers) on uat/prod. The YAML's `deployment` job
targets `fabric-<env>`, so the gate fires before deploy.

### 3.5 Multi-stage promotion (optional)
The shipped YAML deploys one chosen environment. For a promotion pipeline, add stages
`DEV → UAT → PROD`, each a `deployment` job to its Environment, chained with `dependsOn`; the
UAT/PROD Environment approvals become the gates between stages.

---

## 4. Multi-environment model

- One workspace per environment: **`<workspace_base>-<ENV>`** (e.g. `HackathonShuo-UAT`),
  created/updated by the bootstrap.
- Per-env inputs come from **`environments/<env>.yml`** (`workspace_base`, `environment`,
  `tenant_id`, `capacity_id`) — non-secret, in git.
- Per-env runtime values come from the **`cp_vars` Variable Library** value sets (lakehouse
  names, `source_server`) — swap the active value set per environment.
- Per-env **secrets** are scoped by the platform (GitHub Environment secrets / DevOps
  environment + KV-linked variable groups).

```
DEV env file + DEV secrets  ─┐
UAT env file + UAT secrets  ─┼─►  cp_bootstrap.py  ─►  <base>-<ENV> workspace
PROD env file + PROD secrets ┘        (same code, different parameters/identity)
```

---

## 5. Deploy vs. run

- **Deploy (CI/CD, this guide):** provisions workspace/lakehouses/`config_db`, deploys the
  variable library + notebooks + pipelines, loads config-as-code. Needs only the **SP identity**.
- **Run (schedule/trigger):** `cp_pl_main(load_group, run_id, src_user, src_password)`. Schedule
  it in Fabric (or via the Fabric Job Scheduler REST API). The **source password** is the
  run-time secret — see §6.

---

## 6. Vault & secure storage

### Deploy-time identity
Prefer **OIDC / Workload Identity Federation** (GitHub federated credential; DevOps WIF
service connection) so there is **no stored secret**. If you must use a client secret, keep it
in a **GitHub Environment secret** or an **Azure Key Vault-linked DevOps variable group**, and
rotate regularly.

### Run-time source secret (Azure Key Vault) — the target design
The source DB password should live in **Azure Key Vault** and be fetched by the worker at run
time instead of being passed as a parameter:
```python
# in a worker notebook, once a Key Vault + access exist:
pwd = notebookutils.credentials.getSecret("https://<vault>.vault.azure.net/", "source-db-password")
```
Grant the **workspace identity** (or the SP) *Key Vault Secrets User* on the vault. This
removes the password from run parameters entirely. Until a Key Vault is provisioned (needs an
Azure subscription in the tenant), the password is passed at trigger time from the scheduler /
secret store. See `portability-design.md` for the parked decision and the switch plan.

### What must never be in git
Service-principal secrets, source DB passwords, PATs, connection strings. The repo holds only
**non-secret** parameters (`environments/*.yml`) and config **structure**.

---

## 7. GitHub vs Azure DevOps — at a glance

| Concern | GitHub Actions | Azure DevOps |
|---------|----------------|--------------|
| CI file | `.github/workflows/deploy.yml` | `control_plane/cicd/azure-pipelines.yml` |
| Trigger | `workflow_dispatch` (env input); optional push | manual (parameter); optional CI |
| Secure auth | OIDC federated credential (preferred) or SP secret | WIF service connection (preferred) or SP secret |
| Secrets store | Repo/Environment **Secrets** | **Variable group** (linked to Key Vault) |
| Approvals | **Environments** → required reviewers | **Environments** → approvals & checks |
| Multi-env | env input → GitHub Environment | parameter → stage/Environment; multi-stage promotion |
| Vault | Key Vault via `az keyvault` after login, or OIDC | Native **KV-linked variable group** |

---

## 8. Quick checklists

**GitHub**
- [ ] Push repo; `.github/workflows/deploy.yml` present
- [ ] Create DEV/UAT/PROD Environments (+ reviewers on UAT/PROD)
- [ ] SP with Fabric/workspace/capacity/config_db rights
- [ ] OIDC federated credential per environment **or** `AZURE_CLIENT_ID/SECRET` secrets
- [ ] Fill `environments/*.yml`
- [ ] Run **Deploy Control Plane**

**Azure DevOps**
- [ ] Repo in Azure Repos (or GitHub connection)
- [ ] New pipeline → `control_plane/cicd/azure-pipelines.yml`
- [ ] WIF service connection `fabric-sp` (SP with Fabric rights)
- [ ] Variable group `fabric-control-plane` (KV-linked, if runtime secrets)
- [ ] Environments `fabric-dev/uat/prod` (+ approvals)
- [ ] Fill `environments/*.yml`
- [ ] Run the pipeline (pick environment)
