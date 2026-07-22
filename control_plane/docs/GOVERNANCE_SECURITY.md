# Data Governance & Security

How the control plane governs and secures data — **what we've implemented**, **what the Fabric
Lakehouse can do** natively, and **what a Fabric Warehouse adds** if we introduce a serving layer.

---

## 1. Principles

- **Config-as-code & promotable.** Security and governance are *declared* (rows in `config_db`,
  promoted as YAML), not clicked in a UI — so DEV → UAT → PROD is deterministic and auditable.
- **Least privilege, deny-by-default.** OneLake security grants nothing until a role says so.
- **Defense in depth.** Column hiding, row filtering, masking, and quality/quarantine are layered —
  Microsoft's own guidance is to use RLS + CLS + masking together.
- **No secrets in git or Delta.** Connections live in **Key Vault** (`datasource.secret_name`);
  only the secret *name* is in config. The variable library holds no connection info.
- **Separation of duties.** A deploy **service principal** provisions and reads secrets; humans
  author config; enforcement targets non-privileged **Viewer** identities.

---

## 2. What we've implemented (in this framework)

### 2.1 Code-driven data security — `security_policy` + `cp_security.py`
Security policies are rows in **`dbo.security_policy`** (config-as-code, promoted like `dq_rule`),
applied per environment by **`cp_security.py`**. Four methods, each with a different reach:

| Method (`method`) | Applied by | Enforced on | Use for |
|---|---|---|---|
| **`onelake_cls`** | OneLake data-access **role**, column whitelist (REST API) | **all engines incl. Spark** | hide columns entirely |
| **`onelake_rls`** | OneLake role, **T-SQL row predicate** | all engines incl. Spark | filter rows per principal |
| **`ddm`** | **Dynamic Data Masking** (`ALTER … ADD MASKED WITH` on the SQL endpoint) | SQL endpoint / Power BI | mask values (`email()`, `default()`, `partial()`) |
| **`mask`** | **static mask** cleanse function (silver build) | **everywhere** (stored masked) | irreversible redaction/hash |

Validated end-to-end on a synthetic `silver.hr.employees`: CLS hides `ssn`/`salary`, RLS filters
`region='BC'`, DDM masks `email`/`salary`, applied identically in **DEV / UAT / PROD** from one
`security_policy.yml`. Run: `CP_TARGET_WORKSPACE=<ws> python deploy/cp_security.py apply` (or `show`).

### 2.2 Broader governance already in the framework
| Area | How |
|---|---|
| **Data quality** | `dq_rule` (not_null/min/max/allowed_values/expression); error-severity failures **quarantined** off silver, all results logged to `dq_result`. |
| **Cleansing** | `cleanse_rule` fixes rows before validation (trim/normalize/mask/…). |
| **Schema drift** | Column adds/removes detected + logged (`schema_drift_event`). |
| **Auditability** | Every run + object-load logged (`ingestion_run`, `object_load_run`, `pipeline_run_log`). |
| **Secrets** | Connections in **Key Vault**; resolved at run time by the service principal; nothing in git/Delta. |
| **Controlled onboarding** | Objects are **discovered `is_active=0`** — reviewed and activated deliberately, never silently loaded. |
| **Promotion** | Config (incl. security) is versioned YAML applied per environment — repeatable, reviewable. |

---

## 3. What the Fabric **Lakehouse** can do (native surface)

Our medallion runs on **schema-enabled lakehouses**. The full security surface available there:

| Capability | What it does | Enforced on | We use it? |
|---|---|---|---|
| **OneLake security roles — CLS** | Hide columns (column whitelist) | **all engines incl. Spark, SQL endpoint, Power BI Direct Lake** (GA) | ✅ `onelake_cls` |
| **OneLake security roles — RLS** | T-SQL row predicate per principal | **all engines incl. Spark** (GA) | ✅ `onelake_rls` |
| **Dynamic Data Masking** | Mask values (email/default/random/custom) on the **SQL analytics endpoint**; `GRANT/DENY UNMASK` | SQL endpoint / Power BI (Spark **bypasses**) | ✅ `ddm` |
| **Workspace roles** | Admin / Member / Contributor / Viewer — the first boundary | control-plane + data | ✅ (SP admin; Viewer for restricted users) |
| **Item permissions / sharing** | Per-item read/share; ReadAll vs Write | item access | ✅ |
| **Sensitivity labels** (Purview Information Protection) | Classify + optionally encrypt; labels flow downstream to Power BI/exports | classification/DLP | ◻ available (not yet wired) |
| **Microsoft Purview** | Catalog, lineage, DLP policies, insider-risk, access policies | governance plane | ◻ available |
| **Private networking** | Managed Private Endpoints / private links (incl. on-prem via PLS + ExpressRoute) | connection security | ◻ available (see WORKING_GUIDE §4.11) |

**Lakehouse limitations to know:**
- **Masking is not cross-engine.** OneLake CLS *hides* columns everywhere, but *partial masking*
  (DDM) is enforced only on the **SQL analytics endpoint** — a Spark reader of the Delta bypasses it.
  For masking that must hold on Spark, use the static **`mask`** cleanse (stored masked) or CLS-hide.
- **The lakehouse SQL endpoint is read-only.** You get RLS/CLS (via OneLake security) and DDM, but
  **no `GRANT/DENY` object management, views, or stored procedures** as a security layer there.
- **Enforcement bypasses privileged users.** Admin/Member/Contributor (and table owners, for
  `UNMASK`) see clear data — restricted users must be **Viewers**.

---

## 4. What an additional Fabric **Warehouse** adds

A Fabric **Warehouse** is a *writable* T-SQL store. Standing one up (e.g. as the **serving/gold
layer** for BI and SQL consumers) unlocks the mature SQL Server security model that the lakehouse
SQL endpoint can't offer:

| Capability | Warehouse (T-SQL) | vs. Lakehouse |
|---|---|---|
| **Object-level security** | `GRANT` / `DENY` / `REVOKE` on schemas, tables, views, procedures | Lakehouse: OneLake roles only (no fine-grained T-SQL grants) |
| **Column-level security** | `GRANT SELECT ON t(col)` / `DENY` — true column grants | Lakehouse: CLS-hide via OneLake role |
| **Row-level security** | Native **`CREATE SECURITY POLICY`** + inline predicate functions | Lakehouse: RLS via OneLake role predicate |
| **Dynamic Data Masking** | `ALTER TABLE … ADD MASKED WITH` + `GRANT UNMASK` on a **writable** store | Lakehouse: DDM on the read-only SQL endpoint |
| **Views & stored procedures** | Expose **curated views**, `DENY` base tables — a security abstraction layer | Not available on the lakehouse SQL endpoint |
| **Cross-store queries** | Query the lakehouse from the warehouse via T-SQL (serve gold without copying) | — |

**When to add one:** when the **consumption layer** (Power BI, analysts, downstream apps) needs
fine-grained, T-SQL-native governance — role-based column grants, view-based exposure, stored-proc
access patterns — beyond what OneLake security + DDM give on the lakehouse. It does **not** replace
OneLake security for the **engineering** plane (Spark reads of bronze/silver stay governed by
OneLake CLS/RLS).

---

## 5. Recommended architecture

```
                 ┌─────────────────────────── OneLake security (RLS/CLS) — cross-engine ──────────────┐
 sources ──► bronze ──► silver ──► gold        (Spark / SQL endpoint / Direct Lake)                    │
 (KV creds)   (schema-enabled lakehouses; DQ, cleanse, mask, quarantine, audit)                        │
                                   │                                                                    │
                                   └──►  (optional) Fabric WAREHOUSE = serving layer                    │
                                         full T-SQL GRANT/DENY, RLS policies, CLS, DDM, curated views ──┘
                                         → Power BI / analysts / apps
```

- **Data-engineering plane (lakehouse):** OneLake security (CLS/RLS enforced on Spark) + our
  `security_policy`/`cp_security` + DQ/cleanse/mask. This is what we've built.
- **Serving plane (optional warehouse):** promote gold into a Warehouse and govern consumption with
  full T-SQL security (views, column grants, security policies). Extend `cp_security.py` with a
  `warehouse_*` method that emits the T-SQL — same config-as-code, same promotion.
- **Cross-cutting:** sensitivity labels + Purview for classification/lineage/DLP; Key Vault for
  secrets; Managed Private Endpoints for private/on-prem connectivity.

---

## 6. Operating notes

- **Testing enforcement:** OneLake CLS/RLS and DDM only bite for **non-privileged** users. To verify
  live, query as a **Viewer** (admins see clear data). `cp_security.py show` lists applied roles +
  `sys.masked_columns` so you can confirm the *definitions* are in place regardless.
- **Promotion:** edit `security_policy` in DEV → `cp_export_config` → commit → `cp_config` + `cp_security`
  on the target env. Role names must be **letters/numbers only**; RLS predicates must be
  **schema-qualified** (`select * from hr.employees where …`).
- **Roadmap (available, not yet wired):** sensitivity labels + Purview integration; a `warehouse_*`
  policy method for a serving warehouse; automated Viewer-based enforcement tests.

---

### Sources
- OneLake security (roles, RLS/CLS, cross-engine): Microsoft Learn — *OneLake security access control model*.
- Dynamic Data Masking (Warehouse & Lakehouse SQL endpoint): Microsoft Learn — *Dynamic Data Masking in Fabric*.
- Warehouse security (GRANT/DENY, RLS security policies, CLS): Microsoft Learn — *Secure your Fabric Data Warehouse*, *Column-level security*, *SQL granular permissions*.
- On-prem connectivity (managed private endpoints): Microsoft Learn — *Connect on-premises data sources using managed private endpoints*.
