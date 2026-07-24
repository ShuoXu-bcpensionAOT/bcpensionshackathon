# BC Pension Corporation — Fabric Delivery Guide

**What this is.** A plain-language companion to the milestone plan. For every item in the three
milestones it answers two questions: **what it means in Microsoft Fabric** (concept, not deep tech)
and **how we control it** — either because our Fabric **platform** already handles it, or because we
recommend a **standard** for BCPC to adopt.

**Audience.** Anyone with a working familiarity of Fabric. We stay at the "what and how we manage
it" level and avoid implementation detail.

**How to read each item.** Every item below carries one tag:

- **Platform** — already implemented in our Fabric control plane (our config-as-code framework does it).
- **Standard** — a governance/operating standard we recommend BCPC adopt (a policy or decision, not code).
- **Both** — the platform provides the mechanism; a standard sets the policy on top of it.

> Our **control plane** is the config-as-code framework we built on Fabric: you declare *what* to
> load and *how* to govern it as configuration, and the framework connects, ingests, cleanses,
> quality-checks, models, and secures the data across DEV / UAT / PROD. Wherever an item says
> **Platform**, this is what's doing the work.

### The three milestones at a glance

```mermaid
flowchart LR
    M1["Milestone 1<br/>Platform Foundation<br/><i>infra · workspaces · governance<br/>security · architecture</i>"]
    M2["Milestone 2<br/>Data Integration<br/><i>SQL MI · Oracle<br/>ingestion · data products</i>"]
    M3["Milestone 3<br/>Analytics & Reporting<br/><i>semantic models<br/>reporting · Power BI</i>"]
    M1 --> M2 --> M3
```

*Foundation first (the platform everything runs on), then get data in and managed, then turn it
into governed analytics.*

---

## Milestone 1 — Fabric Platform Foundation

**Goal:** stand up the infrastructure, governance, security, architecture, and operating framework
that everything else runs on.

### How the platform fits together

```mermaid
flowchart LR
    SRC[(Sources<br/>databases · APIs<br/>files · on-prem / OCI)] -->|gateway<br/>or mirroring| B
    subgraph OneLake["OneLake — medallion lakehouses"]
      direction LR
      B[Bronze<br/>raw · append-only] --> S[Silver<br/>clean · quality · masked] --> G[Gold<br/>star schema]
    end
    G --> SM[Semantic model<br/>Direct Lake] --> PBI[Power BI<br/>reports]
    SEC[[Security & Governance<br/>Entra groups · row/column security<br/>masking · lineage · audit]] -. governs .-> OneLake
```

*Everything sits on a **Capacity**, is organized into **Workspaces / Domains**, and flows through
the **medallion** to reports — with **security and governance** applied across every layer. The
items below are the pieces of this picture.*

### Establish Fabric Infrastructure Foundation

*The physical footing: the compute we buy, the network path to our data, and the replication options.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Provision Fabric Capacity** | The compute + storage tier (an F-SKU) that powers every workspace, pipeline, and report in the tenant. | **Standard.** Size an F-SKU to the workload and assign it to the project workspaces; our provisioning script then stands up everything that runs on it, identically per environment. |
| **Configure Capacity Administration** | The admin controls for that capacity — who manages it, feature switches, workload limits, surge protection. | **Standard.** A named capacity-admin group manages it in the Admin portal; we recommend surge-protection and throttling guardrails so one workload can't starve others. |
| **Deploy Fabric Data Gateway** | Software that lets Fabric reach data behind a private network (on-prem or private OCI/Azure). | **Both.** Our ingestion routes private sources through the gateway; we've documented the gateway architecture, including reusing the **existing VNet gateway** vs standing up a dedicated one. |
| **Validate Gateway Connectivity** | Confirming Fabric can actually reach a source through the gateway. | **Standard.** A "test connection" per source before onboarding, plus a documented checklist (firewall, DNS, service account). |
| **Review Gateway Architecture** | Choosing gateway type and network path (VNet data gateway vs on-premises data gateway). | **Platform.** We produced the private-OCI architecture diagrams — the framework path reusing the VNet gateway, and the mirroring path requiring a dedicated on-prem gateway. |
| **Validate SQL MI Mirroring Architecture** | Confirming near-real-time mirroring works for Azure SQL Managed Instance. | **Both.** Mirroring is Microsoft-managed; we've mapped how a mirrored database feeds our Silver/Gold and recommend it for specific near-real-time tables. |
| **Assess Oracle Mirroring Feasibility** | Whether Oracle mirroring fits BCPC (prerequisites, limits, cost, promotion). | **Platform.** Delivered as our *Bronze vs Mirroring* analysis — GA status, the invasive prerequisites, coverage limits, gateway needs, and a clear recommendation. |
| **Document Infrastructure Architecture** | A written, shared picture of the platform's infrastructure. | **Platform.** The architecture diagrams and this guide are that documentation, kept in the repo alongside the code. |

### Establish Fabric Platform Foundation

*The logical structure: workspaces, domains, and the lakehouses where data lives — plus the reusable patterns that make new work fast.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Create Initial Fabric Workspaces** | Workspaces are the containers that hold items (lakehouses, pipelines, reports) and carry access + capacity. | **Platform.** Our provisioning script creates the workspaces per environment; nothing is hand-clicked. |
| **Define Workspace Structure** | How you divide work into workspaces (by environment, domain, project, or product). | **Standard.** We recommend an environment split (DEV / UAT / PROD) with domain-aligned workspaces inside, so promotion and access stay clean. |
| **Define Domain Alignment Strategy** | Fabric **domains** group workspaces by business area for ownership and discovery. | **Standard.** Map domains to BCPC business areas (e.g. Pensions, Employer, Member) so ownership and data discovery follow the org. |
| **Create Initial Lakehouses** | Lakehouses are the storage+table layer in OneLake where data lands and is modelled. | **Platform.** The framework creates the schema-enabled lakehouses (metadata, bronze, silver, gold, file-drop) as declared config. |
| **Define Lakehouse Organization Standards** | Naming and layout of lakehouses, schemas, and tables. | **Platform.** Enforced by convention: schema-enabled lakehouses, medallion layers, and a single naming rule (`schema.table`), all driven from config. |
| **Implement Public Dataset Reference Solution** | A pattern for bringing external/open reference data into the platform. | **Platform.** Demonstrated end-to-end with a Statistics Canada open-data feed loaded through our generic HTTP connector to Silver. |
| **Validate Auto-Ingestion Framework** | Proving that data can be onboarded by configuration rather than bespoke code. | **Platform.** This is the core of the control plane — plus an event-driven "drop a file, it loads itself" intake — validated on real data. |
| **Establish Reusable Platform Patterns** | Turning one-off solutions into repeatable building blocks. | **Platform.** One registry of connectors, config-as-code onboarding, and a packaged engine mean a new source is *configuration*, not new code. |

### Define Fabric Platform Governance

*Who runs the platform and by what process — ownership, capacity, onboarding, and endorsement.*

```mermaid
flowchart TB
    PO["Platform Owner<br/>capacity · standards · admin"]
    CA["Capacity Admin<br/>sizing · guardrails"]
    PA["Platform Admins<br/>tenant · gateways · releases"]
    DO["Domain Owners<br/>per business area"]
    DS["Data Stewards<br/>meaning · quality · access"]
    DPO["Data Product Owners<br/>Gold products"]
    PO --> CA
    PO --> PA
    PO --> DO
    DO --> DS
    DO --> DPO
```

*The operating model in one picture: a central **Platform Owner** sets standards and runs capacity;
**Domain Owners** own their business area's data, supported by **Stewards** and **Data Product
Owners**.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Define Platform Ownership Model** | Who owns the Fabric platform overall (the "landlord" of capacity, admin settings, standards). | **Standard.** A platform-owner role (small central team) accountable for capacity, tenant/admin settings, and the standards in this guide. |
| **Define Workspace Creation Process** | How new workspaces get requested, approved, and created consistently. | **Both.** A request→approve step feeding our provisioning script, so every workspace is created to standard (naming, access, capacity) rather than ad hoc. |
| **Define Workspace Ownership Standards** | Who owns each workspace and is accountable for its contents. | **Standard.** Each workspace has a named owner (domain-aligned) responsible for access reviews and its data products. |
| **Define Capacity Governance Process** | How capacity is monitored, allocated, and protected from overload. | **Standard.** Monitor via the Capacity Metrics app; set throttling/surge guardrails; a defined path to scale or rebalance workloads. |
| **Define Platform Administration Responsibilities** | The concrete duties of the admins (tenant settings, gateways, updates, incidents). | **Standard.** A responsibilities matrix (RACI) covering tenant settings, gateway upkeep, capacity, and release management. |
| **Define Monitoring and Support Model** | How platform health and issues are watched and resolved. | **Both.** Fabric monitoring + our run/audit logs feed a defined support model (who watches what, escalation path). |
| **Define Onboarding Process** | The repeatable path for a new team, source, or data product to join the platform. | **Both.** A checklist that ends in config-as-code entries + our discovery step, so onboarding a source is guided and consistent. |
| **Define Endorsement and Certification Process** | Fabric's **Promoted / Certified** labels that signal trustworthy, official items. | **Standard.** Define who can certify, the criteria (owner, quality, documentation), and reserve "Certified" for governed Gold data products. |

### Define Fabric Data Governance Framework

*Who owns the data and how its quality, meaning, lineage, and lifecycle are managed.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Define Data Domain Ownership Model** | Which business area owns which data. | **Standard.** Data domains map to business areas with a named domain owner accountable for the data within. |
| **Define Data Steward Responsibilities** | The people who curate meaning, quality, and access for a data area. | **Standard.** A steward role per domain — owns definitions, quality rules, and access decisions for their data. |
| **Define Data Product Ownership Model** | Treating governed datasets as "products" with an owner and consumers. | **Standard.** Gold outputs are the data products; each has an owner, a documented purpose, and a consumer contract. |
| **Define Metadata Management Approach** | How we capture what data exists, its schema, and its meaning. | **Both.** The platform captures technical metadata (sources, schemas, drift, run history) in config + audit tables; the OneLake catalog + a business glossary standard add business meaning. |
| **Define Data Quality Standards** | The rules that decide whether data is fit to use. | **Platform.** Declarative quality rules (not-null, ranges, allowed values, expressions) run on the way to Silver, with failing rows quarantined off the clean layer. |
| **Define Data Lineage Requirements** | Being able to trace where data came from and what it feeds. | **Both.** Fabric's built-in lineage view across items, plus our audit tables that record every source→bronze→silver→gold run. |
| **Define Data Lifecycle Standards** | Retention, history, and archival of data over time. | **Both.** Bronze is an append-only history (audit trail); Silver/Gold hold current + modelled history (slowly-changing dimensions); a retention standard sets how long each is kept. |
| **Define Data Sharing Standards** | How data is shared across teams without copying it. | **Standard.** Prefer OneLake shortcuts / sharing over copies, governed by access model + certification, so there's one trusted copy. |

### Define Fabric Security Framework

*Who can see what — from workspace down to the row and column.*

```mermaid
flowchart TB
    E["Entra security groups<br/>(access granted to groups, not people)"] --> W["Workspace roles<br/>Admin / Member / Contributor / Viewer"]
    W --> L["Lakehouse access<br/>which tables"]
    L --> C["Column-level security<br/>hide sensitive columns"]
    L --> R["Row-level security<br/>filter rows by rule"]
    C --> M["Data masking<br/>dynamic + static"]
    R --> M
    M --> AU["Audit<br/>who saw / did what"]
```

*Access narrows as you go down: from broad workspace roles to specific tables, then to individual
rows and columns, then masked values — all granted through **Entra groups** and recorded in
**audit**.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Define Entra Security Group Strategy** | Using Microsoft Entra ID groups (not individuals) to grant access. | **Both.** Access is granted to Entra groups; our security config references those groups so membership — not per-person edits — drives access. |
| **Define Workspace Access Model** | The workspace roles (Admin / Member / Contributor / Viewer) that set broad access. | **Standard.** A least-privilege role mapping per workspace, assigned to Entra groups and reviewed on a cadence. |
| **Define Lakehouse Access Model** | Fine-grained access to tables/columns/rows within a lakehouse. | **Platform.** Applied from config as OneLake column-level and row-level security — sensitive columns and restricted rows are enforced consistently per environment. |
| **Define Row-Level Security Standards** | Restricting which **rows** a user can see (e.g. by region or plan). | **Platform.** Row filters declared in config and applied by the framework, so the same rule ships to every environment. |
| **Define Data Masking Requirements** | Hiding or obscuring sensitive values (e.g. SIN, salary). | **Platform.** Dynamic masking at the query layer plus static masking during processing — sensitive columns can be masked, or never land in raw form at all. |
| **Define Audit Requirements** | Recording who did what, and what the pipeline did. | **Both.** Fabric's tenant audit logs for user activity, plus our run/quality/drift audit tables for the data pipeline itself. |
| **Define Compliance Requirements** | Meeting BCPC's regulatory and privacy obligations. | **Standard.** Map controls (access, masking, audit, residency) to the applicable obligations; the platform's security + audit features provide the evidence. |

### Define Fabric Architecture Standards

*The blueprint everyone builds to — layers, storage choice, and how data is brought in and shaped.*

```mermaid
flowchart LR
    B["Bronze<br/>raw copy · every load kept<br/>audit history"] --> S["Silver<br/>dedupe · quality rules<br/>masking · soft-delete"] --> G["Gold<br/>star schema · SCD history<br/>business-ready"]
```

*The **medallion** in one line: raw in Bronze, cleaned and governed in Silver, business-ready in
Gold. Each item below is a rule for one part of this flow.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Define Bronze / Silver / Gold Architecture** | The medallion pattern: raw → cleaned/conformed → business-ready. | **Platform.** This is the spine of the control plane — Bronze (raw, append-only), Silver (deduped, quality-checked, masked), Gold (modelled star schema). |
| **Define Lakehouse vs Warehouse Guidance** | When to use a Lakehouse (files+tables, Spark) vs a Warehouse (T-SQL). | **Standard.** Default to Lakehouse for the medallion (what we've built); reach for a Warehouse only where a pure T-SQL surface is required — with the trade-offs documented. |
| **Define Ingestion Patterns** | The approved ways data comes in (batch pull, event/file drop, mirroring, gateway). | **Platform.** One connector registry covers databases, APIs, files (event-driven drop), and gateway-staged on-prem; mirroring is the managed option for near-real-time. |
| **Define Transformation Patterns** | The approved ways data is cleaned and modelled. | **Platform.** Config-driven cleanse + quality rules into Silver, and SQL-based transforms into a Gold star schema with slowly-changing-dimension history. |

---

## Milestone 2 — Data Integration Foundation

**Goal:** prove BCPC can onboard, move, transform, and manage enterprise data in Fabric.

### Implement SQL Managed Instance Integration

*Bring an Azure SQL Managed Instance in via mirroring and prove it stays fresh.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Validate Mirroring Configuration** | Setting up and confirming a mirrored SQL MI database in Fabric. | **Both.** Mirroring is Microsoft-managed; we validate the setup and confirm the replica lands in OneLake ready for Silver/Gold. |
| **Validate Incremental Data Synchronization** | Confirming ongoing changes flow through continuously (not just the first load). | **Both.** Mirroring streams changes automatically; we verify inserts/updates/deletes propagate and reconcile against expectations. |
| **Define Operational Procedures** | The run-book for keeping mirroring healthy (start/stop, re-seed, monitor). | **Standard.** A documented procedure for monitoring replication state, handling re-seeds, and responding to pauses. |
| **Measure Refresh Performance** | How fresh the mirrored data is (latency) and its load impact. | **Standard.** Baseline latency and resource use, with thresholds that tell us when to tune or scale. |

### Implement Oracle Integration

*Determine how BCPC's Oracle data comes in — and the trade-offs.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Validate Connectivity** | Confirming Fabric can reach the Oracle database (through the gateway). | **Both.** Our Oracle connector reaches the source via the gateway; a connection test confirms the path before onboarding. |
| **Assess Supplemental Logging Requirements** | Oracle change-data-capture needs supplemental logging enabled — an invasive DBA change. | **Platform.** Documented precisely in our analysis (database- and table-level logging), so BCPC's DBAs know exactly what mirroring would require. |
| **Review LogMiner Requirements** | Mirroring reads Oracle's redo logs via LogMiner, which has prerequisites and limits. | **Platform.** Captured in the analysis — LogMiner prerequisites, the broad grants required, and the read-write-source constraint. |
| **Document Limitations and Recommendations** | A clear statement of what works, what doesn't, and the recommended approach. | **Platform.** Delivered as the *Bronze vs Mirroring* recommendation: framework-based ingestion by default, mirroring selectively for near-real-time tables. |

### Establish Data Ingestion Framework

*Make ingestion standard, reusable, monitored, and resilient.*

```mermaid
flowchart LR
    DB[(Databases<br/>SQL · Oracle · DB2 · PG · MySQL)] --> C
    API[(APIs / open data<br/>HTTP)] --> C
    FILE[(Files<br/>drop csv / xlsx)] --> C
    ONP[(On-prem / OCI<br/>via gateway)] --> C
    C{{"Connector registry<br/>(config-driven, reusable)"}} --> B[Bronze]
    MIR[(Mirroring<br/>near real-time)] --> B
```

*Many source types, **one config-driven connector registry**, one landing pattern. Mirroring is the
managed option that lands straight in Bronze for near-real-time needs.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Define Ingestion Standards** | The consistent way every source is onboarded and lands. | **Platform.** Every source is a config entry (connection, keys, load type); the framework applies the same landing, history, and naming rules to all. |
| **Create Reusable Ingestion Templates** | Templates so a new source doesn't start from scratch. | **Platform.** Adding a source is filling in config; adding a *new kind* of source is dropping one connector file — both are templated, not bespoke. |
| **Implement Monitoring and Alerting** | Knowing when a load runs, succeeds, or fails. | **Both.** Every run is logged to audit tables with status and row counts; failures are captured for alerting, wired into the support model. |
| **Implement Error Handling Standards** | Predictable behaviour when something goes wrong. | **Platform.** Failures are isolated per source (one bad file/source doesn't stop the rest), captured with detail, and safe to re-run (idempotent). |

### Establish Data Product Standards

*Turn governed datasets into managed, published, certified products.*

```mermaid
flowchart LR
    D[Draft] --> P[Published] --> Cert[Certified] --> R[Retired]
```

*A **data product** (a governed Gold dataset) moves through a defined lifecycle with an accountable
owner at every stage.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Define Data Product Lifecycle** | The stages a data product moves through (draft → published → certified → retired). | **Standard.** A defined lifecycle with entry/exit criteria at each stage, owned by the data product owner. |
| **Define Publication Standards** | What "published and ready to consume" means. | **Standard.** A publication checklist — owner, documentation, quality passing, access set — before a Gold product is announced. |
| **Define Ownership Standards** | Every product has a clear, accountable owner. | **Standard.** Named owner per data product, recorded and visible, responsible for quality and consumer support. |
| **Define Certification Process** | Using Fabric endorsement to mark trusted, official products. | **Standard.** Only reviewed Gold products earn "Certified"; the process defines reviewer, criteria, and re-review cadence. |

---

## Milestone 3 — Analytics & Reporting Foundation

**Goal:** demonstrate end-to-end analytics on Fabric — from modelled data to governed reports.

### From Gold to a governed report

```mermaid
flowchart LR
    G[Gold<br/>data product] --> SM["Semantic model<br/>Direct Lake (no copy)"] --> R[Report] --> U["Consumers<br/>via app / Viewer"]
    SEC["Row & column security<br/>flows through"] -. enforced .-> SM
```

*Reports read the **Gold** model live (Direct Lake — no data copy), through a **shared semantic
model**, with **security enforced end-to-end**. The items below govern each hop.*

### Establish Semantic Model Standards

*The shared business model that reports sit on.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Define Semantic Model Architecture** | The reusable model (measures, relationships) between Gold data and reports — ideally Direct Lake. | **Both.** Our Gold star schema is built to be Direct-Lake-ready; the standard defines shared models over Gold rather than per-report copies. |
| **Define Naming Standards** | Consistent, business-friendly names for tables, measures, and fields. | **Standard.** A naming convention (business terms, consistent casing) so models read the same across teams. |
| **Define Reuse Standards** | Sharing one certified model instead of rebuilding per report. | **Standard.** One governed semantic model per subject area, reused by many reports; discourage private duplicates. |
| **Define Certification Standards** | Marking the trusted, official semantic models. | **Standard.** Certification criteria for models (owner, built on Gold, documented, quality-backed), aligned with the data-product process. |

### Establish Reporting Standards

*How reports are governed, published, and consumed.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Define Report Governance Standards** | Rules for how reports are built, reviewed, and trusted. | **Standard.** Reports build on certified semantic models, follow design/accessibility guidelines, and are reviewed before wide release. |
| **Define Publishing Standards** | Where and how reports are released to audiences. | **Standard.** Publish through governed workspaces / apps with defined audiences, versioning, and a clear promotion path. |
| **Define Workspace Consumption Standards** | How consumers access reports vs where authors build them. | **Standard.** Separate build workspaces from consumption (apps/Viewer access), so consumers get a stable, curated experience. |

### Validate Power BI Integration

*Prove the end-to-end path from Gold to a governed, secure report.*

| Item | What it means in Fabric | How we control it |
|---|---|---|
| **Validate Semantic Model Integration** | Confirming Power BI reads the Gold model efficiently (Direct Lake, no data copy). | **Both.** Gold is Direct-Lake-ready; we validate reports read it live without import refresh. |
| **Validate Security Integration** | Confirming access rules (row/column security) carry through to reports. | **Platform.** Row- and column-level security applied at the lakehouse flows through to the model and report — one rule, enforced end-to-end. |
| **Validate Workspace Strategy** | Confirming the workspace/app layout works for real authoring and consumption. | **Standard.** Validate the build-vs-consume workspace split with a real report and audience before rolling out broadly. |

---

*Companion documents:* the **Bronze vs Mirroring** analysis (Oracle/SQL-MI ingestion trade-offs and
recommendation), the **private-OCI architecture diagrams** (framework and mirroring paths), and the
control-plane **engine + working guides** in the repository.
