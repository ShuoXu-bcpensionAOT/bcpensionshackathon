# Fabric Control Plane

A metadata-driven lakehouse control plane running as **Fabric Spark notebooks** in
the Hackathon-DEV workspace. Config-as-code → control tables → bronze → silver →
gold, with run audit, watermark incrementals, data-quality quarantine, schema-drift
tracking, and a dependency-ordered gold star schema (SCD1 / SCD2 / fact).

## Layout

```
control_plane/
├── config/            config-as-code (YAML) → control tables
│   ├── datasource.yml
│   ├── source_object.yml      # objects, load_type, keys, watermark
│   ├── dq_rule.yml            # data-quality rules
│   └── gold_model.yml         # model + gold_object + gold_dependency
├── notebooks/         Fabric notebook sources (deployed to the workspace)
│   ├── cp_framework.py        # shared helpers  (workspace: notebook/utility/)
│   ├── cp_01_setup.py
│   ├── cp_02_ingest_bronze.py
│   ├── cp_03_build_silver.py
│   ├── cp_04_build_gold.py
│   └── cp_09_orchestrate.py
└── deploy/            local tooling (deploy + run via Fabric REST API)
    ├── cp_common.py           # GUID-based OneLake paths, tokens
    ├── cp_config.py           # load YAML -> control tables
    ├── fabric_nb.py           # build/deploy/run notebooks
    ├── cp_deploy.py           # deploy + run control-plane notebooks
    └── reorg.py               # workspace folder organization
```

## Flow

1. **cp_config** (local) — load `config/*.yml` into control tables in the `metadata` lakehouse.
2. **cp_01_setup** — start an ingestion run; confirm control tables.
3. **cp_02_ingest_bronze** — JDBC-extract active source objects → bronze (full = overwrite,
   incremental = append rows past the stored watermark). Excludes complex SQL types.
4. **cp_03_build_silver** — snake_case, dedupe by business key (latest wins), row-hash,
   DQ rules (error-severity failures quarantined), schema-drift logging.
5. **cp_04_build_gold** — topo-sort `gold_dependency`; build dims before fact.
   `dim_product` (SCD1), `dim_customer` (SCD2), `fact_sales_order` (fact, RI-joined to dims).
6. **cp_09_orchestrate** — chains 01→02→03→04 (`notebookutils.notebook.run`).

## Control tables (metadata lakehouse)

`datasource`, `source_object`, `source_column`, `dq_rule`, `model`, `gold_object`,
`gold_dependency`, `ingestion_run`, `object_load_run`, `watermark_state`,
`schema_drift_event`, `dq_result`.

## Run

```bash
az login --tenant <tenant> --scope https://api.fabric.microsoft.com/.default --allow-no-subscriptions
cd control_plane/deploy
python cp_config.py                                   # load config-as-code
python cp_deploy.py deploy                             # deploy all notebooks
python cp_deploy.py run cp_09_orchestrate run_id=r1    # run the full pipeline
```

Source DB credentials are read from a local `.env` (never committed) and passed to
notebooks as run-time parameters — never stored in Delta.

## Notes

- OneLake is addressed by **GUID** (workspace + lakehouse); workspace-name paths proved
  unreliable.
- Gold stage builders are currently inline in `cp_04_build_gold`. Splitting them into
  per-object silver→stage "source query" notebooks (Migrato `04/05/06_stage_*` style) is a
  planned follow-up.
