# BCPensions Hackathon — Fabric Items

Fabric-packaged item definitions for the **Hackathon-DEV** workspace, exported
directly from the live workspace. This repo holds only what deploys to Fabric —
the full MXData accelerator framework lives separately.

## Contents

```
workspace/
├── bronze.Lakehouse/      # medallion lakehouses (shell items)
├── silver.Lakehouse/
├── gold.Lakehouse/
├── metadata.Lakehouse/
└── <name>.Notebook/       # imported ELT notebooks (real content)
```

Each folder is a [fabric-cicd](https://microsoft.github.io/fabric-cicd/)-compatible
item definition (`.platform` + content), so the workspace can be re-deployed
from git.

## Status

- ✅ Lakehouses and notebooks are live in Hackathon-DEV.
- ⚠️ Notebooks are the original **Databricks** sources (use `dbutils`, expect the
  `mxdataspark` wheel). They are visible/readable in Fabric but are **not yet
  Fabric-native runnable**. Making them run in Fabric (dbutils→notebookutils,
  package the wheel into a Fabric Environment) is a planned follow-up once the
  content is reviewed and stable.

## Re-deploy

Auth via Azure CLI (handles MFA):

```bash
az login --tenant <tenant-id> --scope https://api.fabric.microsoft.com/.default --allow-no-subscriptions
python deploy.py   # or the scripts in the BCPensions deploy project
```
