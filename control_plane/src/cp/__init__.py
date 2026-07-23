"""cp — the Fabric control-plane engine, as a modular package.

Kept intentionally free of heavy imports so the pure modules (`cp.naming`, `cp.dag`) import
off-cluster for unit tests. On Fabric, import what you need explicitly, e.g.:

    from cp import workers                      # workers.bronze(...), workers.silver(...)
    from cp.connectors import run_connector
    from cp.gold import build_stage_and_gold

For the deployed notebooks this whole package is bundled into the single `cp_framework`
cell by `deploy/cp_bundle.py`, so `%run cp_framework` exposes every symbol flat (unchanged
runtime). The same package also builds a wheel (`pyproject.toml`).
"""
__version__ = "0.1.0"
