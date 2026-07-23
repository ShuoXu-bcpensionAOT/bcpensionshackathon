"""Pure naming/identifier helpers — no Fabric/Spark dependency, unit-testable off-cluster."""
import re
from datetime import datetime, timezone


def now_ts():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def snake(name):
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", str(name))
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower()


def _norm_ident(s):
    """Lowercase, non-alphanumeric -> _, collapse/strip underscores."""
    return re.sub(r"_+", "_", re.sub(r"[^0-9A-Za-z]+", "_", str(s or "").strip().lower())).strip("_")


def landed_table(o):
    """The landed (schema, table) for a source object on a SCHEMA-ENABLED lakehouse:
        schema = datasource (source_name)
        table  = {source_schema|dbo}_{source_table}[_{suffix}]
    e.g. source 'Stats Can', schema null, table 'labour_force', suffix 'bc'
         -> ('stats_can', 'dbo_labour_force_bc')  ->  stats_can.dbo_labour_force_bc."""
    schema = _norm_ident(o.get("source_name")) or "dbo"
    parts = [(o.get("source_schema") or "dbo"), o.get("source_table")]
    name = "_".join(_norm_ident(p) for p in parts if p)
    if o.get("suffix"):
        name = f"{name}_{_norm_ident(o['suffix'])}"
    return schema, name
