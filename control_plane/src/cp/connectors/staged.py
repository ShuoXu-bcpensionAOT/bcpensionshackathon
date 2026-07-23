"""On-prem / staged connector — reads a table an upstream Copy activity already landed in
OneLake (for on-prem sources Spark can't reach directly)."""
from ..naming import _norm_ident
from ..runtime import tpath
from ..storage import delta_exists, read_path
from . import ingest_connector
from .base import _opts


@ingest_connector("onprem", "staged")
def _ic_staged(o, user, password):
    """Read a table an upstream Copy activity already landed in OneLake — for **on-prem** sources
    that Spark can't reach directly. `cp_pl_onprem` copies on-prem->bronze staging via the gateway;
    this connector reads that staged Delta so it flows through the normal bronze pipeline (control
    columns, select, schema naming) into the SAME silver/gold. source_options_json:
    {staging_lakehouse (default 'bronze'), staging_schema (default 'staging'), staging_table}."""
    opts = _opts(o)
    lh = opts.get("staging_lakehouse", "bronze")
    schema = opts.get("staging_schema", "staging")
    table = opts.get("staging_table") or _norm_ident(
        "_".join(x for x in [o.get("source_schema"), o.get("source_table")] if x))
    p = tpath(lh, table, schema)
    if not delta_exists(p):
        raise Exception(f"staged table not found: {lh}.{schema}.{table} "
                        f"(the cp_pl_onprem Copy activity must land it first)")
    return read_path(p)
