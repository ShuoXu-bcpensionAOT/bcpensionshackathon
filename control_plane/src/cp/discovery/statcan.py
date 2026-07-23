"""StatCan WDS object discovery — one object per table_id declared at the datasource."""
import json

from ..connectors.base import _conn
from . import discoverer


@discoverer("statcan_wds")
def _discover_statcan(ds):
    """Materialize one object per table_id declared at the datasource (connection_json.tables),
    seeded with GENERIC http params (the same _ic_http serves it — StatCan is just a zip_csv
    profile). The user then adds filters/select and activates."""
    out = []
    for t in _conn(ds).get("tables", []):
        out.append({"source_schema": None, "source_table": t.get("name") or str(t["table_id"]),
                    "key_columns_json": json.dumps(["REF_DATE", "VECTOR"]),
                    "source_options_json": json.dumps({
                        "url": "https://www150.statcan.gc.ca/t1/wds/rest/"
                               "getFullTableDownloadCSV/{table_id}/{language}",
                        "params": {"table_id": str(t["table_id"]), "language": t.get("language", "en")},
                        "response": {"type": "zip_csv", "url_field": "object", "exclude": "_Meta"}})})
    return out
