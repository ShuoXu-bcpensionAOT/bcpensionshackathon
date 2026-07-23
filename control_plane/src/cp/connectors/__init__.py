"""Ingestion connector registry. Add a source by dropping a module in this folder and
decorating its function with @ingest_connector("name", ...) — you never edit this file or the
framework. A connector is fn(object_config: dict, user, password) -> raw Spark DataFrame.
object_config carries source_object + datasource fields (see workers.plan), plus the parsed
JSON columns connection_json (datasource-level) and source_options_json (per object)."""

INGEST_CONNECTORS = {}
# connectors that support metadata schema discovery (INFORMATION_SCHEMA over JDBC)
DISCOVERABLE_CONNECTORS = {"sqlserver", "oracle", "db2", "postgresql", "mysql", "jdbc"}


def ingest_connector(*names):
    """Register a connector under one or more names/aliases."""
    def deco(fn):
        for n in names:
            INGEST_CONNECTORS[n] = fn
        return fn
    return deco


def register_ingest_connector(name, fn):
    """Imperative registration (equivalent to @ingest_connector)."""
    INGEST_CONNECTORS[name] = fn


def resolve_connector(o):
    """The connector key for a source object: explicit `connector`, else `source_type`."""
    name = (o.get("connector") or o.get("source_type") or "sqlserver").lower()
    return {"sql": "sqlserver", "mssql": "sqlserver", "custom_jdbc": "sqlserver",
            "api": "rest_api", "statcan": "statcan_wds"}.get(name, name)


def run_connector(o, user, password):
    """Dispatch to the datasource's connector and return the raw extract DataFrame."""
    name = resolve_connector(o)
    fn = INGEST_CONNECTORS.get(name)
    if not fn:
        raise Exception(f"no ingest connector registered for '{name}' (object {o.get('object_id')})")
    return fn(o, user, password)


# --8<-- strip (the bundler drops this; the flat cell includes every connector file directly)
import importlib
import pkgutil

for _m in pkgutil.iter_modules(__path__):
    if _m.name != "base":
        importlib.import_module(f"{__name__}.{_m.name}")
# --8<-- endstrip
