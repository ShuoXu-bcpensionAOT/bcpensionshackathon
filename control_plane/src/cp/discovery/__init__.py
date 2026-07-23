"""Object-discovery registry. Add a discoverer by dropping a module here and decorating it with
@discoverer("connector_name") — no edits here. A discoverer is fn(datasource_config: dict) ->
list of candidate object dicts (source_schema, source_table, key_columns_json,
source_options_json; any may be None). The metadata step materializes these as source_object
rows with is_active=0."""
from ..connectors import resolve_connector

DISCOVERERS = {}


def discoverer(*names):
    """Register a discoverer under one or more connector names."""
    def deco(fn):
        for n in names:
            DISCOVERERS[n] = fn
        return fn
    return deco


def register_discoverer(name, fn):
    """Imperative registration (equivalent to @discoverer)."""
    DISCOVERERS[name] = fn


def discover_objects(ds):
    """Return candidate objects for a datasource via its registered discoverer (or None)."""
    fn = DISCOVERERS.get(resolve_connector(ds))
    return fn(ds) if fn else None


# --8<-- strip (the bundler drops this; the flat cell includes every discoverer file directly)
import importlib
import pkgutil

for _m in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{_m.name}")
# --8<-- endstrip
