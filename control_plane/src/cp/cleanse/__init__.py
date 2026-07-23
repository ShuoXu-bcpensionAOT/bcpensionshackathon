"""Cleansing registry. Add a cleanse function by dropping a module in this folder and
decorating it with @cleanse_fn("name") — no edits here. Applied on silver, config-driven."""
import json

CLEANSE_FUNCS = {}


def cleanse_fn(*names):
    """Register a cleanse function under one or more names. Signature: (df, cols:list, params:dict) -> df."""
    def deco(fn):
        for n in names:
            CLEANSE_FUNCS[n] = fn
        return fn
    return deco


def register_cleanse_function(name, fn):
    """Imperative registration (equivalent to @cleanse_fn)."""
    CLEANSE_FUNCS[name] = fn


def apply_cleansing(df, rules):
    """Apply active cleanse rules (list of dicts) in apply_order. Ignores unknown functions."""
    for r in sorted(rules, key=lambda x: (x.get("apply_order") or 0)):
        fn = CLEANSE_FUNCS.get(r.get("function"))
        if not fn:
            continue
        cols = [c.strip() for c in str(r.get("columns") or "").split(";") if c.strip()]
        params = json.loads(r["parameters_json"]) if r.get("parameters_json") else {}
        df = fn(df, cols, params)
    return df


# --8<-- strip (the bundler drops this; the flat cell includes every function file directly)
import importlib
import pkgutil

for _m in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{_m.name}")
# --8<-- endstrip
