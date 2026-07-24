"""Gold table operators — one strategy per gold table type (scd1 / scd2 / fact), mirroring the
MXData model/strategy pattern. Add a type by dropping a module here decorated with
@gold_strategy("name") — no edits here. The gold runner calls
    gold_merge(stage_df, gold_type, gold_table, keys, run_id)
and never needs to know how a type merges. A strategy is fn(gold_path, stage_df, keys) -> None."""
from pyspark.sql import functions as F

from ..runtime import tpath
from ..storage import read_path

GOLD_STRATEGIES = {}


def gold_strategy(*names):
    """Register a gold merge strategy under one or more type names."""
    def deco(fn):
        for n in names:
            GOLD_STRATEGIES[n] = fn
        return fn
    return deco


def register_gold_strategy(name, fn):
    """Imperative registration (equivalent to @gold_strategy)."""
    GOLD_STRATEGIES[name] = fn


def gold_merge(stage_df, gold_type, gold_table, keys, run_id):
    """Merge a staged DataFrame into its gold table using the strategy for `gold_type`.
    Stamps gold control columns, dispatches to the strategy, returns the gold row count."""
    fn = GOLD_STRATEGIES.get(gold_type)
    if not fn:
        raise Exception(f"no gold strategy registered for '{gold_type}' (table {gold_table})")
    stage = (stage_df.withColumn("_gold_run_id", F.lit(run_id))
                     .withColumn("_gold_updated_at", F.current_timestamp()))
    path = tpath("gold", gold_table)
    fn(path, stage, keys)
    return read_path(path).count()


# --8<-- strip (the bundler drops this; the flat cell includes every strategy file directly)
import importlib
import pkgutil

for _m in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{_m.name}")
# --8<-- endstrip
