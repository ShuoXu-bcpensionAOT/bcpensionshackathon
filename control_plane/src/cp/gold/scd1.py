"""Type-1 dimension: upsert by business key — the latest staged row wins, no history."""
from . import gold_strategy
from ..transform import merge_upsert


@gold_strategy("scd1")
def scd1(path, stage, keys):
    merge_upsert(path, stage, keys)
