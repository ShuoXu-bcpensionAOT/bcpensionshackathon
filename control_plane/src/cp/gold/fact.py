"""Fact table: merge by key — idempotent reload (re-running restates matched rows, inserts new)."""
from . import gold_strategy
from ..transform import merge_upsert


@gold_strategy("fact")
def fact(path, stage, keys):
    merge_upsert(path, stage, keys)
