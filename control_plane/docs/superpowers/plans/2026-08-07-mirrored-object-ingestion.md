# Mirrored-Object Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the control plane ingest Oracle tables that have no timestamp column via Fabric Mirroring, keeping silver's current-state contract and adding a per-object history sibling, without changing gold.

**Architecture:** The mirrored table *is* bronze — surfaced in the bronze lakehouse by a OneLake shortcut so `silver` reads it unchanged. Silver keeps one row per key; a new `silver.<table>_history` carries versions, written by one mechanism (compare an incoming set at a pinned Delta version against current, version what differs) regardless of whether the incoming set came from a mirror or a bronze snapshot. Change-level fidelity via Delta change data feed is opt-in and validated; the default is observed-at-load-time.

**Tech Stack:** Python 3.10+, PySpark + Delta Lake on Fabric, Fabric REST API (`requests`), T-SQL config database, pytest for off-cluster tests.

## Global Constraints

- **Decisions are fixed by ADRs 0001–0008 in `control_plane/docs/adr/`.** Read them before starting. Do not re-litigate; if a task appears to contradict one, stop and ask.
- **Domain language is fixed by `control_plane/CONTEXT.md`.** Use its terms exactly: *bronze*, *silver*, *silver history*, *history fidelity*, *seed version*, *gap*, *reseed*, *degraded object*, *stage*, *source query*.
- **The engine is a package that gets bundled.** Anything added under `src/cp/` must be re-bundled with `python deploy/cp_bundle.py`, and `notebooks/cp_framework.py` committed alongside — `tests/test_bundle.py::test_generated_file_in_sync` fails otherwise.
- **Off-cluster tests must not import pyspark, delta, notebookutils or requests.** `tests/` runs without a Fabric runtime. Put logic that needs testing in pure functions; keep Spark code as thin callers.
- **Run tests with** `python -m pytest tests/ -v` from `control_plane/`. `pyproject.toml` puts `src` and `deploy` on the path.
- **Additive migrations only** on the config database. `cp_sqldb.py` adds columns to existing tables; never drop or retype.
- **No secrets in config or git.** Connections are referenced by name or per-environment GUID resolved from the variable library.
- **Commit after every task** with a `feat:`/`fix:`/`test:` prefix.

---

## File Structure

**Created**
- `src/cp/history.py` — pure helpers for history: table naming, hash column selection, fidelity validation, seed/version decisions.
- `src/cp/mirror.py` — pure helpers for mirrored objects: identification, OneLake path building, mirrored-database definition (`mirroring.json`) construction.
- `deploy/cp_mirror.py` — provisioning: create/update a mirrored database, enable CDF and retention, start mirroring, ensure the bronze shortcut. Network I/O only; all shape decisions come from `src/cp/mirror.py`.
- `tests/test_history.py`, `tests/test_mirror.py` — off-cluster tests for the two pure modules.

**Modified**
- `src/cp/transform.py` — add `hash_columns()`; `row_hash()` accepts an explicit column list.
- `src/cp/gold/__init__.py:35-37` — replace silent `dropDuplicates` with a duplicate-key failure.
- `src/cp/gold/scd2.py:14-17` — honour a stage-supplied effective date.
- `src/cp/workers/bronze.py:20-22` — no-op for mirrored objects.
- `src/cp/workers/silver.py` — mirrored branch: read the pinned mirror version, soft-delete by key anti-join.
- `src/cp/workers/plan.py:12-19` — planner returns the new config columns and supports `plan_type="history"`.
- `deploy/cp_sqldb.py:21-33,70` — new config columns.

---

### Task 1: Change detection over common columns

Implements ADR-0006. Everything downstream depends on this, so it goes first.

**Files:**
- Modify: `src/cp/transform.py:9-18`
- Test: `tests/test_pure.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `hash_columns(stored_cols: list[str], incoming_cols: list[str]) -> list[str]` — sorted business columns present on both sides. `row_hash(df, cols=None, out="_row_hash")` unchanged in signature; callers now pass `cols=`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pure.py`:

```python
from cp.transform import hash_columns


def test_hash_columns_excludes_control_and_underscore_columns():
    assert hash_columns(["id", "name", "_row_hash"],
                        ["id", "name", "_silver_run_id"]) == ["id", "name"]


def test_hash_columns_added_column_does_not_change_the_set():
    # a new source column must not make every existing record look changed
    before = hash_columns(["id", "name"], ["id", "name"])
    after = hash_columns(["id", "name"], ["id", "name", "email"])
    assert before == after == ["id", "name"]


def test_hash_columns_dropped_column_narrows_the_set():
    assert hash_columns(["id", "name", "email"], ["id", "name"]) == ["id", "name"]


def test_hash_columns_is_sorted_and_order_independent():
    assert hash_columns(["b", "a"], ["a", "b"]) == ["a", "b"]


def test_hash_columns_no_overlap_is_empty():
    assert hash_columns(["a"], ["b"]) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_pure.py -k hash_columns -v`
Expected: FAIL — `ImportError: cannot import name 'hash_columns' from 'cp.transform'`

- [ ] **Step 3: Implement `hash_columns`**

In `src/cp/transform.py`, directly after `business_cols`:

```python
def hash_columns(stored_cols, incoming_cols):
    """Columns that decide whether a record changed: the business columns present on BOTH sides.

    Schema drift is routine under mirroring (columns are added, dropped and renamed at source).
    Hashing whatever happens to be present would make every schema change look like a change to
    every record — closing and reopening every dimension version and writing a spurious history
    version for every row. Comparing only the common columns keeps a schema change a schema
    change. See ADR-0006."""
    def biz(cols):
        return {c for c in cols if c not in CONTROL_COLS and not c.startswith("_")}
    return sorted(biz(stored_cols) & biz(incoming_cols))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_pure.py -k hash_columns -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Re-bundle and run the whole suite**

Run: `python deploy/cp_bundle.py && python -m pytest tests/ -v`
Expected: all PASS, including `test_generated_file_in_sync`

- [ ] **Step 6: Commit**

```bash
git add src/cp/transform.py tests/test_pure.py notebooks/cp_framework.py
git commit -m "feat: decide record change over columns common to both versions (ADR-0006)"
```

---

### Task 2: Gold fails on duplicate stage keys

Implements ADR-0008. Fixes a pre-existing bug where `gold_merge` silently kept an arbitrary row.

**Files:**
- Modify: `src/cp/gold/__init__.py:30-40`
- Test: `tests/test_pure.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `duplicate_key_message(dupes: list[dict], keys: list[str], gold_table: str) -> str` in `src/cp/gold/__init__.py`. `dupes` is a list of dicts each holding the key columns plus `count`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pure.py`:

```python
from cp.gold import duplicate_key_message


def test_duplicate_key_message_names_the_offending_keys():
    msg = duplicate_key_message(
        [{"member_key": 4471, "count": 3}, {"member_key": 88, "count": 2}],
        ["member_key"], "dim_member")
    assert "dim_member" in msg
    assert "member_key=4471 x3" in msg
    assert "member_key=88 x2" in msg


def test_duplicate_key_message_handles_composite_keys():
    msg = duplicate_key_message([{"a": 1, "b": "x", "count": 2}], ["a", "b"], "dim_thing")
    assert "a=1, b=x x2" in msg
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_pure.py -k duplicate_key -v`
Expected: FAIL — `ImportError: cannot import name 'duplicate_key_message'`

- [ ] **Step 3: Implement the message builder and the check**

In `src/cp/gold/__init__.py`, add before `gold_merge`:

```python
def duplicate_key_message(dupes, keys, gold_table):
    """Human-actionable duplicate-key error. Naming the offending keys matters: an unqualified
    'duplicate keys' on a large stage says there is a problem but not where."""
    shown = "; ".join(", ".join(f"{k}={d[k]}" for k in keys) + f" x{d['count']}" for d in dupes)
    return (f"stage for '{gold_table}' has more than one row per key {keys}: {shown}. "
            "The grain is wrong: either the key is under-specified, or the source query is "
            "fanning out. Fix the source query or widen the key — the runner will not choose "
            "a row for you (ADR-0008).")
```

Then replace the dedup in `gold_merge`:

```python
    if keys:                                              # a stage holds exactly one row per key
        dupes = (stage.groupBy(*keys).count().where(F.col("count") > 1)
                      .orderBy(F.col("count").desc()).limit(10).collect())
        if dupes:
            raise Exception(duplicate_key_message([r.asDict() for r in dupes], keys, gold_table))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_pure.py -k duplicate_key -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Re-bundle and run the whole suite**

Run: `python deploy/cp_bundle.py && python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/cp/gold/__init__.py tests/test_pure.py notebooks/cp_framework.py
git commit -m "fix: fail on duplicate stage keys instead of silently discarding rows (ADR-0008)"
```

---

### Task 3: Gold honours a stage-supplied effective date

Implements the second half of ADR-0008. Without this a type-2 version records when gold ran, never when the fact was true.

**Files:**
- Modify: `src/cp/gold/scd2.py:13-17`
- Test: `tests/test_pure.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `effective_start_column(stage_cols: list[str]) -> str | None` in `src/cp/gold/scd2.py` — returns `"_effective_start_ts"` when the stage already supplies it, else `None`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pure.py`:

```python
from cp.gold.scd2 import effective_start_column


def test_effective_start_column_uses_stage_value_when_present():
    assert effective_start_column(["id", "name", "_effective_start_ts"]) == "_effective_start_ts"


def test_effective_start_column_absent_means_stamp_now():
    assert effective_start_column(["id", "name"]) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_pure.py -k effective_start -v`
Expected: FAIL — `ImportError: cannot import name 'effective_start_column'`

- [ ] **Step 3: Implement**

In `src/cp/gold/scd2.py`, add above the strategy:

```python
def effective_start_column(stage_cols):
    """The stage may carry its own effective date — e.g. a source query that selected a
    point-in-time slice of silver history. Honour it rather than stamping the run time, so a
    dimension version records when the fact was true and not when gold noticed (ADR-0008)."""
    return "_effective_start_ts" if "_effective_start_ts" in stage_cols else None
```

Then in `scd2`, replace the first two statements. This also applies ADR-0006 to gold: without it,
adding a column to a source query re-versions every row in the dimension.

```python
@gold_strategy("scd2")
def scd2(path, stage, keys):
    # Compare over the columns the dimension and the stage share, so a projection change is not
    # mistaken for a change to every record (ADR-0006).
    stored_cols = read_path(path).columns if delta_exists(path) else stage.columns
    cols = hash_columns(stored_cols, stage.columns)
    stage = row_hash(stage, cols)
    start = effective_start_column(stage.columns)
    incoming = (stage.withColumn("_effective_start_ts",
                                 F.col(start) if start else F.current_timestamp())
                     .withColumn("_effective_end_ts", F.lit(None).cast("timestamp"))
                     .withColumn("_is_current", F.lit(True)))
```

Add the import at the top of `src/cp/gold/scd2.py`:

```python
from ..transform import hash_columns, row_hash
```

Then — and this is the part that makes ADR-0006 actually work — **recompute the stored side's
hash over the same columns** rather than trusting the `_row_hash` already on the dimension. That
stored value was computed over whatever columns were in force when the row was written; comparing
it against a hash over the current intersection would report every row as changed the first time
the projection moves, which is precisely the failure this ADR exists to prevent. Replace the
`cur` line:

```python
    # re-hash the stored rows over `cols` so both sides are hashed over the SAME column set
    cur = row_hash(tgt.toDF().where(F.col("_is_current")), cols)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_pure.py -k effective_start -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Re-bundle and run the whole suite**

Run: `python deploy/cp_bundle.py && python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/cp/gold/scd2.py tests/test_pure.py notebooks/cp_framework.py
git commit -m "feat: scd2 honours a stage-supplied effective date (ADR-0008)"
```

---

### Task 4: Config columns for mirrored objects and history

Implements the config shape from ADR-0003 and ADR-0005.

**Files:**
- Modify: `deploy/cp_sqldb.py:21-33` (DDL), `deploy/cp_sqldb.py:70` (promotion column list)
- Modify: `src/cp/workers/plan.py:12-19`
- Test: `tests/test_pure.py`

**Interfaces:**
- Consumes: nothing.
- Produces: config columns `source_object.track_history BIT`, `source_object.history_fidelity NVARCHAR(20)` (`observed` | `change`), `datasource.mirror_json NVARCHAR(MAX)`. The planner's `objects` rows gain `track_history`, `history_fidelity`, `mirror_json`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pure.py`:

```python
def test_source_object_ddl_has_history_columns():
    import cp_sqldb
    ddl = dict(cp_sqldb.TABLES)["source_object"]
    assert "track_history" in ddl
    assert "history_fidelity" in ddl


def test_datasource_ddl_has_mirror_json():
    import cp_sqldb
    assert "mirror_json" in dict(cp_sqldb.TABLES)["datasource"]


def test_promotion_columns_include_the_new_config():
    import cp_sqldb
    cols = cp_sqldb.COLUMNS
    assert "track_history" in cols["source_object"]
    assert "history_fidelity" in cols["source_object"]
    assert "mirror_json" in cols["datasource"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_pure.py -k "ddl or promotion_columns" -v`
Expected: FAIL — `AssertionError` (or `AttributeError` if `TABLES`/`COLUMNS` are named differently; read `deploy/cp_sqldb.py` and use the real names, adjusting the test to match)

- [ ] **Step 3: Add the columns**

In `deploy/cp_sqldb.py`, extend the `datasource` DDL with `mirror_json NVARCHAR(MAX)` and the `source_object` DDL with `track_history BIT, history_fidelity NVARCHAR(20)`. Add the same names to the promotion column lists so `cp_config` / `cp_export_config` round-trip them. Follow the file's existing additive-migration mechanism — do not rewrite the tables.

In `src/cp/workers/plan.py`, add the columns to the `objects` query:

```python
            "SELECT o.object_id,o.source_schema,o.source_table,o.target_name,o.load_type,"
            "o.key_columns_json,o.watermark_column,o.source_options_json,o.suffix,"
            "o.track_history,o.history_fidelity,"
            "d.database_name,d.source_name,d.source_type,d.connector,d.connection_json,"
            "d.mirror_json,d.secret_name "
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_pure.py -k "ddl or promotion_columns" -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Re-bundle and run the whole suite**

Run: `python deploy/cp_bundle.py && python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add deploy/cp_sqldb.py src/cp/workers/plan.py tests/test_pure.py notebooks/cp_framework.py
git commit -m "feat: config columns for mirrored objects and history fidelity (ADR-0003, ADR-0005)"
```

---

### Task 5: Mirror identity, paths and history naming

Pure helpers every later task uses. No Spark, no network.

**Files:**
- Create: `src/cp/mirror.py`, `src/cp/history.py`
- Create: `tests/test_mirror.py`, `tests/test_history.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `mirror.is_mirrored(o: dict) -> bool`
  - `mirror.mirror_item_id(o: dict) -> str | None` — from `mirror_json.item_id`
  - `mirror.onelake_path(ws_id: str, item_id: str, schema: str, table: str) -> str`
  - `history.history_table(schema: str, table: str) -> tuple[str, str]`
  - `history.fidelity(o: dict) -> str` — `"observed"` (default) or `"change"`
  - `history.tracks_history(o: dict) -> bool` — defaults **on** for mirrored, **off** otherwise

- [ ] **Step 1: Write the failing tests**

`tests/test_mirror.py`:

```python
"""Off-cluster tests for mirrored-object helpers (no Fabric/Spark needed)."""
import json

from cp.mirror import is_mirrored, mirror_item_id, onelake_path


def test_is_mirrored_reads_the_connector():
    assert is_mirrored({"connector": "mirror"}) is True
    assert is_mirrored({"connector": "MIRROR"}) is True
    assert is_mirrored({"connector": "sqlserver"}) is False
    assert is_mirrored({}) is False


def test_mirror_item_id_from_mirror_json():
    o = {"connector": "mirror", "mirror_json": json.dumps({"item_id": "abc-123"})}
    assert mirror_item_id(o) == "abc-123"


def test_mirror_item_id_absent_or_unparseable_is_none():
    assert mirror_item_id({"connector": "mirror"}) is None
    assert mirror_item_id({"connector": "mirror", "mirror_json": "not json"}) is None


def test_onelake_path_targets_the_item_not_a_lakehouse():
    p = onelake_path("WS", "ITEM", "dbo", "members")
    assert p == "abfss://WS@onelake.dfs.fabric.microsoft.com/ITEM/Tables/dbo/members"
```

`tests/test_history.py`:

```python
"""Off-cluster tests for silver-history helpers (no Fabric/Spark needed)."""
from cp.history import history_table, fidelity, tracks_history


def test_history_table_is_a_sibling_of_the_silver_table():
    assert history_table("oracle", "dbo_members") == ("oracle", "dbo_members_history")


def test_fidelity_defaults_to_observed():
    assert fidelity({}) == "observed"
    assert fidelity({"history_fidelity": None}) == "observed"
    assert fidelity({"history_fidelity": "change"}) == "change"
    assert fidelity({"history_fidelity": "CHANGE"}) == "change"


def test_tracks_history_defaults_on_for_mirrored_off_otherwise():
    assert tracks_history({"connector": "mirror"}) is True
    assert tracks_history({"connector": "sqlserver"}) is False


def test_tracks_history_explicit_flag_wins():
    assert tracks_history({"connector": "mirror", "track_history": False}) is False
    assert tracks_history({"connector": "sqlserver", "track_history": True}) is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_mirror.py tests/test_history.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'cp.mirror'`

- [ ] **Step 3: Implement both modules**

`src/cp/mirror.py`:

```python
"""Mirrored-object helpers. A mirrored object's bronze IS the Fabric mirrored database table —
a workspace item, not a lakehouse table — so these resolve identity and OneLake location.
Pure: no Spark, no network. See ADR-0001."""
import json


def is_mirrored(o):
    """True when the object is ingested by Fabric Mirroring rather than a control-plane connector."""
    return str(o.get("connector") or "").lower() == "mirror"


def _mirror_cfg(o):
    try:
        return json.loads(o.get("mirror_json") or "{}") or {}
    except (ValueError, TypeError):
        return {}


def mirror_item_id(o):
    """The mirrored database's Fabric item id, recorded at provisioning time."""
    return _mirror_cfg(o).get("item_id")


def onelake_path(ws_id, item_id, schema, table):
    """A table inside any OneLake item, addressed by GUID. Reading another item's path directly
    needs no shortcut — `runtime.read_config` already does this for the config SQL database."""
    return (f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/"
            f"{item_id}/Tables/{schema}/{table}")
```

`src/cp/history.py`:

```python
"""Silver-history helpers. History is a sibling of a silver table carrying its versions; silver
itself keeps one row per key. Pure: no Spark, no network. See ADR-0001, ADR-0005."""
from .mirror import is_mirrored

OBSERVED, CHANGE = "observed", "change"


def history_table(schema, table):
    """The history sibling for a silver table: same schema, `_history` suffix."""
    return (schema, f"{table}_history")


def fidelity(o):
    """How completely history represents the past. `observed` (a version per load) is the
    default; `change` (a version per source change) needs the Delta change data feed."""
    return CHANGE if str(o.get("history_fidelity") or "").lower() == CHANGE else OBSERVED


def tracks_history(o):
    """Whether this object materialises history. Defaults ON for mirrored objects, because
    nothing else holds their past, and OFF for framework-ingested ones, whose bronze does."""
    flag = o.get("track_history")
    if flag is not None:
        return bool(flag)
    return is_mirrored(o)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_mirror.py tests/test_history.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Re-bundle and run the whole suite**

Run: `python deploy/cp_bundle.py && python -m pytest tests/ -v`
Expected: all PASS. If `test_bundle.py` complains that the new modules aren't in the bundle, add them to the bundler's module list following the pattern already there for `naming`/`dag`.

- [ ] **Step 6: Commit**

```bash
git add src/cp/mirror.py src/cp/history.py tests/test_mirror.py tests/test_history.py notebooks/cp_framework.py
git commit -m "feat: pure helpers for mirrored objects and silver history"
```

---

### Task 6: Mirrored-database definition and fidelity validation

Builds the `mirroring.json` payload from config, and enforces ADR-0005's rule that change fidelity requires a change-feed-enabled datasource.

**Files:**
- Modify: `src/cp/mirror.py`
- Test: `tests/test_mirror.py`

**Interfaces:**
- Consumes: `history.fidelity`, `mirror._mirror_cfg` from Task 5.
- Produces:
  - `mirror.mirroring_json(datasource: dict, objects: list[dict]) -> dict`
  - `mirror.validate_fidelity(datasource: dict, objects: list[dict]) -> None` — raises `ValueError` naming the offending objects.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_mirror.py`:

```python
import pytest

from cp.mirror import mirroring_json, validate_fidelity

DS = {"connector": "mirror", "database_name": "PENSIONS",
      "mirror_json": '{"connection_id": "CONN-1", "source_type": "Oracle", '
                     '"default_schema": "dbo", "retention_days": 7}'}


def test_mirroring_json_mounts_only_active_objects():
    objs = [{"source_schema": "dbo", "source_table": "members"},
            {"source_schema": "dbo", "source_table": "plans"}]
    d = mirroring_json(DS, objs)
    mounted = [(m["source"]["typeProperties"]["schemaName"],
                m["source"]["typeProperties"]["tableName"]) for m in d["properties"]["mountedTables"]]
    assert mounted == [("dbo", "members"), ("dbo", "plans")]


def test_mirroring_json_carries_connection_and_retention():
    d = mirroring_json(DS, [{"source_schema": "dbo", "source_table": "members"}])
    src = d["properties"]["source"]["typeProperties"]
    tgt = d["properties"]["target"]["typeProperties"]
    assert src["connection"] == "CONN-1"
    assert src["database"] == "PENSIONS"
    assert tgt["retentionInDays"] == 7          # explicit: the documented default is inconsistent
    assert tgt["format"] == "Delta"


def test_mirroring_json_enables_cdf_only_when_an_object_wants_change_fidelity():
    off = mirroring_json(DS, [{"source_schema": "dbo", "source_table": "members"}])
    assert off["properties"]["target"]["typeProperties"].get("enableDeltaChangeDataFeed") is not True

    on = mirroring_json(DS, [{"source_schema": "dbo", "source_table": "members",
                              "history_fidelity": "change"}])
    assert on["properties"]["target"]["typeProperties"]["enableDeltaChangeDataFeed"] is True


def test_validate_fidelity_rejects_change_without_an_enabled_datasource():
    ds = {"mirror_json": '{"cdf_enabled": false}'}
    objs = [{"object_id": "o1", "history_fidelity": "change"},
            {"object_id": "o2", "history_fidelity": "observed"}]
    with pytest.raises(ValueError) as e:
        validate_fidelity(ds, objs)
    assert "o1" in str(e.value)
    assert "o2" not in str(e.value)


def test_validate_fidelity_passes_when_datasource_is_enabled():
    ds = {"mirror_json": '{"cdf_enabled": true}'}
    validate_fidelity(ds, [{"object_id": "o1", "history_fidelity": "change"}])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_mirror.py -k "mirroring_json or validate_fidelity" -v`
Expected: FAIL — `ImportError: cannot import name 'mirroring_json'`

- [ ] **Step 3: Implement**

Append to `src/cp/mirror.py`:

```python
from .history import fidelity, CHANGE


def mirroring_json(datasource, objects):
    """The mirrored database item definition. `mountedTables` is generated from the active source
    objects, so what is replicated stays config-as-code rather than a portal click (ADR-0005)."""
    cfg = _mirror_cfg(datasource)
    target = {"defaultSchema": cfg.get("default_schema", "dbo"),
              "format": "Delta",
              # explicit: Microsoft documents conflicting defaults (1 day vs 7)
              "retentionInDays": int(cfg.get("retention_days", 7))}
    if any(fidelity(o) == CHANGE for o in objects):
        target["enableDeltaChangeDataFeed"] = True
    return {"properties": {
        "source": {"type": cfg.get("source_type", "Oracle"),
                   "typeProperties": {"connection": cfg.get("connection_id"),
                                      "database": datasource.get("database_name")}},
        "target": {"type": "MountedRelationalDatabase", "typeProperties": target},
        "mountedTables": [
            {"source": {"typeProperties": {"schemaName": o.get("source_schema") or "dbo",
                                           "tableName": o["source_table"]}}}
            for o in objects],
    }}


def validate_fidelity(datasource, objects):
    """Change fidelity needs the change feed, which is enabled per mirrored DATABASE — so one
    object asking for it bills every table in that database. Enabling it is a deliberate
    datasource decision; requesting it from a datasource that has not been enabled is an error,
    not something to switch on silently (ADR-0005)."""
    if _mirror_cfg(datasource).get("cdf_enabled"):
        return
    bad = [o.get("object_id") for o in objects if fidelity(o) == CHANGE]
    if bad:
        raise ValueError(
            f"objects {bad} request change fidelity but datasource "
            f"'{datasource.get('source_name')}' is not change-feed enabled. Enabling it affects "
            "every table in the mirrored database and is billed — set mirror_json.cdf_enabled "
            "deliberately, or use observed fidelity (ADR-0005).")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_mirror.py -v`
Expected: PASS (9 tests)

- [ ] **Step 5: Re-bundle and run the whole suite**

Run: `python deploy/cp_bundle.py && python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/cp/mirror.py tests/test_mirror.py notebooks/cp_framework.py
git commit -m "feat: build mirrored-database definition from config; gate change fidelity (ADR-0005)"
```

---

### Task 7: Provision the mirrored database and its bronze shortcut

Network I/O. All shape decisions come from Task 6, so this module stays thin.

**Files:**
- Create: `deploy/cp_mirror.py`
- Reference: `deploy/cp_bootstrap.py:292-310` (`ensure_shortcut`), `deploy/cp_auth.py` (token)

**Interfaces:**
- Consumes: `mirror.mirroring_json`, `mirror.validate_fidelity`.
- Produces: `ensure_mirrored_database(token, ws_id, datasource, objects) -> str` (item id); `ensure_bronze_shortcuts(token, ws_id, bronze_lh_id, item_id, objects) -> None`.

- [ ] **Step 1: Write the module**

Create `deploy/cp_mirror.py`:

```python
"""Provision a Fabric mirrored database from config: create it, set change feed and retention,
start replication, and shortcut its tables into the bronze lakehouse so silver reads one bronze
surface. Idempotent. Shape comes from cp.mirror; this file only talks to the API. See ADR-0001."""
import base64
import json
import sys
import time

import requests

from cp.mirror import mirroring_json, validate_fidelity

API = "https://api.fabric.microsoft.com/v1"


def _H(t):
    return {"Authorization": f"Bearer {t}", "Content-Type": "application/json"}


def _payload(defn):
    return base64.b64encode(json.dumps(defn).encode()).decode()


def _find(t, ws_id, name):
    r = requests.get(f"{API}/workspaces/{ws_id}/mirroredDatabases", headers=_H(t), timeout=60)
    r.raise_for_status()
    for i in r.json().get("value", []):
        if i["displayName"] == name:
            return i["id"]
    return None


def ensure_mirrored_database(t, ws_id, datasource, objects):
    """Create or update the mirrored database for a datasource. Returns its item id."""
    validate_fidelity(datasource, objects)
    name = f"MD_{datasource['source_name']}"
    defn = mirroring_json(datasource, objects)
    item_id = _find(t, ws_id, name)
    if item_id is None:
        r = requests.post(f"{API}/workspaces/{ws_id}/mirroredDatabases", headers=_H(t), timeout=120,
                          json={"displayName": name,
                                "definition": {"parts": [{"path": "mirroring.json",
                                                          "payload": _payload(defn),
                                                          "payloadType": "InlineBase64"}]}})
        if r.status_code not in (200, 201, 202):
            sys.exit(f"create mirrored database failed [{r.status_code}] {r.text[:300]}")
        item_id = r.json()["id"]
        print(f"  + mirrored database {name} {item_id}")
    else:
        r = requests.post(f"{API}/workspaces/{ws_id}/mirroredDatabases/{item_id}/updateDefinition",
                          headers=_H(t), timeout=120,
                          json={"definition": {"parts": [{"path": "mirroring.json",
                                                          "payload": _payload(defn),
                                                          "payloadType": "InlineBase64"}]}})
        if r.status_code not in (200, 202):
            print(f"  WARN updateDefinition [{r.status_code}] {r.text[:200]}")
        else:
            print(f"  = mirrored database {name} definition refreshed")
    _start(t, ws_id, item_id)
    return item_id


def _start(t, ws_id, item_id):
    """Start replication unless it is already running. Mirroring cannot be started while the
    item reports Initializing, so poll briefly rather than failing the deploy."""
    for _ in range(12):
        s = requests.post(f"{API}/workspaces/{ws_id}/mirroredDatabases/{item_id}/getMirroringStatus",
                          headers=_H(t), timeout=60)
        status = s.json().get("status") if s.status_code == 200 else "Unknown"
        if status == "Running":
            print("    mirroring already running")
            return
        if status != "Initializing":
            r = requests.post(f"{API}/workspaces/{ws_id}/mirroredDatabases/{item_id}/startMirroring",
                              headers=_H(t), timeout=60)
            print(f"    startMirroring [{r.status_code}] (was {status})")
            return
        time.sleep(10)
    print("    WARN still Initializing; start mirroring later")


def ensure_bronze_shortcuts(t, ws_id, bronze_lh_id, item_id, objects):
    """Point bronze at the mirrored tables. The shortcut API documents its OneLake targets as
    lakehouse/KQL/warehouse, but that list is stale — a shortcut to a non-lakehouse item is
    accepted (verified, ADR-0001)."""
    for o in objects:
        schema = (o.get("source_schema") or "dbo").lower()
        table = o["source_table"].lower()
        body = {"path": f"Tables/{schema}", "name": table,
                "target": {"oneLake": {"workspaceId": ws_id, "itemId": item_id,
                                       "path": f"Tables/{schema}/{table}"}}}
        r = requests.post(
            f"{API}/workspaces/{ws_id}/items/{bronze_lh_id}/shortcuts"
            "?shortcutConflictPolicy=CreateOrOverwrite",
            headers=_H(t), json=body, timeout=60)
        if r.status_code in (200, 201):
            print(f"    + shortcut bronze/{schema}.{table}")
        else:
            print(f"    WARN shortcut {schema}.{table} [{r.status_code}] {r.text[:160]}")
```

- [ ] **Step 2: Verify it imports and the payload round-trips**

Run:

```bash
python -c "
import json, base64, sys; sys.path[:0]=['src','deploy']
from cp_mirror import _payload
from cp.mirror import mirroring_json
ds={'source_name':'oracle','database_name':'PENSIONS','mirror_json':'{\"connection_id\":\"C\"}'}
d=mirroring_json(ds,[{'source_schema':'dbo','source_table':'members'}])
assert json.loads(base64.b64decode(_payload(d)))==d
print('payload round-trips')"
```

Expected: `payload round-trips`

- [ ] **Step 3: Run the whole suite**

Run: `python -m pytest tests/ -v`
Expected: all PASS (this module is deploy-side and not bundled into `cp_framework`)

- [ ] **Step 4: Commit**

```bash
git add deploy/cp_mirror.py
git commit -m "feat: provision mirrored databases and bronze shortcuts from config (ADR-0001)"
```

---

### Task 8: Bronze no-ops for mirrored objects

Implements ADR-0003. The mirrored table *is* bronze, so there is nothing to load — but the step still runs for the load group.

**Files:**
- Modify: `src/cp/workers/bronze.py:20-24`

**Interfaces:**
- Consumes: `mirror.is_mirrored`.
- Produces: nothing new.

- [ ] **Step 1: Implement the skip**

In `src/cp/workers/bronze.py`, import at the top:

```python
from ..mirror import is_mirrored
```

and make `_work` return immediately for mirrored objects, before any connector work:

```python
    def _work():
        oid, load_type = o["object_id"], o["load_type"]
        schema, table = landed_table(o)                        # schema-enabled: (schema, table)
        if is_mirrored(o):
            # A mirrored object's bronze IS the mirrored table, maintained by Fabric. The step
            # still runs for the load group; there is simply nothing for it to load (ADR-0003).
            log_object_run(run_id, oid, "bronze", "SKIPPED", details={"reason": "mirrored"})
            print(f"bronze {schema}.{table}: skipped (mirrored)")
            return
```

- [ ] **Step 2: Verify the skip is reachable without Spark**

Run:

```bash
python -c "
import sys; sys.path.insert(0,'src')
from cp.mirror import is_mirrored
o={'object_id':'x','load_type':'mirrored','connector':'mirror'}
assert is_mirrored(o); print('mirrored objects will skip bronze')"
```

Expected: `mirrored objects will skip bronze`

- [ ] **Step 3: Re-bundle and run the whole suite**

Run: `python deploy/cp_bundle.py && python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add src/cp/workers/bronze.py notebooks/cp_framework.py
git commit -m "feat: bronze skips mirrored objects (ADR-0003)"
```

---

### Task 9: Silver reads a mirrored object at a pinned version

Implements ADR-0001 (mirror is bronze) and Q19 (pin one version per run). Soft-delete parity comes from a key anti-join, not the change feed, so current state never depends on the fragile path.

**Files:**
- Modify: `src/cp/workers/silver.py:56-95`
- Modify: `src/cp/history.py` (add the pure decision helper)
- Test: `tests/test_history.py`

**Interfaces:**
- Consumes: `mirror.is_mirrored`, `mirror.mirror_item_id`, `mirror.onelake_path`.
- Produces: `history.should_skip_load(last_version: int | None, current_version: int) -> bool`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_history.py`:

```python
from cp.history import should_skip_load


def test_skip_when_the_mirror_has_not_advanced():
    assert should_skip_load(41, 41) is True


def test_do_not_skip_when_the_mirror_advanced():
    assert should_skip_load(41, 42) is False


def test_never_skip_the_first_run():
    assert should_skip_load(None, 0) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_history.py -k skip -v`
Expected: FAIL — `ImportError: cannot import name 'should_skip_load'`

- [ ] **Step 3: Implement the helper and the silver branch**

Append to `src/cp/history.py`:

```python
def should_skip_load(last_version, current_version):
    """A mirrored table's Delta version not advancing proves nothing changed — stronger evidence
    than a scan, and far cheaper. A skip is a valid observation, so it still advances the
    knowledge boundary (ADR-0007)."""
    return last_version is not None and int(last_version) == int(current_version)
```

In `src/cp/workers/silver.py`, import:

```python
from ..mirror import is_mirrored, mirror_item_id, onelake_path
from ..runtime import WS_ID
```

and replace the bronze-read block (currently `bp = tpath(...)` through `df = read_path(bp)`) with:

```python
        if is_mirrored(o):
            item_id = mirror_item_id(o)
            if not item_id:
                raise Exception(f"{oid}: mirrored object has no mirror_json.item_id — "
                                "run the mirror provisioning step first")
            src_schema = (o.get("source_schema") or "dbo").lower()
            bp = onelake_path(WS_ID, item_id, src_schema, o["source_table"].lower())
        else:
            bp = tpath("bronze", table, schema)
        if not delta_exists(bp):
            print(f"skip {schema}.{table}: no bronze")
            return
        df = read_path(bp)
```

Then guard the bronze-history logic, which only applies to append-only bronze. Immediately after `sdf` is built, replace the `latest_ts` block with:

```python
        if is_mirrored(o):
            # The mirrored table already IS current state: one row per key, no accumulated
            # snapshots. Deletes are established by comparing keys against silver, so current
            # state never depends on the change feed (ADR-0001).
            latest_ts, current_keys_df, max_ts = None, None, None
            if has_key and all(k in sdf.columns for k in keys):
                current_keys_df = sdf.select(*keys).distinct()
        else:
            latest_ts = sdf.agg(F.max("_bronze_ingest_ts")).first()[0] if is_snapshot else None
            current_keys_df, max_ts = None, None
            if soft_delete and all(k in sdf.columns for k in keys):
                max_ts = latest_ts
                current_keys_df = sdf.where(F.col("_bronze_ingest_ts") == F.lit(max_ts)) \
                                     .select(*keys).distinct()
            if has_key and all(k in sdf.columns for k in keys):
                w = Window.partitionBy(*keys).orderBy(F.col("_bronze_ingest_ts").desc())
                sdf = sdf.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
            elif is_snapshot and latest_ts is not None:
                sdf = sdf.where(F.col("_bronze_ingest_ts") == F.lit(latest_ts))
        sdf = sdf.drop("_bronze_ingest_ts") if "_bronze_ingest_ts" in sdf.columns else sdf
```

Leave the soft-delete flagging block below unchanged — it already keys off `current_keys_df`, so mirrored objects get `_is_deleted` parity for free. Use `F.current_timestamp()` in place of `F.lit(max_ts)` when `max_ts` is `None`.

- [ ] **Step 4: Wire the skip so an unchanged mirror costs a metadata read, not a scan**

Reading the whole mirrored table to discover nothing changed is the single most expensive thing
this design does. The Delta version not advancing proves nothing changed, so check it first.

Add to the imports in `src/cp/workers/silver.py`:

```python
from ..audit import get_watermark, update_watermark
from ..history import should_skip_load

MIRROR_WM = "{oid}#mirror_version"
```

Then immediately after `bp` is resolved for a mirrored object, before `read_path(bp)`:

```python
        mirror_version = None
        if is_mirrored(o):
            from delta.tables import DeltaTable
            from ..runtime import spark
            mirror_version = DeltaTable.forPath(spark, bp).history(1).select("version").first()[0]
            if should_skip_load(get_watermark(MIRROR_WM.format(oid=oid)), mirror_version):
                log_object_run(run_id, oid, "silver", "SKIPPED",
                               details={"reason": "mirror unchanged", "version": mirror_version})
                print(f"silver {schema}.{table}: skipped (mirror at v{mirror_version}, unchanged)")
                return
```

and record the version at the end of a successful run, next to the existing `log_object_run` call:

```python
        if mirror_version is not None:
            update_watermark(MIRROR_WM.format(oid=oid), mirror_version)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_history.py -v`
Expected: PASS (7 tests)

- [ ] **Step 6: Re-bundle and run the whole suite**

Run: `python deploy/cp_bundle.py && python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add src/cp/workers/silver.py src/cp/history.py tests/test_history.py notebooks/cp_framework.py
git commit -m "feat: silver reads mirrored objects and keeps soft-delete parity (ADR-0001)"
```

---

### Task 10: Write silver history

The core of the feature: seed, version, bound the change window, flag quality, reconcile.

**Files:**
- Modify: `src/cp/history.py`
- Create: `src/cp/workers/history.py`
- Test: `tests/test_history.py`

**Interfaces:**
- Consumes: `history.history_table`, `history.tracks_history`, `transform.hash_columns`.
- Produces: `workers.history.history(run_id, object_json=..., object=None, **kw)` — the notebook entrypoint, matching the shape of `workers.silver.silver`. Pure helper `history.version_origin(is_seed: bool) -> str`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_history.py`:

```python
from cp.history import version_origin, SEED, OBSERVED_ORIGIN


def test_version_origin_distinguishes_a_seed_from_an_observed_change():
    assert version_origin(True) == SEED
    assert version_origin(False) == OBSERVED_ORIGIN
    assert SEED != OBSERVED_ORIGIN
```

`OBSERVED_ORIGIN` is deliberately not the string `"change"` — `history.CHANGE` already means a
*fidelity*, and reusing the value for a *version origin* would make two different concepts
indistinguishable in the data.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_history.py -k version_origin -v`
Expected: FAIL — `ImportError: cannot import name 'version_origin'`

- [ ] **Step 3: Implement the helper**

Append to `src/cp/history.py`:

```python
SEED, OBSERVED_ORIGIN = "seed", "observed"


def version_origin(is_seed):
    """A seed version's start date is a KNOWLEDGE boundary, not a business date: it records when
    the platform began observing the record, not when the record came into existence. Marking it
    stops a reader concluding a member created in 2003 was created the day we onboarded (ADR-0007)."""
    return SEED if is_seed else OBSERVED_ORIGIN
```

- [ ] **Step 4: Implement the worker**

Create `src/cp/workers/history.py`:

```python
"""Silver-history entrypoint: version ONE object. Compares the incoming current set against the
history's current versions over the columns common to both, closes what changed, opens the new
version, and reconciles against silver. See ADR-0001, 0006, 0007."""
import json
import traceback

from pyspark.sql import functions as F

from ..naming import landed_table, now_ts
from ..runtime import tpath
from ..storage import delta_exists, read_path, write_path, files_put
from ..transform import hash_columns, row_hash
from ..audit import log_object_run, get_watermark, update_watermark
from ..history import history_table, tracks_history, version_origin

HISTORY_WM = "{oid}#history"          # distinct from the bronze watermark for the same object


def history(run_id="manual", object_json="{}", object=None, **kw):
    o = object if object is not None else json.loads(object_json or "{}")

    def _work():
        oid = o["object_id"]
        schema, table = landed_table(o)
        if not tracks_history(o):
            print(f"history {schema}.{table}: not tracked")
            return
        sp = tpath("silver", table, schema)
        if not delta_exists(sp):
            print(f"history {schema}.{table}: no silver yet")
            return
        keys = [k for k in json.loads(o.get("key_columns_json") or "[]")]
        hs, ht = history_table(schema, table)
        hp = tpath("silver", ht, hs)
        incoming = read_path(sp)
        # A concrete instant, not F.current_timestamp(). That is a lazy expression evaluated per
        # query, so the merge that closes a version and the append that opens the next one would
        # each evaluate it separately — leaving _valid_to and _valid_from disagreeing, and a
        # point-in-time query on that instant returning nothing or two rows.
        run_ts = now_ts()
        stamp = F.lit(run_ts).cast("timestamp")
        # The change is bounded to (previous successful run, now]. That lower bound is a run-level
        # fact, so it is read once here rather than carried per row (ADR-0007).
        prev = get_watermark(HISTORY_WM.format(oid=oid))
        after = F.lit(prev).cast("timestamp") if prev else F.lit(None).cast("timestamp")

        if not delta_exists(hp):
            # Seed: everything we can see becomes the first version, marked as a seed so its
            # start date is never read as a creation date.
            cols = hash_columns(incoming.columns, incoming.columns)
            seed = (row_hash(incoming, cols)
                    .withColumn("_valid_from", stamp)
                    .withColumn("_valid_to", F.lit(None).cast("timestamp"))
                    .withColumn("_is_current", F.lit(True))
                    .withColumn("_changed_after", after)
                    .withColumn("_version_origin", F.lit(version_origin(True)))
                    .withColumn("_hash_columns", F.lit(",".join(cols)))
                    .withColumn("_history_run_id", F.lit(run_id)))
            write_path(seed, hp, mode="overwrite")
            n = read_path(hp).count()
            log_object_run(run_id, oid, "history", "SUCCEEDED", target_count=n,
                           details={"origin": "seed"})
            print(f"history {schema}.{table}: seeded {n} versions")
            return

        hist = read_path(hp)
        cur = hist.where(F.col("_is_current"))
        cols = hash_columns(cur.columns, incoming.columns)
        inc = row_hash(incoming, cols).withColumnRenamed("_row_hash", "_inc_hash")
        # Re-hash the stored versions over the SAME `cols` rather than trusting their stored
        # _row_hash, which was computed over whatever columns existed when they were written.
        # Comparing across two different column sets would report every record as changed the
        # first time the schema moves — the exact failure ADR-0006 prevents.
        stored = row_hash(cur, cols).select(*keys, F.col("_row_hash").alias("_cur_hash"))
        joined = inc.join(stored, keys, "left")
        changed = joined.where(F.col("_cur_hash").isNull() | (F.col("_inc_hash") != F.col("_cur_hash")))
        # Materialise before the merge. `changed` derives from the same delta path the merge is
        # about to mutate; left lazy, Spark would recompute it afterwards against the post-merge
        # state. count() alone forces one action but does not retain the result.
        changed = changed.persist()
        n_changed = changed.count()
        if n_changed:
            ck = changed.select(*keys).distinct()
            from delta.tables import DeltaTable
            from ..runtime import spark
            cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
            (DeltaTable.forPath(spark, hp).alias("t").merge(ck.alias("s"), cond)
             .whenMatchedUpdate(condition="t._is_current",
                                set={"_is_current": F.lit(False), "_valid_to": stamp}).execute())
            new = (changed.drop("_cur_hash").withColumnRenamed("_inc_hash", "_row_hash")
                   .withColumn("_valid_from", stamp)
                   .withColumn("_valid_to", F.lit(None).cast("timestamp"))
                   .withColumn("_is_current", F.lit(True))
                   .withColumn("_changed_after", after)
                   .withColumn("_version_origin", F.lit(version_origin(False)))
                   .withColumn("_hash_columns", F.lit(",".join(cols)))
                   .withColumn("_history_run_id", F.lit(run_id)))
            write_path(new, hp, mode="append")
        changed.unpersist()

        # Reconcile: every key in silver must have a current, quality-passing history version and
        # vice versa. Both sides were read from the same pinned state, so this asserts rather than
        # tolerates (ADR-0001).
        final = read_path(hp)
        good = final.where(F.col("_is_current"))
        if "_dq_failed" in final.columns:
            good = good.where(~F.coalesce(F.col("_dq_failed"), F.lit(False)))
        missing = incoming.select(*keys).join(good.select(*keys), keys, "left_anti").count()
        extra = good.select(*keys).join(incoming.select(*keys), keys, "left_anti").count()
        if missing or extra:
            raise Exception(
                f"history/silver reconciliation failed for {schema}.{table}: "
                f"{missing} key(s) in silver with no current history version, "
                f"{extra} current history version(s) with no silver row (ADR-0001)")
        # A run that wrote nothing is still an observation: it proves nothing changed, so it
        # advances the knowledge boundary for the next run (ADR-0007).
        update_watermark(HISTORY_WM.format(oid=oid), now_ts())
        log_object_run(run_id, oid, "history", "SUCCEEDED", source_count=incoming.count(),
                       target_count=final.count(),
                       details={"versioned": n_changed, "hash_columns": len(cols)})
        print(f"history {schema}.{table}: {n_changed} new version(s)")

    try:
        _work()
    except Exception:
        files_put(f"_cp_err_history_{o.get('object_id', 'x')}_{run_id}.txt", traceback.format_exc())
        raise
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/ -v`
Expected: all PASS (the worker is Spark-side; its pure helpers are covered)

- [ ] **Step 6: Re-bundle**

Run: `python deploy/cp_bundle.py && python -m pytest tests/ -v`
Expected: all PASS. Add `workers/history.py` to the bundler alongside the other workers if `test_bundle.py` reports it missing.

- [ ] **Step 7: Commit**

```bash
git add src/cp/history.py src/cp/workers/history.py tests/test_history.py notebooks/cp_framework.py
git commit -m "feat: write silver history with bounded change windows and reconciliation (ADR-0001, 0006, 0007)"
```

---

### Task 11: Orchestration and end-to-end verification

Adds the `load_history` step and proves the whole path on Fabric. This is where the residual mirrored-database uncertainty dies.

**Files:**
- Modify: `src/cp/workers/plan.py` (support `plan_type="history"`)
- Create: `notebooks/history_worker.py`
- Modify: `deploy/manifest.yml`, `config/steps.yml`

**Interfaces:**
- Consumes: everything above.
- Produces: a `load_history` step runnable per load group.

- [ ] **Step 1: Add the planner branch**

In `src/cp/workers/plan.py`, inside `plan`, after the `objects` branch:

```python
    elif plan_type == "history":
        rows = config_query(
            "SELECT o.object_id,o.source_schema,o.source_table,o.target_name,o.load_type,"
            "o.key_columns_json,o.source_options_json,o.suffix,o.track_history,o.history_fidelity,"
            "d.source_name,d.connector,d.mirror_json "
            "FROM dbo.source_object o JOIN dbo.datasource d ON o.source_id=d.source_id "
            "WHERE o.is_active=1 AND o.processing_state='ACTIVE' AND d.load_group=? "
            "ORDER BY o.object_id", (lg,))
```

- [ ] **Step 2: Create the notebook shell**

Create `notebooks/history_worker.py` as a 3-cell shell matching `notebooks/silver_worker.py` exactly in structure — `%run cp_framework`, parameter cell (`run_id`, `object_json`), then:

```python
from cp.workers.history import history
history(run_id=run_id, object_json=object_json)
```

Register it in `deploy/manifest.yml` alongside `silver_worker`, and add a `load_history` row to `config/steps.yml` for each load group, ordered after `load_silver` and before `load_gold`.

- [ ] **Step 3: Re-bundle, test, deploy to DEV**

Run:

```bash
python deploy/cp_bundle.py && python -m pytest tests/ -v
python deploy/cp_bootstrap.py HackathonShuo DEV
```

Expected: tests PASS; bootstrap reports the new notebook and step deployed.

- [ ] **Step 4: Provision one mirrored object end to end**

Prerequisite (human, not code): a platform admin creates the Oracle connection in the workspace and records its GUID — the service principal cannot create connections in this tenant.

Then register a datasource with `connector='mirror'` and `mirror_json` carrying `connection_id`, `source_type`, `default_schema`, `retention_days`, `cdf_enabled: false`; register one `source_object` with `load_type='mirrored'`, real `key_columns_json`, `track_history` left null. Run:

```bash
python -c "
import sys; sys.path[:0]=['src','deploy']
import cp_auth, cp_mirror
t = cp_auth.get_token('https://api.fabric.microsoft.com')
ds = {'source_name':'oracle','database_name':'PENSIONS',
      'mirror_json':'{\"connection_id\":\"<GUID>\",\"source_type\":\"Oracle\",\"default_schema\":\"dbo\",\"retention_days\":7,\"cdf_enabled\":false}'}
objs = [{'object_id':'oracle.members','source_schema':'dbo','source_table':'MEMBERS'}]
iid = cp_mirror.ensure_mirrored_database(t, '79f30543-1f5c-451f-9ef7-b46b24dc7223', ds, objs)
print('mirrored database', iid)
cp_mirror.ensure_bronze_shortcuts(t, '79f30543-1f5c-451f-9ef7-b46b24dc7223', '8e60262f-3598-4fab-b3e7-178ae185f718', iid, objs)"
```

Expected: a mirrored database id printed, `startMirroring` accepted, and `+ shortcut bronze/dbo.members`. **If the shortcut is rejected here, stop** — that is the one result that reopens ADR-0001, and the fallback is to read `onelake_path()` directly and skip shortcuts for the non-CDF path.

Record the returned item id into the datasource's `mirror_json.item_id`.

- [ ] **Step 5: Verify the data path**

Run `cp_pl_main` for the load group, then confirm in a notebook:

```python
spark.read.format("delta").load(tpath("silver", "dbo_members", "oracle")).count()
spark.read.format("delta").load(tpath("silver", "dbo_members_history", "oracle")) \
     .groupBy("_version_origin").count().show()
```

Expected: silver has one row per member; history has the same count, all `_version_origin = seed`. Change one row at source, rerun, and confirm exactly one new version appears with `_version_origin = change` and a non-null `_changed_after`.

- [ ] **Step 6: Commit**

```bash
git add src/cp/workers/plan.py notebooks/history_worker.py deploy/manifest.yml config/steps.yml notebooks/cp_framework.py
git commit -m "feat: load_history step and mirrored-object end-to-end path (ADR-0003)"
```

---

## Deferred

Not in this plan; each needs its own once the above is running:

- **Change fidelity (CDF capture).** `history_fidelity='change'`: read `readChangeFeed` from the last processed commit version, requires the bronze shortcut, and needs gap detection (ADR-0002) plus reseed suppression before it is production-safe.
- **Reseed detection.** A commit that deletes and reinserts approximately the whole table must be recognised and suppressed (ADR-0002).
- **Rename confirmation.** Drift flagged for human confirmation, with a repair merging the two history columns (ADR-0004).
- **`_dq_failed` on history rows.** Task 10's reconciliation already tolerates the column; the DQ evaluation that populates it is not built.
- **Framework-object history.** `track_history=true` on a non-mirrored object — same mechanism, incoming set from bronze's latest snapshot rather than the mirror (ADR-0001).
