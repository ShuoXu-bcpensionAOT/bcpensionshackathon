# Mirrored objects keep silver current-state and gain a history sibling

---
status: accepted
---

Fabric Mirroring lets us ingest Oracle tables that have no timestamp column, which today force a
full daily pull because incremental load is impossible. Adopting it removes the append-only
bronze those objects relied on for history — a mirrored table holds only current state. Rather
than redefine silver as effective-dated (which would change every existing silver table and make
`gold/scd2.py` version already-versioned rows), **silver keeps its contract of one row per key,
current state, and every object may additionally expose `silver.<table>_history` carrying its
versions.** History is materialised by a single mechanism — compare the incoming set at a pinned
version against current, and write a version where they differ — so a mirrored object and a
framework-ingested one differ only in where that incoming set comes from. It is written at load
time by default, and from the Delta change data feed only where change-level fidelity is
explicitly requested (ADR-0005). Gold is untouched and continues to read the current table.

## Considered options

- **Silver becomes effective-dated everywhere.** One contract across all objects, but every
  existing silver table changes shape and gold would double-date rows it already versions.
- **Silver's shape depends on ingestion mode.** Rejected outright: "what shape is this silver
  table?" becomes unanswerable without reading config, and gold could not be written generically.
- **Treat the mirror as a source and keep writing an append-only bronze.** Preserves every
  existing contract unchanged and needs no new concepts, but stores a third copy of the data for
  no benefit the history sibling doesn't already provide.
- **Derive a framework object's history as a view over bronze rather than materialising it.**
  Appealing because bronze already holds every version, but unusable for snapshot-loaded objects:
  bronze holds a complete copy per load, so the view would scan every snapshot ever taken on every
  query. It works only for incremental objects — the load type this change is *not* about — and
  buys a conditional promise and a second implementation in exchange for little storage.

## Consequences

- Current state and history are produced by two independent passes, so a reconciliation check
  between them is required rather than optional. Both passes read the same pinned version of the
  mirror, so they agree by construction and the check asserts rather than tolerates: for each key,
  the latest history version that passed quality must match the current row, and a key with no
  passing version must be absent from current. Fused into the change-detection pass it is close to
  free, since that pass already joins the same two sets.
- History carries cleansed values so it aligns with silver, but is not quarantine-filtered — rows
  failing quality rules are flagged, not dropped, because omitting a version would make history
  lie. Cleanse rules evolve, so history reflects the rules in force when each version was
  captured; `_silver_run_id` makes that traceable but not reproducible.
- Change history is only obtainable while the change feed is retained. Capture must run more often
  than `retentionInDays`, and a missed window loses changes permanently — see ADR-0002.
- Surfacing a mirrored table inside the bronze lakehouse relies on a OneLake shortcut to an item
  that is not a lakehouse. The shortcut API documents its OneLake targets as lakehouse, KQL
  database or warehouse, which would rule this out; that list is stale. Creating a shortcut to a
  Fabric SQL Database item was accepted (201), including into a schema folder of a schema-enabled
  lakehouse. Reading the item's OneLake path directly by GUID also works and needs no shortcut,
  which covers every case except the change feed — Microsoft requires a shortcut for that.
