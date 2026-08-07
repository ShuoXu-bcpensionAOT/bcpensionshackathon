# Load groups stay business-shaped; mirrored objects skip bronze at the worker

---
status: accepted
---

A mirrored object has no bronze load step — the mirrored table *is* bronze — but the `steps` table
that drives orchestration is keyed by load group, not by object, so a group containing both
mirrored and framework-ingested objects cannot simply switch `load_bronze` off. **The bronze step
still runs for the group and no-ops per object where the connector is `mirror`, rather than
segregating mirrored objects into their own load groups.** A load group means "a business batch";
splitting it by ingestion mechanism would let an implementation detail dictate the shape of the
schedule, so that a single business domain fragments into a mirrored group and a framework group
for no reason a business owner would recognise.

## Consequences

- A new `load_history` step captures the change feed, active per load group, planning only objects
  flagged to track history. That flag defaults on for mirrored objects, because nothing else holds
  their history, and off for framework-ingested objects, whose bronze already does.
- `load_type` must be set explicitly for mirrored objects. Left at its usual default, `silver.py`
  classes the object as a snapshot and takes a path that assumes accumulated bronze — a silent
  wrong answer rather than a failure.
- The last processed commit version is per-environment runtime state and must never be promoted.
  It follows the existing rule that runtime state lives in the lakehouse, but it is the item most
  likely to be swept into a configuration snapshot by accident.
