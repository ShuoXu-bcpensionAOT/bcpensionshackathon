# Observed history records the window a change happened in, not a point

---
status: accepted
---

Under observed fidelity a version is written when a load *notices* a record differs, which is not
when the record changed — the change occurred somewhere since the previous load. Recording only
the load time would present an observation as a fact, and anyone reasoning about *when* something
changed (time-to-event analysis, cohorting by change date, "when did this member's status change")
would silently be wrong by up to one load interval. **Each version therefore records
`_changed_after`, the previous successful load for that object, alongside `_valid_from`, so the
change is bounded to `(_changed_after, _valid_from]` — exactly what is known and no more.**

## Consequences

- `_changed_after` is a run-level value already captured in the object's run log, not a per-row
  observation, so it costs one column and no extra work. The alternative — refreshing a
  `_last_observed_at` on every current row every load — carries the same information at the price
  of rewriting the entire table on every run.
- The two fidelities become self-describing in the data. Under change fidelity the commit
  timestamp *is* the change time, so `_changed_after` and `_valid_from` collapse together; a
  consumer can tell which fidelity they hold without consulting configuration.
- A load skipped because the mirrored table's Delta version has not advanced is still a valid
  observation — indeed a stronger one than a scan, since the absence of a new version proves
  nothing changed. Skips therefore advance `_changed_after` and legitimately tighten the window;
  treating a skip as "no information" would widen the bound for no reason.
