# Incomplete history fails loudly; reseeds are suppressed

---
status: accepted
---

Silver history for a mirrored object is folded from the Delta change data feed, which is retained
only for `retentionInDays` before vacuum. Two events can make that history wrong rather than
merely late: a **gap**, where capture did not run inside the retention window and changes were
vacuumed away, and a **reseed**, where Fabric restates an entire table (typically after a source
schema change) and the change feed reports a delete and insert for every row. **On a gap the
object fails loudly — history is not written, the object is marked degraded, and an operator
decides how to repair. A reseed is recognised and suppressed: it restates rows the business never
changed, so it must not be recorded as history.** Continuing silently was rejected because a data
scientist can work around a hole they know about, but cannot detect a fabricated change.

## Consequences

- Every mirrored object tracks the last processed commit version, and each run reads from the next
  one. A version that is no longer available proves a gap.
- Reseed detection is heuristic: a single commit deleting and reinserting approximately the whole
  table is treated as a restatement, not as business change. A genuine mass update is
  indistinguishable and would be suppressed too — accepted, because falsely recording one is worse
  than falsely ignoring one, and the current state is unaffected either way.
- Capture cadence and `retentionInDays` are coupled operational settings, not independent knobs.
  `retentionInDays` is set explicitly rather than inherited, because the documented default is
  inconsistent between Microsoft's own pages.
