# History is observed at load time by default; change fidelity is opt-in and gated

---
status: accepted
---

Change-level history for a mirrored object requires the Delta change data feed, which is a billed
extended capability and — critically — **is enabled per mirrored database, not per table**, so one
object asking for it makes every table in that database produce change files. **Silver history is
therefore captured at load time by default, giving a version per load rather than per source
change, and change fidelity is requested per object but only permitted when its datasource has
been deliberately enabled for it.** Deployment fails when an object requests change fidelity from
a datasource that is not change-feed enabled, rather than silently switching on a capability that
bills across every table in the database.

## Consequences

- A silver history's shape does not tell you its fidelity. An object declares whether it offers
  *observed* or *change* history, and consumers must be able to see which — a record that changed
  three times between loads yields one version in the first case and three in the second.
- Turning on change fidelity is a datasource-level decision made with the cost in view, not a
  side effect of an object-level flag. Where one table in a high-change database needs history and
  the rest would be expensive noise, the remedy is a second mirrored database scoped to that
  table, not blanket enablement.
- Observed history still detects deletes, because deletion is established by comparing keys at
  load time rather than by reading the change feed. Only *intermediate* states are lost.
