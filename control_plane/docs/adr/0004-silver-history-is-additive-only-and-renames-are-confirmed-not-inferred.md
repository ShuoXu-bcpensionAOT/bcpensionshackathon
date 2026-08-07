# Silver history is additive-only, and renames are confirmed rather than inferred

---
status: accepted
---

Fabric Mirroring propagates column additions, drops and renames from the source, so a mirrored
object's schema moves underneath the platform. Silver current follows the source, but silver
history cannot: following a drop would delete the past. **A column is never removed from silver
history, so history's schema is a superset of current's rather than identical to it.** A rename is
indistinguishable from a drop-plus-add at the point of detection, and guessing wrong splits one
attribute's history across two columns without anyone noticing — so **a suspected rename is
flagged for human confirmation, while capture continues treating it as a drop-plus-add**, and a
repair merges the two columns once confirmed.

## Consequences

- After a column is dropped, later history versions carry null for it, which is indistinguishable
  from a genuine null without knowing when the drop happened. `schema_drift_event` already records
  that, so interpreting history correctly *depends* on it — it is a documented part of the
  contract, not incidental logging.
- An unconfirmed rename leaves history accurate but awkward: values split across the old and new
  column at the rename point. This does not escalate. History that is true but inconveniently
  shaped is not the same failure as history that is missing or fabricated, and only the latter
  two degrade an object (ADR-0002).
- Source data type changes are out of scope: Fabric cannot mirror them, and attempting one can
  trigger repeated reseeds. That surfaces as an operational alert, not as schema evolution.
