# A stage has one row per key, and gold honours the effective date it carries

---
status: accepted
---

The source query is the deliberate human seam between silver and gold: a modeller decides which
silver tables a dimension is built from. The gold runner previously undermined that in two ways —
it silently de-duplicated the stage by key, and it stamped every type-2 version with the time gold
happened to run. **A stage must contain exactly one row per key, and a stage that does not is a
failure rather than something to de-duplicate; where the stage carries an effective date, the
type-2 strategy uses it instead of the current timestamp.** Duplicate keys are always a human
error — either the key is under-specified for the intended grain, or the source query is fanning
out — and silently keeping an arbitrary row hides both.

## Consequences

- Silver history cannot be fed directly into a type-2 dimension: it holds many rows per key by
  definition and so fails the grain check. Record history and dimension versions therefore remain
  distinct artifacts, separated by construction rather than by convention. A source query may
  still read silver history, provided it selects a single version per key.
- The duplicate-key failure must identify the offending keys and their counts. An unqualified
  "duplicate keys" on a large stage tells the author there is a problem but not where it is.
- Honouring a stage-supplied effective date gives gold the same temporal honesty ADR-0007 gave
  silver history. Where the stage carries no such column the behaviour is unchanged, so this costs
  nothing for dimensions built from current state.
- Keyless gold objects (facts) are unaffected: with no key there is no grain to violate.
