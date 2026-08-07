# "A record changed" is decided over the columns common to both versions

---
status: accepted
---

Versioning in both silver history and gold type-2 dimensions asks "did this record change" by
comparing a hash over the record's business columns. That hash is computed from whatever columns
happen to be present, so **adding or removing a single column changes the hash of every row** —
closing and reopening every dimension version, and writing a new history version for every record,
none of which changed. Under Fabric Mirroring, schema drift is routine rather than exceptional, so
this would recur on every source column change. **The comparison is therefore made over the
columns common to the stored version and the incoming one, and the column set is recorded with
each version**, so a schema change appears in a record's timeline as a schema event rather than
disguised as a data change.

## Consequences

- Adding a column no longer re-versions anything. The column is added to the table by Delta schema
  evolution when the next genuine insert occurs, and rows written before it exist carry null.
- A gold column materialises **lazily** — the append that would evolve the schema is skipped when
  nothing changed, so a newly added source column does not appear in a dimension until the first
  real new-or-changed record. This is correct but surprising, and worth saying out loud.
- Recording the column set per version makes silver history self-describing: a null in an old
  version is explained by the column set in force at the time, rather than requiring a reader to
  cross-reference drift events.
- Gold does not carry the column set, so nulls in old dimension versions stay ambiguous. Accepted:
  gold is the curated layer, and silver history can reconstruct the distinction if anyone needs it.
