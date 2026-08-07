# Context — Fabric Control Plane

The shared language of the control plane. Glossary only: what a term means, and what it does
not. No implementation detail, no decisions, no plans.

## Layers

**Bronze** — the raw landing layer. Holds data as it arrived from the source, unmodelled and
uncleansed. Bronze is *not* a single shape: how it retains data depends on how the object is
ingested (see *Framework-ingested object*, *Mirrored object*).

**Silver** — the conformed **current state** of an object: one row per business key (or one row
per distinct row for keyless objects), cleansed, quality-checked, with failing rows removed to
quarantine. Silver answers "what is true now". Silver never carries multiple versions of the
same key.

**Silver history** — the companion to a silver table, carrying the **versions** of each record
rather than only the current one. Effective-dated, with a current-version marker. Silver history
answers "what was true then". It is the layer a data scientist queries for a record's past.

**History fidelity** — how completely a silver history represents a record's past. Every silver
history has the same *shape*; they differ in *resolution*, and an object declares which it offers:
*observed* (a version per load — the default) or *change* (a version per source change, which
requires the change feed). The distinction is not cosmetic: a record that changed three times
between loads yields one version under *observed* and three under *change*.

**Gold** — the modelled layer: star schema, dimensions and facts, built from silver. Gold is a
*modelling* concern and is unaffected by how an object reached silver.

**Quarantine** — rows removed from silver because they failed a data-quality rule of error
severity. Quarantined rows are excluded from silver, not deleted.

**Source query** — the deliberate human seam between silver and gold. A modeller writes it,
choosing which silver tables a gold object is built from and how they are shaped. It is the one
place in the pipeline where judgement, rather than configuration, decides what the data means.

**Stage** — the result of a source query, and the input to a gold merge. A stage holds **exactly
one row per key**: its grain is the gold object's grain. More than one row per key means the key
is under-specified or the source query is fanning out — never that the extra rows should be
discarded.

## Ingestion

**Framework-ingested object** — an object the control plane pulls itself, through a registered
connector. Bronze for such an object is **append-only**: every load is retained and stamped, so
bronze holds the object's full history.

**Mirrored object** — an object whose data arrives through Fabric Mirroring rather than a
control-plane connector. Bronze for such an object is the **mirrored table**, which holds only
the current state; Fabric maintains it continuously from the source's own change log.

**Change history** — every intermediate value a record held, including values that were
superseded between two observations. Obtainable for a mirrored object because the change feed
records each change; not obtainable by polling a table.

**Observed-state history** — the values a record held *at the moments it was observed*. Changes
occurring between two observations are not represented. This is the strongest guarantee a
snapshot-based load can offer.

**Soft delete** — a record removed at source but retained downstream, marked as deleted with the
time it disappeared. The record's last known values are preserved. A key that reappears later
ceases to be marked deleted.

**Reseed** — a full reload of a mirrored table initiated by Fabric rather than by a change at
source (for example, after a source schema change). A reseed is *not* a business event: the rows
it restates did not change at source, and history must not record them as though they did.

**Change feed** — the record of every insert, update and delete applied to a mirrored table.
Readable only while retained; it is a means of observing changes, not a place changes are kept.

**Capture** — the act of folding the change feed into silver history. Capture must happen while
the change feed is still retained; changes not captured in time are lost, not merely delayed.

**Gap** — a period for which changes were not captured before the change feed was discarded. A
gap is permanent and unrecoverable: the intermediate values no longer exist anywhere.

**Degraded object** — an object whose history is known to be incomplete, because of a gap. Its
current state remains trustworthy; its history does not, and it is marked rather than quietly
carried forward.

**Seed version** — the first version of a record in silver history, written from a snapshot taken
when the object was onboarded (or re-onboarded after a gap). Its start date is a **knowledge
boundary**, not a business date: it records when the platform began observing the record, not
when the record came into existence. A seed version is marked as such so it can never be mistaken
for the record's creation.

## Configuration

**Datasource** — a registered source system: its type, connector, and where its connection
secret lives. Never the credentials themselves.

**Source object** — a single table or resource within a datasource, together with how it should
be loaded and keyed.

**Config-as-code** — the principle that what the platform loads and how it governs it is
declared as configuration held in git and promoted between environments, rather than built by
hand per source.
