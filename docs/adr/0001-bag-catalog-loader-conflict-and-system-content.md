# ADR-0001: BagCatalogLoader — conflict handling and system content

Date: 2026-05-11
Status: Proposed

## Context

`BagCatalogLoader` writes a bag's contents into a destination ERMrest
catalog. End-to-end live-catalog testing (driven by `deriva-ml`'s
`clone_via_bag` flow against a `localhost` Deriva server) exposed a
cluster of issues that share a root cause: the loader assumed a
**clean destination** with no schema initialized and no system content
pre-populated, while the realistic destination has the **ML schema
already created** with required vocabulary terms loaded.

The bugs we hit while diagnosing:

1. `_infer_schemas_from_bag` returned every schema in the bag's
   `schema.json`, including ERMrest's structural schemas (`public`,
   `WWW`, `_acl_admin`). The bag walker correctly excludes them on
   build, but the export engine snapshots the full source model.
   Loading them produced SQLAlchemy automap classes with no usable
   primary key, then crashed the cross-schema FK loop with
   `'NoneType' has no attribute '__table__'`.

2. `BagDatabase._load_data` iterated CSVs in `rglob` order, which is
   filesystem-walk order — child rows land before parents. With
   `foreign_keys=ON` (the project's default), child inserts failed.

3. Deriva-ml's schema has real FK cycles (`Dataset → Dataset_Version →
   Dataset`). Topological sort can't be strict; `ForeignKeyOrderer`
   breaks one back-edge. But SQLite's per-statement FK enforcement
   doesn't honor cycle-broken orderings, and even one-shot deferred FK
   checks don't help because the bag legitimately ships dangling FK
   refs (an anchor-scoped walk picks one `Dataset` and pulls every
   `Dataset_Version`, leaving rows whose `Dataset` ref is outside the
   slice).

4. The loader's dangling-FK check ran against *every* FK on the table,
   including `RCB`/`RMB` references to `public.ERMrest_Client`. Because
   `public` is correctly excluded from the bag, the loader saw an
   empty parent set and reported the URL as "dangling," firing
   `DanglingFKStrategy.FAIL`.

5. `clone_via_bag` didn't materialize the bag's `fetch.txt` references
   before invoking the loader. With the default `UPLOAD_IF_MISSING`,
   `validate_with_bag_state` rejected the holey bag up front.

6. `_insert_rows` posted CSV-encoded array values
   (PostgreSQL literal form `{}`, `{a,b}`) directly to ERMrest, which
   expects JSON arrays. ERMrest returned
   `cannot call json_array_elements_text on a scalar`.

7. `deriva.core.ermrest_model.ArrayType.__init__` set `is_array = True`
   as a *local variable* instead of `self.is_array = True`, so every
   array column reported `is_array=False`. This had been masked because
   no caller before us inspected the attribute.

8. The next blocker on the test run: `Asset_Type` in the destination
   catalog was pre-populated by `create_ml_catalog` with system terms
   (e.g., `Execution_Config`). The bag also carried those rows. POSTing
   them produced 409 `duplicate key value violates unique constraint`.
   Same applies to `Workflow_Type`, `Dataset_Type`, `Execution_Status`,
   and every other system vocabulary.

(8) is the one we cannot fix by extending the existing loader.
Vocabulary tables are **content-addressed by `Name`**, not by `RID`,
and the same Name may have *different RIDs* on source vs destination.
An `on_conflict=skip` keyed on RID would silently create duplicate
Names; a strict `on_conflict=fail` blocks any clone into an
initialized catalog.

## Decision

`BagCatalogLoader` adopts a **per-table-class conflict policy** with
explicit destination preconditions, replacing the current "POST every
row from every reached table" approach.

### Destination preconditions

The loader assumes the destination catalog is **schema-initialized**:

- The `deriva-ml` schema exists (or whatever schema the loader is
  pointed at).
- System vocabularies exist with their schema-required terms
  pre-populated.
- Data tables are empty (or, with the new policy, may contain rows
  the loader treats per the conflict policy).

`create_ml_catalog` already produces this state, so it remains the
canonical way to prepare a destination. Loading into a truly empty
catalog (no schema, no system content) is **out of scope** for
`clone_via_bag`; that's a `create_ml_catalog` job.

### Conflict policy

The loader classifies each in-scope table at start-up and chooses one
of four strategies:

| Table class | Strategy | Reasoning |
|-------------|----------|-----------|
| System schema (`public`, `WWW`, `_acl_admin`) | `skip-table` | Never copy; destination owns these. |
| Out-of-bag schema | `skip-table` | Same as above but for any schema not in `self.bag_db.schemas`. |
| Vocabulary table (has a unique `Name` key per `VocabularyTableDef`) | `match-by-name` | Look up existing row by `Name`; if absent, insert; if present, **remap the source RID to the destination RID** for any child row that references it. RIDs differ across catalogs for vocab. |
| Content table (everything else) | `fail-on-rid-conflict` (default) / `skip-by-rid` (opt-in) | The destination is supposed to be empty; conflict means caller error. `skip-by-rid` is for re-runs of a partial load. |

`fail-on-rid-conflict` is the default rather than skip because a
content-table RID collision usually means the destination already has
a partial copy from a prior run — re-running blindly would mask data
loss. The opt-in `skip-by-rid` is the explicit "I know, keep going"
signal.

### System columns (`RCT`/`RMT`/`RCB`/`RMB`)

The loader preserves provenance for **creation** (`RCT`, `RCB`) and
lets the destination set **modification** (`RMT`, `RMB`):

- `RCT` (Row Creation Time) and `RCB` (Row Created By) carry real
  provenance about when and by whom the source row was first written.
  We pass them through to preserve audit history across a clone.
- `RMT` (Row Modification Time) and `RMB` (Row Modified By) describe
  the *latest* modification. Whatever value the source had is about
  to be overwritten by the destination on the very next update —
  preserving them buys nothing.

Concretely, replace today's `?nondefaults=RID` (which silently
preserves any caller-supplied `RMT`/`RMB`) with the same
`?nondefaults=RID,RCT,RCB` that deriva-py's existing
`ErmrestCatalog.clone_catalog` and `asyncio/clone.py` use. That's the
authoritative precedent — every other catalog-clone path in deriva-py
already does this.

The dangling-FK check skips system-column FKs into out-of-bag schemas
(typically `public.ERMrest_Client`) since those parents are resolved
by the destination catalog at insert time, not by the loader.

### Bag mirror integrity is advisory

The bag SQLite mirror is a transport medium, not the authoritative
store. The loader's job is to apply destination integrity (via
`DanglingFKStrategy`), not to re-enforce source integrity in the
mirror. Concrete consequences:

- `BagDatabase._load_data` runs under `PRAGMA foreign_keys = OFF` for
  the load transaction (re-enabled after). The pragma is on for
  read-side queries from the ORM (well-formed bags still satisfy FK
  on read).
- FK-typo'd CSVs from a buggy producer surface at the destination
  catalog with `DanglingFKStrategy`, not at bag-open time.

### What the seven fixes already shipped imply

The seven fixes from the diagnostic pass are **all consistent with
this decision**:

- Fixes 1, 4 — schema filtering / dangling-FK skip — encode "the
  destination owns system content."
- Fixes 2, 3 — FK-aware CSV ordering, foreign-keys-off during load —
  encode "the bag mirror is advisory."
- Fix 5 — `bdb.materialize` before load — encodes "asset bytes must be
  local before the upload phase."
- Fixes 6, 7 — array coercion + `ArrayType.is_array` typo — encode
  "the wire payload must match ERMrest's JSON ingest expectations."

What they *don't* address is (8): the conflict-policy gap. That's the
work this ADR opens.

## Consequences

**Positive:**

- `clone_via_bag` becomes a real production path against an ML-
  initialized destination.
- Vocabulary RIDs stay deterministic on the destination — child rows
  that referenced source RIDs get rewritten to destination RIDs during
  the load, which is the only correct behavior.
- The loader's behavior matches a documented contract; future changes
  to "what gets copied vs reconciled" have a place to land.

**Negative:**

- Vocabulary remap requires a second pass on data-table rows that
  reference vocab. We track this as an in-memory `{vocab_table: {src_rid:
  dst_rid}}` translation table; rewrite child rows during the
  serialize-for-post step. Not free, not expensive.
- `fail-on-rid-conflict` is a behavior change for callers expecting
  silent re-runs. The opt-in `skip-by-rid` mirrors the prior implicit
  behavior; document the switch in the migration notes.

**Out of scope (separate work):**

- Loading into a destination with no schema at all (use
  `create_ml_catalog` first).
- Schema *evolution* across the boundary (source has a column
  destination lacks): orthogonal to conflict handling.
- Asset-mode rewrites beyond the current `ROWS_ONLY` /
  `UPLOAD_IF_MISSING` / `UPLOAD_FORCE` trio.

## Status: Proposed

This ADR captures the diagnostic findings from the
`clone_via_bag` end-to-end run. The seven fixes have landed in the
deriva-py `fix/bag-system-schema-filter` branch (with unit tests). The
conflict-policy work itself is tracked separately and gates the
`xfail` removal on `deriva-ml`'s `clone_via_bag` integration tests.
