# Column-construction dedup — scoping note

## Context

Three places in `deriva.bag` turn an ERMrest column description into a
SQLAlchemy `Column`. They do most of the same work, and the divergence
is partly by-design (read vs write paths have different fidelity needs)
but partly accidental drift.

This note scopes a possible dedup refactor: what's actually shared, what's
intentionally different, what's drift, and whether the cleanup is worth
the churn.

The three implementations:

| # | Location | Direction | Used by |
|---|---|---|---|
| 1 | `deriva.bag.schema.SchemaBuilder` | bag-build (in-memory) | bag-export flow |
| 2 | `deriva.bag.database.BagDatabase` | bag-read (reflect from SQLite) | downstream bag consumers |
| 3 | `deriva.bag.schema_io.ermrest_json_to_metadata` | bag-write (lossless round-trip) | `BagBuilder` callers needing `col.info` |

deriva-ml's `bag_commit.build_execution_bag` exercises all three
transitively: uses #3 directly, and the bag-load step in
`BagCatalogLoader` uses #1 and #2 under the hood.

## What's genuinely shared

All three implementations agree on the core mapping rules. These should
stay shared after dedup:

- **ERMrest type → SQLAlchemy type via a dict, `String` fallback.**
  All three follow the same pattern.
- **`RID` is the sole primary key** (not a generic unique constraint).
  `_is_key_column` (or equivalent inline check) returns true only for
  `name == "RID"` in all three.
- **Nullable relaxation for non-`RID` columns.**
  `RID`'s `nullable` mirrors the catalog's `nullok`; every other column
  is locally nullable regardless. The rationale is staging-friendliness:
  server-set columns (`RCT`/`RCB`/`RMT`/`RMB`) won't violate NOT-NULL in
  the local mirror, and unique constraints are checked at load time by
  ERMrest, not by the local SQLite/SQLAlchemy layer.
- **`default` and `comment` pass through unchanged.**

Citations (current versions of each file):

- Type map: `schema.py:557–569`, `database.py:249–263`, `schema_io.py:107–128`.
- PK detection: `schema.py:617–630`, `database.py:265–267`, `schema_io.py:323`.
- Column-creation loop: `schema.py:784–791`, `database.py:307–314`, `schema_io.py:382–391`.

## What's intentionally different

`schema_io.ermrest_json_to_metadata` is the **only** path that populates
`col.info`. It stashes:

- `nullok` — the authoritative ERMrest flag (kept separate from
  SQLAlchemy's relaxed `nullable`).
- `ermrest_typename` — original ERMrest typename (`int2`/`int4`/`int8`
  distinctions, `timestamp` vs `timestamptz`).
- `annotations`, `acls`, `acl_bindings` — ERMrest metadata that has no
  SQLAlchemy equivalent.

This is correct: `schema_io` is the write path; it needs lossless
round-trip so the bag can later regenerate `schema.json` for ERMrest
ingest. The other two are read paths and don't need to preserve this
information. **Do not unify away the `col.info` stash.**

## Accidental drift

`schema_io.py` recognises 18 ERMrest types; `SchemaBuilder` and
`BagDatabase` only recognise 12. The missing six are:

- `text`, `longtext`
- `markdown`
- `ermrest_rid`, `ermrest_rct`, `ermrest_rcb`, `ermrest_rmt`, `ermrest_rmb`

The system types are unlikely to surface in column definitions in
practice (they're typically reserved for the system columns themselves,
which have hard-coded names). But `text`, `longtext`, and `markdown` are
ordinary catalog types that any caller could trip over.

When `SchemaBuilder` or `BagDatabase` encounters one of these, the
fallback silently downgrades the column to plain `String`. No data loss
(both fit in `TEXT` storage) — but if the value happens to have a type
constraint that depended on the original typename, that constraint is
gone from the local mirror.

This is the only candidate for genuine dedup gain.

## Recommendation

**Two changes, both small:**

1. **Promote the type map to a shared module.** Move it (and the
   `String` fallback) into `deriva.bag._column_types` (or similar).
   `SchemaBuilder` and `BagDatabase` import the canonical 18-type map.
   `schema_io.py` already uses the full set. No behaviour change for
   `schema_io`; `SchemaBuilder`/`BagDatabase` gain the six missing
   types.

2. **Promote `_is_key_column` to the same shared module.** It's
   trivial (one-line `name == "RID"` check) but currently appears in
   three places. Same module makes the "everywhere we look at
   columns, here's the rule" colocation natural.

**Do _not_** try to share the column-creation loop itself — the
`col.info`-stashing path is materially different and the three call
sites have different orchestration concerns (table-by-table vs.
schema-wide, with or without FK collection). Leave those separate.

## Cost / benefit

- **Cost:** changes touch three files plus a new shared module. No
  caller-side changes — pure refactor.
- **Benefit:** defensive correctness (`text`/`longtext`/`markdown`
  columns won't silently downgrade), plus a clear "this is the rule"
  module for future maintainers.
- **Risk:** very low — the type map is data, not behaviour. The
  `_is_key_column` extraction is pure refactor.

## Status

Scoping only. Not scheduled. Captured here so the future implementer
doesn't have to re-derive the analysis.

## References

- `docs/adr/0001-bag-catalog-loader-conflict-and-system-content.md` —
  bag-loader conflict policy and the system-content boundary.
- deriva-ml's `docs/design/bag-based-commit-execution.md` — the largest
  downstream caller of these paths.
