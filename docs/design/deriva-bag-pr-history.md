# deriva.bag — PR history

This document preserves the merge-time documentation from every pull request that contributed to the **`deriva.bag`** submodule on the `deriva-ml` branch of `deriva-py`. The branch's commit history was squashed on 2026-05-13 to collapse 35 small merged PRs (plus the audit-cleanup work) into a single coherent commit; this file is the durable record of the per-PR documentation that history rewrite would otherwise have buried.

Each section below is the full body of one merged PR, in merge order, with the original PR title, number (linked), head ref, and merge timestamp. The original commit SHAs are preserved at the `deriva-ml-pre-squash-2026-05-13` tag on origin.

## Table of contents

- [#212 — feat(bag): deriva.bag submodule — bag-oriented data movement](#pr-212)
- [#213 — fix(bag): seven fixes for BagCatalogLoader end-to-end](#pr-213)
- [#216 — fix(clone): repair Column.sname AttributeError + warn on default mismatch](#pr-216)
- [#215 — feat(bag): BagCatalogLoader conflict-policy rewrite (closes #214)](#pr-215)
- [#217 — feat(bag): implement BagCatalogLoader._upload_assets via HatracStore](#pr-217)
- [#218 — fix(bag): strip versioned-URL suffix; coerce date/datetime to ISO](#pr-218)
- [#219 — feat(bag): anchor-scoped FK-path query paths in CatalogBagBuilder](#pr-219)
- [#220 — fix(bag): vocab FULL emits one unfiltered processor; coerce empty strings to NULL](#pr-220)
- [#222 — fix(bag): _validate_anchors uses ERMrest GET (datapath has no .in_)](#pr-222)
- [#223 — feat(bag): two-phase insert for FK cycles via deferred-column PUT](#pr-223)
- [#224 — feat(bag): multi-path emission — union FK routes to each target table](#pr-224)
- [#225 — feat(bag): FKTraversalPolicy.terminal_tables for vocab-style enter-don't-exit](#pr-225)
- [#226 — fix(bag): terminal_tables block inbound only, still follow outbound](#pr-226)
- [#227 — feat(bag): BagBuilder.add_asset link mode (hardlinks for transient staging)](#pr-227)
- [#228 — feat(bag): DanglingFKStrategy.PRESERVE — trust destination for orphans](#pr-228)
- [#229 — feat(bag): FKTraversalPolicy.preserve_provenance — clone vs commit semantics](#pr-229)
- [#230 — fix(bag): _localize_asset_row falls back to embedded-asset path](#pr-230)
- [#231 — fix(bag): preserve Filename column; resolve bag-local path on demand](#pr-231)
- [#232 — fix(bag): SchemaBuilder.make_table_name normalises hyphens](#pr-232)
- [#233 — fix(bag): _make_bdbag pre-seeds bagit scaffolding to avoid double-data/](#pr-233)
- [#234 — fix(bag): SQLite mirror relaxes NOT-NULL on non-PK columns](#pr-234)
- [#235 — fix(bag): ermrest_json_to_metadata also relaxes non-PK NOT-NULL](#pr-235)
- [#236 — fix(bag): BagDatabase mirror also relaxes non-PK NOT-NULL](#pr-236)
- [#237 — fix(bag): preserve_provenance=False strips RCT/RCB/RMT/RMB from row body](#pr-237)
- [#238 — feat(bag): schema_io round-trips annotations, ACLs, snaptime](#pr-238)
- [#240 — fix(bag): schema_io preserves int2/int4/int8 distinction on round-trip](#pr-240)
- [#241 — feat(bag): BagCatalogLoader.run() handles notebook event loops](#pr-241)
- [#242 — feat(datapath): _ColumnWrapper.in_(values) operator](#pr-242)
- [#243 — feat(bag): hatrac_url_for helper for canonical asset URLs](#pr-243)
- [#244 — docs: scoping note for column-construction dedup](#pr-244)
- [#245 — feat(bag): FKTraversalPolicy.match_by_columns](#pr-245)
- [#246 — fix(bag): _rewrite_fks before insert in vocab + match_by_columns paths](#pr-246)
- [#247 — fix(bag): _rewrite_fks before match query in _load_match_by_columns_table](#pr-247)
- [#248 — refactor(bag): column-construction dedup — single source of truth](#pr-248)
- [#249 — refactor(bag): audit cleanup sprint 2026-05 (13 of 15 actions)](#pr-249)

---

<a id="pr-212"></a>

## PR #212 — feat(bag): deriva.bag submodule — bag-oriented data movement

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/212
- **Head ref:** `carl/deriva-bag-submodule`
- **State at squash:** MERGED
- **Merged at:** 2026-05-11T18:09:28Z

## Summary

Lands the **`deriva.bag`** submodule — a unified pipeline for moving Deriva
catalog content via BDBags. Implements the producer/consumer/loader trio
described in [ADR-0006 in deriva-ml](https://github.com/informatics-isi-edu/deriva-ml/blob/main/docs/adr/0006-bag-oriented-data-movement.md):

- **`BagDatabase`** — open a bag as a SQLAlchemy ORM (moved from `deriva.core.bag_database`, now uses WAL + `schema_meta` versioning).
- **`BagBuilder`** — write a bag from in-memory data (cifar/Kaggle, end-of-execution upload).
- **`CatalogBagBuilder`** — write a bag from a live ERMrest catalog by generating an export spec and driving `GenericDownloader`.
- **`BagCatalogLoader`** — load a bag into a destination ERMrest catalog (rows in FK-safe order + asset bytes via deriva-py's upload recipe + dangling-FK resolution).

Plus the foundation: `SchemaBuilder`/`SchemaORM`, `schema_io` (ERMrest JSON ↔ SQLAlchemy `MetaData` ↔ ERMrest `Model` ↔ `typed.SchemaDef`), `DataLoader` with `Sink` protocol and two implementations (`SQLiteSink`, `CSVSink`), five `DataSource` adapters (Bag/Catalog/DataFrame/Iterable/LocalDB), `Anchor` discriminated union, `FKTraversalPolicy` with `AssetMode`/`DanglingFKStrategy`/`VocabExport` `StrEnum`s, content-addressed `BagCacheIndex`, `sqlite_helpers` (WAL engine + `schema_meta`), and the deriva-bag BagIt Profile JSON document.

## Commits (eight, designed for clean cherry-pick into 2.0-dev later)

1. `0dc077e` foundation (init, sqlite_helpers, profile, profile JSON)
2. `e346425` BagDatabase move + WAL + schema_meta (shim left at `deriva.core.bag_database`)
3. `978379b` SchemaBuilder, schema_io, sources, loader
4. `5ac50c8` Anchor + FKTraversalPolicy
5. `028a092` content-addressed BagCacheIndex
6. `2902f7e` BagBuilder (constructive producer)
7. `129c91b` CatalogBagBuilder (walking producer)
8. `44188b0` BagCatalogLoader (bag → catalog)

## Tests

- **187 unit tests** pass (1 skipped pandas-conditional, 1 xfailed documenting a pre-existing upstream bug in `_localize_asset_row`).
- **94 non-network `deriva.core` tests** still pass — the `BagDatabase` move was non-breaking (legacy import path works via shim).
- Live-catalog integration tests for `CatalogBagBuilder.build()` and `BagCatalogLoader.run()` will be added once a test catalog is wired up. The unit tests use mocks to exercise spec-generation and FK-walk logic without a network dependency.

## Known follow-ups for the deriva-ml migration PR

- Fix pre-existing asset-URL localization bug in `_localize_asset_row` (full URL vs. path-keyed map mismatch) — xfail'd test pins the intended behavior.
- Apply the same `ensure_schema_meta` `INSERT OR IGNORE` fix to deriva-ml's `local_db/sqlite_helpers.py` (the bug is fixed in `deriva.bag.sqlite_helpers`).
- Wire `BagCatalogLoader`'s asset upload to `DerivaUpload._uploadAsset` (currently stubbed with a warning; ROWS_ONLY mode is fully functional now; UPLOAD_IF_MISSING/UPLOAD_FORCE pass policy validation but defer the bytes transfer pending destination-catalog asset-mapping config).

## Cherry-pick plan for 2.0-dev

Each commit leaves the tree in a working state with passing tests, so commits can be picked individually as 2.0's other in-flight PRs land. Suggested batching:

- **First slice (foundation + consumer)**: commits 1-3. Lands the WAL engine factory, the profile artifact, `BagDatabase` at its new home, and the primitives shared with `local_db`.
- **Second slice (shared types + cache)**: commits 4-5. Adds `Anchor`/`FKTraversalPolicy`/`BagCacheIndex` — pure additions, no behavior changes.
- **Third slice (producers + loader)**: commits 6-8. The new producer/loader classes.

## Test plan

- [x] All unit tests pass locally (`uv run pytest tests/deriva/bag/`)
- [x] Legacy `deriva.core.bag_database` import path still works (shim verified)
- [x] Non-network `deriva.core` test suite still passes (94 passed, 105 skipped)
- [ ] Integration test against a live ERMrest catalog (TODO: wire test catalog into CI)
- [ ] Cherry-pick rehearsal onto 2.0-dev (verify each commit lands cleanly in order)

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-213"></a>

## PR #213 — fix(bag): seven fixes for BagCatalogLoader end-to-end

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/213
- **Head ref:** `fix/bag-system-schema-filter`
- **State at squash:** MERGED
- **Merged at:** 2026-05-11T22:54:43Z

## Summary

End-to-end testing of [deriva-ml](https://github.com/informatics-isi-edu/deriva-ml)'s `clone_via_bag` flow against a live Deriva server surfaced **nine distinct bag-pipeline bugs**. Each is small and self-contained; together they let the catalog→bag→catalog round-trip get past load-time validation.

[ADR-0001](docs/adr/0001-bag-catalog-loader-conflict-and-system-content.md) captures the broader design gap these fixes surface — `BagCatalogLoader` still lacks vocabulary-by-name reconciliation against an already-initialized destination — and catalogs the seven loader-side fixes as consistent prerequisites for that follow-up work. The remaining two are pre-existing latent bugs the loader-rewrite work would have hit eventually anyway.

## The fixes

### Loader / database / model (7)

| # | File | Symptom | Fix |
|---|------|---------|-----|
| 1 | [catalog_loader.py](deriva/bag/catalog_loader.py) | `'NoneType' has no attribute '__table__'` on bag open | Filter `public`/`WWW`/`_acl_admin` in `_infer_schemas_from_bag` |
| 2 | [database.py](deriva/bag/database.py) | `sqlite3.IntegrityError: FOREIGN KEY constraint failed` mid-load | Order CSVs via `ForeignKeyOrderer` before ingest |
| 3 | [database.py](deriva/bag/database.py) | FK constraint failure at COMMIT (cycles + slice gaps) | Run load with `PRAGMA foreign_keys = OFF` (restored after) |
| 4 | [catalog_loader.py](deriva/bag/catalog_loader.py) | Dangling-FK `FAIL` fires on `RCB`/`RMB` URLs | Skip FKs into out-of-bag schemas |
| 5 | [catalog_loader.py](deriva/bag/catalog_loader.py) | RCT/RCB stripped on insert | Use `?nondefaults=RID,RCT,RCB` (matches `clone_catalog`) |
| 6 | [catalog_loader.py](deriva/bag/catalog_loader.py) | `cannot call json_array_elements_text on a scalar` (400) | Coerce PG array literals to JSON arrays before POST |
| 7 | [ermrest_model.py](deriva/core/ermrest_model.py) | Every array column reports `is_array=False` | Typo: `is_array = True` → `self.is_array = True` in `ArrayType.__init__` |

### Latent bugs the integration run flushed out (2 — second commit)

| # | File | Symptom | Fix |
|---|------|---------|-----|
| 8 | [database.py](deriva/bag/database.py) | Asset URL never localized to local path on bag load | `_build_asset_map` keys by both full URL and `urlparse(url).path` |
| 9 | [sqlite_helpers.py](deriva/bag/sqlite_helpers.py) | `ensure_schema_meta` flakes under concurrent initial calls | `INSERT OR IGNORE` + re-read MAX in the "table empty" branch |

## What this does *not* fix

`BagCatalogLoader` doesn't yet have a conflict-handling story for an **ML-initialized destination** — vocabulary tables pre-populated by `create_ml_catalog` collide with the bag's vocab rows on insert. The [ADR](docs/adr/0001-bag-catalog-loader-conflict-and-system-content.md) lays out the per-table-class conflict policy needed (vocabulary → match-by-name with RID remap; content → fail-on-rid-conflict). That work is tracked separately in [#214](https://github.com/informatics-isi-edu/deriva-py/issues/214).

## Test plan

- [x] `tests/deriva/bag/` — 197 pass, 2 skipped (was 188 before)
- [x] New unit tests:
  - `test_infer_schemas_skips_system_schemas` (fix 1)
  - `test_bag_database_loads_child_after_parent` (fix 2)
  - `test_coerce_pg_array_*` (fix 6, four cases)
  - `test_bag_database_localizes_asset_urls_path_only_form` (fix 8)
- [x] xfails removed: `test_bag_database_localizes_asset_urls` (fix 8) now PASS
- [x] Broader `tests/deriva/core/` — 308 pass, 185 env-skipped (no `httpx` / no live catalog)
- [x] Concurrent-init race test in deriva-ml (`test_concurrent_initial_inserts_do_not_race`) — 5/5 runs pass with this branch installed (was flaky on main)
- [ ] Reviewer self-check: read [ADR-0001](docs/adr/0001-bag-catalog-loader-conflict-and-system-content.md) and confirm the design framing makes sense

## Companion PR

The matching deriva-ml PR adds live-catalog integration tests for `clone_via_bag` end-to-end, currently `xfail`-marked against the loader-rewrite tracked by ADR-0001 + [#214](https://github.com/informatics-isi-edu/deriva-py/issues/214). They auto-flip to PASS once the rewrite lands.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-216"></a>

## PR #216 — fix(clone): repair Column.sname AttributeError + warn on default mismatch

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/216
- **Head ref:** `fix/clone-catalog-column-sname`
- **State at squash:** MERGED
- **Merged at:** 2026-05-11T23:50:03Z

## Summary

Two adjacent fixes to `ErmrestCatalog.clone_catalog`, both surfaced when [deriva-ml#98](https://github.com/informatics-isi-edu/deriva-ml/pull/98)'s integration-test fixture tries to schema-clone a deriva-ml source catalog into a sibling destination.

## Fix 1: `Column.sname` AttributeError

`check_column_compatibility` referenced `src.sname` / `src.tname` to label the offending column, but `Column` doesn't have those attributes — it carries a `self.table` backreference. When a real column mismatch fired, the error builder raised `AttributeError` instead of the intended `ValueError`, masking the actual mismatch from callers.

**Fix:** use `src.table.schema.name` and `src.table.name`.

## Fix 2: over-strict `default` comparison

`check_column_compatibility` aborted whenever source and destination column `default` values differed. Too strict for the typical deriva-ml case: every vocabulary table's `ID` column default is a CURIE template baked with the catalog's project name — `"src-name:{RID}"` vs `"dst-name:{RID}"`. Both are correct for their respective catalogs, and `clone_catalog` never actually uses them — it carries explicit values for every copied row.

**Fix:** log a warning instead of raising. Type and nullability mismatches still abort (real schema incompatibilities).

## Verification

End-to-end repro: schema-clone of a deriva-ml catalog into a sibling now succeeds with the expected warnings:

```
WARNING:root:Source/dest column default differs for deriva-ml:Asset_Role:ID (src='clone-repro-src:{RID}', dst='clone-repro-dst:{RID}'); proceeding (clone copies explicit values for every row)
[similar for Execution_Status, Dataset_Type, Feature_Name, ...]
CLONE OK
```

`tests/deriva/` (excluding env-dependent live-catalog and `httpx` suites) — **310 pass**, 185 skipped (unchanged).

## Companion work

This unblocks the `dest_catalog` fixture in [deriva-ml#98](https://github.com/informatics-isi-edu/deriva-ml/pull/98). One more upstream item remains before those integration tests can XPASS — the `BagCatalogLoader._upload_assets` stub awaiting `DerivaUpload` integration. Tracked separately.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-215"></a>

## PR #215 — feat(bag): BagCatalogLoader conflict-policy rewrite (closes #214)

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/215
- **Head ref:** `feat/bag-loader-conflict-policy`
- **State at squash:** MERGED
- **Merged at:** 2026-05-11T23:51:08Z

## Summary

Implements the per-table-class conflict policy laid out in [ADR-0001](https://github.com/informatics-isi-edu/deriva-py/blob/feat/bag-loader-conflict-policy/docs/adr/0001-bag-catalog-loader-conflict-and-system-content.md). Closes [#214](https://github.com/informatics-isi-edu/deriva-py/issues/214).

**Vocabulary tables** are now reconciled by `Name` against the destination; the loader records a `src_rid → dst_rid` remap entry per matched row, and rewrites FK columns on **content tables** through that remap before POST. Content tables stay RID-stable with a fail-by-default conflict policy and an opt-in `SKIP_BY_RID` for resumable loads.

### Why this matters

The realistic destination for `clone_via_bag` is one created by `create_ml_catalog`, which has the `deriva-ml` schema initialized and the system vocabulary terms (`Execution_Config`, `Test`, `Training`, etc.) already inserted. Before this PR the loader POSTed every vocab row from the bag against the destination, triggering 409 `duplicate key value` on the very first system term.

## What changed

| Surface | Change |
|---|---|
| `traversal.py` | New `ContentConflictStrategy` enum (`FAIL` \| `SKIP_BY_RID`); new `FKTraversalPolicy.content_on_conflict` field (default `FAIL`) |
| `catalog_loader.py` | New `_TableClass` enum + `_classify_table`; new `_load_vocabulary_table` and `_load_content_table` paths; `_rewrite_fks` rewrites FK columns through the loader's `_rid_remap`; `_fetch_existing_rids` for the `SKIP_BY_RID` filter |
| `catalog_loader.py` | Two new `TableLoadStats` fields: `rows_matched_by_name`, `rows_skipped_on_conflict` |
| `__init__.py` | Re-export `ContentConflictStrategy` at package level |
| ADR-0001 | Promoted Proposed → Accepted; status section updated with implementation summary |

## Tests

Four new mocked unit tests in `tests/deriva/bag/test_catalog_loader.py`:

- `test_classify_table_detects_vocabulary` — classifier dispatch
- `test_vocab_load_matches_by_name_and_records_remap` — vocab path inserts new rows, remaps matched RIDs, propagates remap into child FKs
- `test_content_conflict_fail_propagates` — default `FAIL` surfaces ERMrest 409 to caller
- `test_content_conflict_skip_by_rid_filters_existing` — `SKIP_BY_RID` fetches existing RIDs and filters payload

`tests/deriva/bag/` goes 197 → **201 passing**, 2 skipped.

## Out of scope

End-to-end live-catalog runs against deriva-ml's [#98](https://github.com/informatics-isi-edu/deriva-ml/pull/98) integration tests **still fail** — but no longer on the conflict-policy gap. The remaining blockers are independent:

1. `ErmrestCatalog.clone_catalog` raises `'Column' object has no attribute 'sname'` when invoked with a schema that has feature association tables — surfaces when the integration-test fixture tries to clone the source's schema to set up the destination. **deriva-py bug, not loader bug**.
2. `BagCatalogLoader._upload_assets` for `UPLOAD_IF_MISSING` is a documented stub awaiting `DerivaUpload` integration.

Both deserve their own PRs.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-217"></a>

## PR #217 — feat(bag): implement BagCatalogLoader._upload_assets via HatracStore

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/217
- **Head ref:** `feat/bag-loader-asset-upload`
- **State at squash:** MERGED
- **Merged at:** 2026-05-11T23:58:24Z

## Summary

Replaces the documented stub in `BagCatalogLoader._upload_assets` with a real implementation that pushes asset bytes to the destination Hatrac. The third and final upstream blocker preventing [deriva-ml#98](https://github.com/informatics-isi-edu/deriva-ml/pull/98)'s integration tests from going green.

## How it works

For each asset row:

- Local file path comes from `Filename` (which `BagDatabase._localize_asset_row` has already rewritten to a bag-local path on bag open).
- Destination Hatrac path comes from the row's `URL` column (its path component — source and destination share the `/hatrac/...` namespace layout).
- `UPLOAD_IF_MISSING` (default): HEAD the destination object; skip upload when `Content-MD5` matches the row's stored MD5.
- `UPLOAD_FORCE`: skip the HEAD, re-upload unconditionally.
- Rows missing a local file or URL log a warning and are skipped (counted as neither uploaded nor deduped).

The destination `HatracStore` is constructed from the catalog's own server + credentials and cached on the loader for the duration of the run. We use `HatracStore.put_loc` (via `asyncio.to_thread`) and `HatracStore.head` for dedupe — no new infrastructure, just composing existing primitives.

## Why not `DerivaUpload._uploadAsset`?

`DerivaUpload._uploadAsset` is tightly coupled to the asset-mapping annotation paradigm — regex-based directory scanning, `match_groupdict`, per-mapping config — which a programmatic bag-to-catalog upload doesn't need. Using `HatracStore` directly keeps the loader narrowly focused on the bag → Hatrac transfer.

## Tests

7 new unit tests in `test_catalog_loader.py`:

- `test_hatrac_path_for_full_url` / `_bare_path` / `_non_hatrac_returns_none` — URL parsing helper
- `test_upload_assets_dedupe_skips_when_md5_matches` — HEAD-then-skip path
- `test_upload_assets_uploads_when_md5_differs` — HEAD-then-PUT path
- `test_upload_assets_force_bypasses_head` — `UPLOAD_FORCE` skips HEAD
- `test_upload_assets_skips_missing_local_file` — defensive

`tests/deriva/bag/` goes from 197 → **204 passing**, 2 skipped.

## Sequence after merge

With this and [#215](https://github.com/informatics-isi-edu/deriva-py/pull/215) (conflict policy) and [#216](https://github.com/informatics-isi-edu/deriva-py/pull/216) (clone_catalog fix) merged, all three upstream blockers for [deriva-ml#98](https://github.com/informatics-isi-edu/deriva-ml/pull/98)'s integration tests are gone. The xfail markers there should flip to XPASS on the next CI run.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-218"></a>

## PR #218 — fix(bag): strip versioned-URL suffix; coerce date/datetime to ISO

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/218
- **Head ref:** `fix/bag-loader-versioned-urls-and-dates`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T00:58:11Z

## Summary

Two end-to-end fixes surfaced when the [deriva-ml#98](https://github.com/informatics-isi-edu/deriva-ml/pull/98) integration tests reached the actual loader path (now possible thanks to #215, #216, #217).

### Fix 1: Versioned Hatrac URLs caused 405 on PUT

The bag's CSV carries `URL = .../hatrac/path/file.ext:VERSIONID` (versioned form, so consumers can fetch the exact source snapshot). The loader's `_upload_assets` passed that URL's path straight to `HatracStore.put_loc`, but Hatrac assigns versions on PUT and returns `405 Method Not Allowed` for writes to a versioned path.

**Fix:** `_hatrac_path_for` strips the `:VERSIONID` suffix when the last colon comes after the last slash. Three new unit tests cover full URL + bare path + the "colon in directory component" non-version edge case.

### Fix 2: Date/datetime values crashed JSON serialization

Bag rows come back from the SQLite mirror as Python `datetime.date` / `datetime.datetime` objects (via the type decorators in `deriva.bag.database`). `json.dumps` can't serialize those directly, and ERMrest expects ISO-8601 strings.

**Fix:** new `_coerce_datetimes` static method walks each row before POST and converts date/datetime values to `.isoformat()` strings. Two unit tests pin the behavior.

## Tests

`tests/deriva/bag/` goes 208 → **212 passing**, 2 skipped.

## Remaining work

deriva-ml#98's integration tests now reach the **third and final** upstream gap: the bag walker over-includes when anchored at a single RID. With `RIDAnchor(table="Dataset", rids=["5HE"])` the bag includes every `Dataset_Version` row, including rows that reference *other* Datasets (5HP, 5HY, …) that aren't in the bag. The loader's default `DanglingFKStrategy.FAIL` then aborts with `Dangling FK detected in Dataset_Version.Dataset`.

That's a producer-side design question (what does anchor-scoped semantics mean for transitive FK references?) and deserves its own discussion / PR. Not in scope here.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-219"></a>

## PR #219 — feat(bag): anchor-scoped FK-path query paths in CatalogBagBuilder

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/219
- **Head ref:** `feat/bag-walker-anchor-scoped-paths`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T01:21:29Z

## Summary

The bag walker now records the BFS-shortest FK path from an anchor to each reached table, and `_table_query_path` uses it to chain non-anchor table queries through the anchor's filter. Without this, a single `RIDAnchor` (e.g., one Dataset RID) pulled **every** row of **every** reached table — including rows that referenced other Datasets outside the anchored slice — which then produced dangling FKs at load time.

This was the third architectural issue surfaced by deriva-ml#98's integration tests (after the loader conflict policy #215, the clone_catalog `Column.sname` #216, and the asset upload + URL + date fixes #217 + #218).

## Before / after

**Before:**
```
/entity/deriva-ml:Dataset/RID=any(5HE)              # anchor (filtered)
/entity/deriva-ml:Dataset_Version                   # every row (wrong!)
```

**After:**
```
/entity/deriva-ml:Dataset/RID=any(5HE)
/entity/deriva-ml:Dataset/RID=any(5HE)/deriva-ml:Dataset_Version
```

ERMrest handles the natural-FK joins between path segments. The walker still records each table only once (BFS-shortest path), and `TableAnchor` (whole-table) anchors still get their unfiltered `/entity/...` query.

## What changed

- New `_table_paths: dict[(schema, table), list[(schema, table)]]` on the builder, populated by `_compute_reached_tables` as a side output of the BFS.
- New `_anchor_tables: set[(schema, table)]` to distinguish anchor-tables (RID-filtered) from FK-reached tables (chained-path).
- `_compute_reached_tables` refactored from `set.pop()` (non-deterministic) to a `deque` for true BFS ordering, so the recorded path is genuinely the shortest.
- `_enqueue_if_in_scope` removed (replaced by `_enqueue_if_in_scope_with_path` that carries the path prefix).

## Tests

Two new tests in `test_catalog_builder.py`:
- `test_spec_rid_anchor_chains_path_to_fk_reachable_tables` — two-table chain (`Dataset → Dataset_Version`)
- `test_spec_rid_anchor_chains_through_intermediate_table` — three-table chain (`Dataset → Dataset_Image → Image`)

`tests/deriva/bag/` goes from 212 → **214** passing. Existing single-anchor and `TableAnchor` tests still pass.

## Known limitations / remaining work

Composite FKs and multi-FK ambiguities aren't disambiguated in the generated path — ERMrest's natural-FK resolution chooses one. For the common deriva-ml topology (single-column FKs on RID, plus deriva-ml's `Name`-keyed vocab references) this is fine. Disambiguation hooks can be added when a real case needs them.

The integration tests in [deriva-ml#98](https://github.com/informatics-isi-edu/deriva-ml/pull/98) get *further* with this change (no more `Dataset_Version` dangling FKs) but now surface a separate issue: some vocab tables (notably `Asset_Type`) reached via FK aren't getting their CSVs emitted by the export engine, leaving downstream FK references with empty parent sets. That's an export-engine-shape question, tracked separately.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-220"></a>

## PR #220 — fix(bag): vocab FULL emits one unfiltered processor; coerce empty strings to NULL

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/220
- **Head ref:** `feat/bag-vocab-full-and-empty-null-coercion`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T02:51:41Z

## Summary

Two more fixes from the deriva-ml#98 integration run, both in the same area (loader+builder behavior the live tests surface).

## Fix 1: `vocab_export=FULL` was emitting duplicate processors

`CatalogBagBuilder._build_export_spec` always emitted the FK-bounded CSV processor for every reached table — and *additionally* emitted a full-table processor for vocab tables when `policy.vocab_export == FULL`. Both processors wrote to the same `output_path`; the export engine's resolution between them was unpredictable.

The intent of `FULL` is "the bag carries every term in this vocab, regardless of which the slice cites." Duplicate processors don't deliver that guarantee.

**Fix:** vocab + `FULL` → one unfiltered processor; vocab + `REFERENCED_ONLY` (default) → one FK-chained processor. No duplicates either way.

The clone-style use case wants `FULL` because the destination's child rows may reference any vocab term, not just the ones the BFS-shortest path happens to walk. The existing test that asserted "2 processors for the same vocab" was rewritten to assert the new (correct) "1 unfiltered processor" semantics.

## Fix 2: Empty-string FK values blocked the loader

The bag's CSVs serialize NULL as the empty string (BDBag/CSV convention has no native NULL sentinel). SQLite preserves the empty string verbatim, which then tripped:

- `_apply_dangling_fk_strategy`: `"'' not in parent {'X'}"` (FAIL)
- ERMrest's JSON ingest: it expects real NULL on nullable cols

**Fix:** new `_coerce_empty_to_null(table, row)` static method walks each row before the dangling-FK check, converting empty strings on nullable columns to `None`. Non-nullable columns are left alone — an empty string there is a real data error that should surface, not get silently nullified.

## Tests

Four new unit tests in `test_catalog_loader.py`:
- `test_coerce_empty_to_null_nullable_empty_string`
- `test_coerce_empty_to_null_skips_non_null_columns`
- `test_coerce_empty_to_null_leaves_real_values`
- `test_coerce_empty_to_null_does_not_mutate_input`

Plus the rewritten `test_spec_vocab_full_export_uses_unfiltered_query` in `test_catalog_builder.py` asserting the new vocab semantics.

`tests/deriva/bag/` goes **214 → 218** passing.

## Remaining work for the deriva-ml#98 integration tests

This unblocks two of the earlier dangling-FK failures (vocab and empty-string). The next layer is the **content-table FK cycle**: `Dataset ↔ Dataset_Version` form a real two-way cycle. `ForeignKeyOrderer` breaks it but the inserts then hit ERMrest with the cycle-FK pointing at a not-yet-inserted parent. That's a deferred-constraint / two-phase-insert design conversation, not a one-line fix. Tracked separately.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-222"></a>

## PR #222 — fix(bag): _validate_anchors uses ERMrest GET (datapath has no .in_)

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/222
- **Head ref:** `fix/bag-validate-anchors-ermrest`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T02:51:45Z

## Summary

The bag walker's anchor pre-flight tried `entity_path.RID.in_(rids)` to validate a multi-RID `RIDAnchor`. But deriva-py's datapath `_ColumnWrapper` only exposes `__eq__/__ne__/__lt__/...` predicate methods — there's no `in_`. Multi-RID anchors triggered:

```
AttributeError: '_ColumnWrapper' object has no attribute 'in_'
```

on the very first validation call.

## Fix

Drop the datapath API and issue the `RID=any(...)` filter directly via `self.catalog.get` against `/attribute/{schema}:{table}/RID=any(...)/RID`. ERMrest's `any()` filter is the same query expression the rest of the walker uses for filtered anchor-table queries; this just reuses it for the validation step.

## Impact

Single-RID anchors worked fine before (they hit the `RID == rid` branch). Multi-RID anchors are produced by:
- callers that bundle several RIDs into one anchor for slice export
- deriva-ml's `clone_via_bag` after nested-dataset expansion (a parent Dataset RID expands into the parent + all transitively-nested children)

Both now work.

## Tests

`tests/deriva/bag/` stays at 214 passing — no test regression. The live-catalog integration test in deriva-ml is what exercises this path; the unit-test fixtures all use single-RID anchors.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-223"></a>

## PR #223 — feat(bag): two-phase insert for FK cycles via deferred-column PUT

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/223
- **Head ref:** `feat/bag-loader-fk-cycle-deferred`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T02:56:00Z

## Summary

(Reopened from #221 — the original PR auto-closed when its base branch `feat/bag-vocab-full-and-empty-null-coercion` got deleted alongside the merge of #220. Same code, now rebased onto current `deriva-ml`.)

When `ForeignKeyOrderer` detects an FK cycle and drops an edge to make a topological sort possible, the dropped FK can't be satisfied at insert time — the target row hasn't landed yet. Today the loader sends the FK value anyway, and ERMrest rejects with `insert or update on table "X" violates foreign key constraint "Y"`.

This PR adds a two-phase insert path:

1. **First pass (insert).** For each row in a content table that has a cycle-cut FK, send the FK column as `NULL`. Save the original value keyed by RID.
2. **Second pass (PUT).** After every table has loaded, walk the saved values and `PUT /attributegroup/{schema}:{table}/RID;col` to fill them in. ERMrest now has both endpoint rows present, so the FK resolves.

**NOT-NULL guard:** if a cycle-cut FK lands on a NOT-NULL column, the first pass can't null it — raise a clear `ValueError` at load-time (`_init_cycle_deferred_state`) listing the offending columns, before any rows are sent.

## Surface additions

| File | Change |
|---|---|
| `loader.py` | `ForeignKeyOrderer.cycle_broken_edges()` returns `[(dependent_table, fk), ...]` — the edges the orderer cut. |
| `catalog_loader.py` | `_init_cycle_deferred_state` translates edges to per-table column sets and validates nullability. |
| `catalog_loader.py` | `_apply_deferred_fk_updates` runs the second-pass PUT after `arun`'s main loop finishes. |
| `catalog_loader.py` | `_load_content_table` strips deferred columns from each row before POST and stashes original values. |

## Tests

Six new unit tests:
- `test_orderer_cycle_broken_edges_reports_dropped_fk` — orderer exposes the dropped edges
- `test_orderer_cycle_broken_edges_empty_for_dag` — no cycles, no broken edges
- `test_loader_raises_when_cycle_fk_is_not_null` — NOT-NULL guard fires before insert
- `test_loader_defers_cycle_fks_and_patches_in_second_pass` — end-to-end first-pass NULL + second-pass PUT

`tests/deriva/bag/` goes from 218 → **222** passing.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-224"></a>

## PR #224 — feat(bag): multi-path emission — union FK routes to each target table

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/224
- **Head ref:** `carl/bag-multipath-emit`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T03:24:42Z

## Summary

Multi-path emission for `CatalogBagBuilder` plus consumer-side
union in `BagDatabase._load_data`. Together these unblock the
deriva-ml `clone-via-bag` integration tests that fail today
with `Dataset_Image.Image` dangling FKs: rows live on a long
FK route (`Dataset → Subject → Subject_Image → Image`) that
the BFS-shortest walker was silently dropping in favor of an
unpopulated short route (`Dataset → Dataset_File → File → Image`).

Two coherent commits, in this order:

1. **`fix(bag): BagDatabase unions multi-CSV-per-table at load time`** — index CSVs by qualified table name as `dict[str, list[Path]]` rather than `dict[str, Path]`. Multiple files per table are loaded sequentially with the existing `ON CONFLICT DO NOTHING` providing RID dedup. The legacy deriva-ml export spec generator has been emitting multi-CSV bags for years; this fixes the latent loader bug that picked one CSV per stem via `rglob` order.
2. **`feat(bag): CatalogBagBuilder emits one query_processor per FK route`** — walker records every distinct simple FK path per target (in a new `_table_path_set`, bounded by `_max_paths_per_table` = 16). `_build_export_spec` emits one CSV processor per `(table, path)` at a distinct `output_path` of the form `{schema}/{intermediate_chain}/{table}`. Vocab `FULL` and asset `fetch` processors remain single-emit per table — vocab is path-independent and fetch is RID-addressed.

## Test plan

- [x] `uv run pytest tests/deriva/bag/` — 224 passed, 2 skipped (same as `deriva-ml` baseline before this branch)
- [x] New unit test `test_bag_database_unions_multipath_csvs` — two `Parent.csv` files under different subdirs land all unique RIDs with the dup resolved
- [x] New unit test `test_spec_multipath_emits_one_processor_per_fk_route` — diamond topology emits two CSV processors with distinct query paths, plus zero per-route fetch processors
- [x] Existing tests updated for the new per-route `output_path` form (`demo/A/B` instead of `demo/B`)
- [ ] Will validate end-to-end against the deriva-ml `tests/catalog/test_clone_via_bag_integration.py` xfailed tests once this lands and the deriva-ml `uv.lock` is bumped

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-225"></a>

## PR #225 — feat(bag): FKTraversalPolicy.terminal_tables for vocab-style enter-don't-exit

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/225
- **Head ref:** `carl/bag-terminal-tables`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T04:14:59Z

## Summary

New ``FKTraversalPolicy.terminal_tables: set[tuple[str, str]]``
field. The walker treats listed tables the same way it already
treats vocabulary tables: enters them (so their rows ship in
the slice and downstream FKs resolve), but does **not** follow
outbound or inbound FKs from them.

This generalizes the vocab-terminal rule to non-vocab tables
that aggregate cross-anchor state. In the deriva-ml schema,
``Execution`` and ``Workflow`` are the canonical examples: a
single Execution row is referenced by every Subject, Image,
Dataset, and BoundingBox the workflow run touched. Without
terminal treatment, the walker entering Execution from one
anchor's path then walks out through every other anchor's
\*_Execution association — silently polluting the slice. The
legacy ``_schema_to_paths`` walker in deriva-ml has carried a
``_DEFAULT_SKIP_TABLES`` heuristic for years to handle this;
this PR lifts the idea into the bag walker's policy as a
first-class field.

Empty by default — callers opt in based on schema-domain
knowledge. The deriva-ml ``clone_via_bag`` will set ``Execution``
and ``Workflow`` as terminal in a follow-up deriva-ml PR.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 226 passed, 2 skipped (was 224/2)
- [x] New unit test ``test_terminal_table_enters_but_does_not_exit`` — Subject anchor with Execution as terminal: walker reaches Execution (provenance preserved) but does not continue through it to Image_Quality / Image (over-fetch prevented)
- [x] New unit test ``test_terminal_table_query_path_terminates_at_table`` — the CSV processor's query path for a terminal table ends at the table, never joins through it
- [ ] Will validate against the deriva-ml clone-via-bag integration tests once this lands and the deriva-ml lock is bumped

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-226"></a>

## PR #226 — fix(bag): terminal_tables block inbound only, still follow outbound

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/226
- **Head ref:** `carl/bag-terminal-asymmetry`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T04:30:18Z

## Summary

Follow-up to #225. The terminal-tables rule needs to be **asymmetric**:

- **Outbound** FKs (refs the terminal table declares) must still be followed so rows it references land in the slice. Without this, a terminal Execution row's ``Workflow`` FK ends up dangling.
- **Inbound** FKs (refs declared AT the terminal table by other tables) stay blocked. This is the over-fetch direction the rule exists to prevent.

Symptom in integration testing: ``Dangling FK detected in Execution.Workflow (value='49G' not in parent set())``. The Execution row was reaching the slice (correct), but Workflow wasn't — the walker had stopped fully at Execution.

Vocab tables remain fully terminal (both directions blocked) — vocab terms are leaves by design and have no outbound FKs to worry about.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 226 passed, 2 skipped
- [x] Renamed test ``test_terminal_table_blocks_inbound_but_follows_outbound`` extends the topology with an outbound FK (Execution → Workflow) plus an inbound chain (Execution ← Image_Quality → Image). Asserts Workflow lands in the slice, Image_Quality / Image do not.
- [ ] Will validate end-to-end against the deriva-ml clone-via-bag integration tests once this lands.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-227"></a>

## PR #227 — feat(bag): BagBuilder.add_asset link mode (hardlinks for transient staging)

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/227
- **Head ref:** `carl/bag-builder-add-asset-link`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T15:50:31Z

## Summary

Adds ``link: bool = False`` to ``BagBuilder.add_asset`` (and ``add_assets``). When ``link=True``, source files get a hardlink (``os.link``) into the bag at the profile-standard path instead of a copy. Same inode, second directory entry — zero bytes duplicated, zero extra I/O.

This is the deriva-py half of the bag-based ``commit_execution`` design (see deriva-ml's [docs/design/bag-based-commit-execution.md](https://github.com/informatics-isi-edu/deriva-ml/blob/main/docs/design/bag-based-commit-execution.md)). The bag-based commit pipeline needs the bag layout for ``BagCatalogLoader`` to consume, but the actual durable artifact is the destination catalog — the bag itself is throwaway. Copying multi-GB execution outputs was the cost objection that previously rejected the bag-based commit; hardlinks make it free.

### Why hardlinks rather than symlinks

bagit's ``_validate_bag_contents`` rejects manifest entries that resolve outside the bag root (``BagError("... is unsafe")``). Symlinks pointing at flat asset storage fail this check. Hardlinks live inside the bag tree as ordinary directory entries; bagit sees regular files and validation passes. MD5 manifest is correct because hashlib reads through the shared inode.

Cross-filesystem hardlinks raise ``OSError(EXDEV)``; the implementation catches this and falls back to ``shutil.copy2`` with a warning. Users with bag+source on different filesystems pay the copy cost transparently rather than seeing a confusing error.

### Backward compat

Default ``link=False`` unchanged. Existing callers (clone-via-bag, constructive bag-building tests) get the copy semantics they've always had. Hardlinks are opt-in for transient-staging use cases.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 232 passed, 2 skipped (was 226 / 2 before, +6 new tests)
- [x] ``test_builder_add_asset_link_mode_hardlinks_source`` — inode sharing pinned
- [x] ``test_builder_add_asset_link_mode_survives_source_deletion`` — hardlink keeps file alive after source unlink
- [x] ``test_builder_add_asset_link_mode_passes_bagit_validation`` — ``make_bdbag=True`` succeeds (this is the smoking-gun guard: symlinks fail here)
- [x] ``test_builder_add_asset_link_mode_md5_matches_content`` — manifest digest correct
- [x] ``test_builder_add_asset_link_mode_default_is_copy`` — backward-compat
- [x] ``test_builder_add_assets_bulk_link_mode`` — bulk-add propagates ``link=``

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-228"></a>

## PR #228 — feat(bag): DanglingFKStrategy.PRESERVE — trust destination for orphans

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/228
- **Head ref:** `carl/bag-dangling-preserve`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T15:50:35Z

## Summary

Adds a fourth ``DanglingFKStrategy`` value. ``PRESERVE`` short-circuits the bag-side parent-row check entirely: bag rows pass through verbatim with their FK columns unchanged, and any genuinely-missing parent surfaces as ERMrest's HTTP 409 at insert time.

This is the deriva-py companion to [#227](https://github.com/informatics-isi-edu/deriva-py/pull/227) — together they enable the bag-based ``commit_execution`` design (see deriva-ml's [docs/design/bag-based-commit-execution.md](https://github.com/informatics-isi-edu/deriva-ml/blob/main/docs/design/bag-based-commit-execution.md)).

### Use case

End-of-execution commit. The commit bag contains output rows (Image, BoundingBox, feature rows, ``*_Execution`` associations) whose ``Subject`` / ``Observation`` / ``Workflow`` FK targets already exist at the destination — they were created in a previous execution and weren't transitively walked into the commit bag.

A bag-side ``parent_rids`` check would flag them as dangling (the bag doesn't carry the parent rows), even though the references are perfectly valid at the destination. ``PRESERVE`` is the explicit "I know my bag references rows outside itself, and I trust the destination" choice.

### Implementation

One-line short-circuit at the top of ``_apply_dangling_fk_strategy``:

```python
if self.policy.dangling_fk_strategy == DanglingFKStrategy.PRESERVE:
    return rows, 0, 0  # trust destination
```

The "no orphans found" success path is reused so the loader's report aggregation doesn't need to special-case ``PRESERVE``.

### Backward compat

``PRESERVE`` is opt-in. The existing FAIL (default), DELETE, NULLIFY semantics are unchanged.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 227 passed, 2 skipped (was 226 / 2 before, +1 new test)
- [x] ``test_dangling_fk_strategy_preserve_short_circuits`` — locks in the verbatim pass-through: ``survivors == rows_in``, ``skipped == 0``, ``nullified == 0``, even when half the rows reference parents not in the bag

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-229"></a>

## PR #229 — feat(bag): FKTraversalPolicy.preserve_provenance — clone vs commit semantics

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/229
- **Head ref:** `carl/bag-preserve-provenance`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T16:25:29Z

## Summary

Adds ``preserve_provenance: bool = True`` to ``FKTraversalPolicy``. Controls whether ``_insert_rows`` opts out of ERMrest's default-fill for the source audit columns (``RCT``, ``RCB``).

This is the third deriva-py piece needed for bag-based ``commit_execution`` in deriva-ml (companion to #227 hardlink mode and #228 PRESERVE strategy).

### Two modes

- **True** (default — clone semantics): wire URL is ``?nondefaults=RID,RCT,RCB``. The bag's audit columns ride through to the destination. Matches the canonical clone paths.
- **False** (commit semantics): wire URL is ``?nondefaults=RID``. ``RID`` is preserved (callers lease it ahead of time); ``RCT`` and ``RCB`` get the destination's server-set defaults. Required when the bag carries newly-minted rows.

### How I found this

A proof-of-concept for bag-based ``commit_execution`` hit a ``409 Conflict`` inserting an Image row:

```
Image_RCB_fkey ... Key (RCB)=(_ermrest.current_client()) is not present in table "ERMrest_Client"
```

The bag had no ``RCB``/``RCT`` for the new row (they're not known at bag-build time), so the loader sent NULL with the clone-style ``nondefaults=RID,RCT,RCB`` URL — which blocked ERMrest from filling defaults, and the NULL ``RCB`` violated the FK constraint against ``public.ERMrest_Client``.

The existing FAIL/DELETE/NULLIFY/PRESERVE strategies don't touch insert semantics, only dangling-FK validation. ``preserve_provenance`` separates the two policy axes cleanly.

### Backward compat

Default behavior unchanged. The existing clone path (and all 233 existing tests) continue passing.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 235 passed, 2 skipped (was 233 / 2 before, +2 new tests)
- [x] ``test_insert_rows_preserve_provenance_default_sends_rct_rcb`` — backward-compat guard via mocked ``catalog.post`` URL inspection
- [x] ``test_insert_rows_preserve_provenance_false_sends_only_rid`` — commit mode emits the truncated nondefaults

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-230"></a>

## PR #230 — fix(bag): _localize_asset_row falls back to embedded-asset path

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/230
- **Head ref:** `carl/bag-localize-embedded-assets`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T16:35:27Z

## Summary

``BagBuilder.add_asset`` writes asset bytes to ``data/asset/{table}/{rid}/{filename}`` (the deriva-bag profile-standard embedded-asset path) but doesn't populate ``fetch.txt`` — there's no remote source URL for embedded bytes. The loader's ``_localize_asset_row`` only consulted ``fetch.txt`` via ``_build_asset_map``, so for constructive bags it left the ``Filename`` column holding the bare filename. ``_upload_assets`` then saw ``Path("bare-name.bin").is_file() == False`` and silently skipped the upload.

This is the broken half of the ``BagBuilder``↔``BagCatalogLoader`` round trip for embedded assets. Clone-via-bag works because ``CatalogBagBuilder`` writes ``fetch.txt`` entries; constructive bags built for end-of-execution commit don't have remote URLs and fail this lookup.

### The fix

When the URL isn't in ``asset_map`` but the row has a RID, try ``data/asset/{table}/{rid}/{filename}``. If the file exists, rewrite ``Filename`` to that absolute path.

### How I found this

Proof-of-concept for bag-based ``commit_execution`` in deriva-ml: the loader inserted the Image row successfully (#229 made that work), but the HEAD against the expected hatrac URL returned 404. Tracing showed ``_upload_assets`` saw a bare filename and skipped — the localization step had never replaced it.

### Backward compat

The new fallback only fires when ``asset_map`` doesn't have the URL. Existing clone-via-bag tests continue passing — they always have ``fetch.txt``, so the new code path is never reached.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 236 passed, 2 skipped (was 235 / 2 before, +1 new test)
- [x] ``test_bag_database_localizes_embedded_asset_without_fetch_txt`` — no ``fetch.txt``, bytes at ``data/asset/Image/I3/embedded.bin``, ``Filename`` comes out absolute-path-rewritten

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-231"></a>

## PR #231 — fix(bag): preserve Filename column; resolve bag-local path on demand

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/231
- **Head ref:** `carl/bag-asset-path-side-channel`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T16:45:29Z

## Summary

The asset-localization step in ``_localize_asset_row`` overwrote the row's ``Filename`` column in place with the bag-local path so ``_upload_assets`` could find the bytes. The rewritten path then leaked through to the catalog at insert time — destinations stored ``/private/var/.../bag/data/asset/Image/RID/file.bin`` as the Filename instead of the bare ``file.bin``.

The bug pre-existed in the clone-via-bag flow (clone tests check row counts, not Filename equality) but was newly visible in the bag-based ``commit_execution`` POC, which compares Filename against the original value the caller registered.

### Fix

Stop rewriting ``Filename``. Move the bag-local-path resolution into a new ``BagDatabase.resolve_asset_local_path(table_name, row)`` method that the loader's ``_upload_assets`` calls lazily, per asset row. The SQLite mirror preserves ``Filename`` verbatim; the destination catalog receives the catalog-facing filename; only the in-process upload step ever sees the bag-local path.

### Two lookup strategies (unchanged)

1. ``fetch.txt``-based ``asset_map`` (URL → local path) for clone/MINID bags.
2. Profile-standard embedded-asset path ``data/asset/{table}/{rid}/{filename}`` for constructive bags built via ``BagBuilder.add_asset``.

Both strategies live inside ``resolve_asset_local_path`` instead of inside ``_localize_asset_row``, which is now a documented no-op for call-site compatibility (will be pruned in a follow-up).

### Side effect on clone-via-bag

Clone targets that previously got an absolute bag-local path in their ``Filename`` column will now get the source's bare filename — the catalog-facing value. This is the correct behavior; the existing clone-via-bag integration tests in deriva-ml don't assert Filename and continue passing.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 236 passed, 2 skipped (same count as before)
- [x] Three existing localization tests updated to assert the new contract: ``rows[0]["Filename"]`` equals the source value verbatim, ``db.resolve_asset_local_path("Image", rows[0])`` returns the bag-local path

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-232"></a>

## PR #232 — fix(bag): SchemaBuilder.make_table_name normalises hyphens

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/232
- **Head ref:** `carl/bag-automap-hyphen-fix`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T16:57:56Z

## Summary

In in-memory mode (``database_path=":memory:"``, used by ``BagBuilder``), ``SchemaBuilder`` folds schema names into the SQLAlchemy table name and applies ``replace("-", "_")`` so the result is a valid Python identifier. But the cross-schema FK wiring pass uses ``make_table_name`` for the lookup, and that helper did NOT apply the transform — schemas like ``test-schema`` and ``deriva-ml`` (real names in deriva-ml's schema) generated SQLTables under ``test_schema_Image`` but the FK wiring tried to look up ``test-schema_Image``. KeyError, automap aborts mid-build.

This blocked using ``BagBuilder`` with deriva-ml's schema document. Hand-built bags work; this PR makes ``BagBuilder`` work too, which is the cleaner abstraction for the upcoming bag-based ``commit_execution``.

### Fix

One-line: apply the same ``replace("-", "_")`` transform inside ``make_table_name`` that the SQLTable-creation site does. All three name-construction sites in ``schema.py`` (line 760 new, line 791 SQLTable name, line 822 FK ref column) now agree.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 237 passed, 2 skipped (was 236 / 2 before, +1 new test)
- [x] ``test_schemabuilder_in_memory_resolves_cross_schema_fk_with_hyphen`` — hyphenated source + destination schemas plus a cross-schema FK; the build completes and both folded table names land in ``orm.list_tables()``

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-233"></a>

## PR #233 — fix(bag): _make_bdbag pre-seeds bagit scaffolding to avoid double-data/

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/233
- **Head ref:** `carl/bag-builder-no-data-prefix`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T17:06:15Z

## Summary

``BagBuilder.finalize(make_bdbag=True)`` produced a doubly-nested ``data/data/`` payload tree because bdbag's ``make_bag(update=False)`` (create path) moves everything into a fresh ``data/`` via ``bagit.make_bag``. ``BagBuilder`` already writes ``data/schema.json`` and ``data/asset/...`` directly, so when bdbag's create path ran on top, the existing ``data/`` ended up at ``data/data/``.

The ``update=True`` kwarg we were already passing only enters the update branch when ``BDBag(path)`` constructs successfully — which requires ``bagit.txt`` to exist. ``BagBuilder`` didn't write that, so ``BDBag`` raised, ``bag = None``, and the code fell through to the create branch.

### Fix

``_make_bdbag`` now pre-seeds the minimal bagit scaffolding (``bagit.txt``, ``bag-info.txt``, ``manifest-{md5,sha256}.txt`` all empty) before calling ``bdb.make_bag``. ``BDBag(path)`` succeeds, bdbag takes the update path, and the payload tree stays where ``BagBuilder`` put it. The update path's ``bag.save()`` then regenerates manifests with real content.

### How I found this

Proof-of-concept for bag-based ``commit_execution``: hand-built bags worked, but switching to ``BagBuilder.finalize(make_bdbag=True)`` produced an asset at ``bag/data/data/asset/Image/RID/file.bin`` that the post-finalize hardlink-existence assertion couldn't find.

Surprisingly, the pre-existing ``test_builder_finalize_make_bdbag=True`` tests didn't catch this — they only check ``manifest-md5.txt`` content, never the actual payload-tree layout.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 238 passed, 2 skipped (was 237 / 2 before, +1 new test)
- [x] ``test_builder_finalize_make_bdbag_preserves_data_layout`` — schema.json at ``data/schema.json``, no ``data/data/`` subdir, bagit scaffolding at the bag root

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-234"></a>

## PR #234 — fix(bag): SQLite mirror relaxes NOT-NULL on non-PK columns

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/234
- **Head ref:** `carl/bag-mirror-skip-system-cols`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T17:13:13Z

## Summary

The bag's SQLite mirror inherited every catalog column's ``nullok`` flag directly, which meant catalog NOT-NULL columns with server-side defaults (``RCT``, ``RCB``, ``RMT``, ``RMB``) showed up as NOT-NULL in the mirror too. Rows that legitimately lacked those values — because the destination ERMrest would fill them in at insert time — failed the mirror's local constraint instead of getting through.

### How I found this

The bag-based ``commit_execution`` POC, switched to use ``BagBuilder``: passed an Image row without ``RCT``/``RCB``, ``BagDatabase._load_data`` rejected the insert with ``sqlite3.IntegrityError: NOT NULL constraint failed: Image.RCT``.

### Fix

The mirror is **staging**, not a fidelity copy of catalog constraints. Make every **non-PK** column nullable so rows that will be server-defaulted can land in the mirror. The destination's ERMrest endpoint is the authoritative validator at insert time.

PK columns keep their NOT-NULL: that's a real row-construction-bug detector and PKs never have server-set defaults.

### Backward compat

The existing bag tests pass (clone bags never had this problem because clone-via-bag's source CSVs include the audit-column values). The new behavior is strictly more permissive.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 239 passed, 2 skipped (was 238 / 2 before, +1 new test)
- [x] ``test_schemabuilder_non_pk_columns_nullable_in_mirror`` — pins the rule: PK NOT-NULL preserved, non-PK NOT-NULL relaxed

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-235"></a>

## PR #235 — fix(bag): ermrest_json_to_metadata also relaxes non-PK NOT-NULL

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/235
- **Head ref:** `carl/bag-ermrest-json-relax-not-null`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T17:18:45Z

## Summary

Companion to #234 (``SchemaBuilder`` mirror relaxation). ``BagBuilder`` uses ``ermrest_json_to_metadata`` to build the in-memory ``SchemaORM`` that backs ``_write_pending_rows`` — NOT ``SchemaBuilder``. The two factories produce SQLAlchemy metadata for the same purpose (bag staging) but #234 only fixed one of them. Result: even after #234, ``BagBuilder.add_row`` calls for rows missing ``RCT``/``RCB`` still failed with ``sqlite3.IntegrityError: NOT NULL constraint failed``.

### Fix

Apply the same rule to ``ermrest_json_to_metadata``:
- PK columns keep their NOT-NULL constraint.
- Non-PK columns are nullable in the mirror regardless of the catalog's ``nullok`` flag.

Plus a round-trip-preservation safeguard: stash the catalog's authoritative ``nullok`` in ``col.info["nullok"]`` at construction; read from there in ``metadata_to_ermrest_json`` instead of from ``col.nullable``. Without this, a doc → metadata → doc round-trip would lose the catalog's NOT-NULL for non-PK columns.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 241 passed, 2 skipped (was 239 / 2 before, +2 new)
- [x] ``test_ermrest_json_to_metadata_relaxes_non_pk_not_null`` — PK NOT-NULL preserved, non-PK relaxed, ``col.info["nullok"]`` carries original
- [x] ``test_ermrest_json_to_metadata_roundtrip_preserves_nullok`` — doc → metadata → doc preserves the catalog's NOT-NULL on non-PK columns

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-236"></a>

## PR #236 — fix(bag): BagDatabase mirror also relaxes non-PK NOT-NULL

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/236
- **Head ref:** `carl/bag-database-relax-not-null`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T17:26:09Z

## Summary

Completes the trio with #234 (``SchemaBuilder``) and #235 (``ermrest_json_to_metadata``). ``BagDatabase._create_tables`` has its own column-construction loop — duplicated logic that each location maintained separately. After #234 / #235 the in-memory and SchemaBuilder paths were fixed, but ``BagDatabase`` (the path that loads already-built bags from disk) still inherited the catalog's ``nullok`` verbatim.

### How I found this

Re-running the bag-based ``commit_execution`` POC after #235 still failed with ``sqlite3.IntegrityError: NOT NULL constraint failed: Image.RCT`` — but the failing INSERT was inside ``BagDatabase._load_data``, not the BagBuilder side. Three duplicated implementations; this PR is the last one.

### Fix

Same rule as #234 / #235:
- PK column keeps its NOT-NULL constraint.
- Non-PK columns nullable in the mirror regardless of catalog ``nullok``.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 242 passed, 2 skipped (was 241 / 2 before, +1 new test)
- [x] ``test_bag_database_relaxes_non_pk_not_null`` — RID NOT-NULL preserved, RCT/Name (catalog ``nullok=False``) come out nullable

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-237"></a>

## PR #237 — fix(bag): preserve_provenance=False strips RCT/RCB/RMT/RMB from row body

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/237
- **Head ref:** `carl/bag-coerce-empty-when-not-preserving`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T17:33:00Z

## Summary

Symmetrical counterpart to the ``nondefaults=RID`` URL change in #229. Commit-mode inserts now strip the system-audit columns from the row dict entirely so empty-string values the bag's CSV carried (the BDBag/CSV NULL sentinel) don't reach ERMrest as ``""``, which would 400 with ``invalid input syntax for type timestamp with time zone: ""``.

### The wire-format contract for ``preserve_provenance=False``

- **URL**: ``?nondefaults=RID`` (only RID is caller-supplied; RCT/RCB/RMT/RMB get server defaults).
- **Body**: rows have no RCT/RCB/RMT/RMB keys at all. ERMrest populates ``RCT`` (``now()``), ``RCB`` (current user), and the RMT/RMB defaults from the server.

### How I found this

Bag-based ``commit_execution`` POC: after the SQLite mirror's NOT-NULL relaxation (#234–#236) let rows land in the mirror, the wire body still carried ``"RCT": ""`` and ``"RCB": ""`` from the CSV-empty-string serialization, and the destination's ERMrest rejected them with a 400.

### Backward compat

Default behavior (``preserve_provenance=True``) unchanged — audit columns ride through verbatim for clone semantics.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 244 passed, 2 skipped (was 242 / 2 before, +2 new)
- [x] ``test_insert_rows_preserve_provenance_false_strips_system_columns`` — system columns stripped from POSTed body
- [x] ``test_insert_rows_preserve_provenance_true_keeps_system_columns`` — clone-mode keeps RCT/RCB verbatim

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-238"></a>

## PR #238 — feat(bag): schema_io round-trips annotations, ACLs, snaptime

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/238
- **Head ref:** `carl/bag-schema-io-annotations`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T17:41:31Z

## Summary

``ermrest_json_to_metadata`` and ``metadata_to_ermrest_json`` were lossy on ERMrest-specific metadata: column / table / schema annotations, ACLs, ACL bindings, and the document-level snaptime all got dropped on a json → metadata → json round-trip. Downstream consumers like ``Table.is_asset()`` (which checks for ``tag.asset`` on the URL column) saw asset tables as ordinary content tables after the round-trip.

### How I found this

Bag-based ``commit_execution`` POC: ``BagBuilder`` writes its bag's ``schema.json`` by running ``metadata_to_ermrest_json``, then ``BagCatalogLoader._load_table`` calls ``table.is_asset()`` on the re-parsed model. With ``tag.asset`` stripped, ``is_asset`` returned False, ``_upload_assets`` never ran, and the bag's asset bytes never reached hatrac.

### Fix

Same approach as the ``nullok`` round-trip fix in #235: stash the lossy values on SQLAlchemy ``info``:
- ``Column.info``: column-level ``annotations``, ``acls``, ``acl_bindings``.
- ``Table.info``: table-level ``annotations``, ``acls``, ``acl_bindings``, ``comment``.
- ``MetaData.info["schemas"][name]``: schema-level metadata.
- ``MetaData.info["snaptime"]``: document-level snaptime.

``metadata_to_ermrest_json`` reads them back on the way out, emitting each key only if present in the source.

### Backward compat

Callers that build SQLAlchemy metadata directly (without going through ``ermrest_json_to_metadata``) emit no annotations, same as before.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 246 passed, 2 skipped (was 244 / 2 before, +2 new)
- [x] ``test_ermrest_json_to_metadata_preserves_column_annotations`` — pins the column-level stash
- [x] ``test_ermrest_json_to_metadata_roundtrip_preserves_annotations`` — pins the full round-trip across all three levels plus snaptime

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-240"></a>

## PR #240 — fix(bag): schema_io preserves int2/int4/int8 distinction on round-trip

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/240
- **Head ref:** `carl/bag-schema-io-int-typename`
- **State at squash:** MERGED
- **Merged at:** 2026-05-12T17:54:45Z

## Summary

ERMrest's ``int2``, ``int4``, and ``int8`` all map to one SQLAlchemy column type (``StringToInteger``), so a naive ``metadata_to_ermrest_json`` emits ``int4`` for any of them. ``int8`` becomes ``int4`` and the distinction is lost.

### How I found this

After #238 landed, the bag's ``schema.json`` carried the ``tag.asset`` annotation correctly, but the bag-based ``commit_execution`` POC still saw ``_upload_assets`` skip. Trace: ``Table.is_asset()`` exact-matches ``int8`` for the ``Length`` column. After a json → metadata → json round-trip, ``Length`` came out as ``int4``, ``is_asset`` returned False.

### Fix

Stash the original ERMrest typename on ``col.info["ermrest_typename"]`` at read time; prefer it at write time. Falls back to the existing ``sql_type_to_ermrest_name`` for callers that built metadata directly (no info stash).

### Backward compat

Default callers (no info stash) see exactly the previous behavior. Round-trippers through ``ermrest_json_to_metadata`` get type fidelity.

This PR replaces #239 (which was based on a pre-#238 HEAD and showed phantom conflicts after #238 squash-merged with a different hash). Same content, fresh cherry-pick onto current deriva-ml.

## Test plan

- [x] ``uv run pytest tests/deriva/bag/`` — 247 passed, 2 skipped (was 246 / 2 before, +1 new)
- [x] ``test_ermrest_json_to_metadata_roundtrip_preserves_int8`` — pins ``int8`` and ``int2`` round-trip correctly (not downgraded to ``int4``)

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-241"></a>

## PR #241 — feat(bag): BagCatalogLoader.run() handles notebook event loops

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/241
- **Head ref:** `carl/bag-loader-notebook-loop`
- **State at squash:** MERGED
- **Merged at:** 2026-05-13T00:20:02Z

## Summary

- ``run()`` is the sync entry point into ``BagCatalogLoader``. The previous one-line ``return asyncio.run(self.arun())`` raised ``RuntimeError: asyncio.run() cannot be called from a running event loop`` when invoked from Jupyter / papermill kernels (the kernel owns the main-thread loop). Sync callers had to wrap ``arun`` themselves with ``nest_asyncio`` + loop detection.
- Now ``run()`` does the loop detection itself: outside a loop, plain ``asyncio.run`` (unchanged); inside a loop, ``nest_asyncio.apply()`` + ``loop.run_until_complete``. ``nest_asyncio`` is imported lazily — the bag module stays import-safe without it; the cost only appears for in-notebook callers and is documented in the ``run()`` docstring.

This obsoletes a helper in deriva-ml that does the same dance manually. Filed as part of an audit-driven series of upstream fixes that came out of [deriva-ml#104](https://github.com/informatics-isi-edu/deriva-ml/pull/104).

## Test plan

- [x] ``test_run_outside_event_loop_uses_asyncio_run`` — plain sync caller; ``arun`` stubbed to a coroutine returning a sentinel ``LoadReport``; verifies ``run()`` returns it. Confirms the no-loop path is unchanged.
- [x] ``test_run_inside_event_loop_uses_nest_asyncio`` — drives ``run()`` from inside ``async def`` so ``loop.is_running()`` is True; verifies the sentinel comes back. Skips when ``nest_asyncio`` isn't installed (it's a soft dep, same pattern as the rest of the bag module's runtime imports).
- [x] Full ``test_catalog_loader.py`` suite — 45/45 pass.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-242"></a>

## PR #242 — feat(datapath): _ColumnWrapper.in_(values) operator

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/242
- **Head ref:** `carl/datapath-column-in`
- **State at squash:** MERGED
- **Merged at:** 2026-05-13T00:20:05Z

## Summary

Adds a first-class membership operator on column wrappers so callers can write ``col.in_(["a", "b", "c"])`` instead of hand-rolling ``reduce(operator.or_, (col == v for v in values))``. Wire format unchanged — same ``;``-separated equality disjunction the manual idiom produces today.

Semantics chosen to fail loudly on caller bugs rather than silently produce surprising filters:

- **Empty iterable** raises ``ValueError``. ERMrest has no "always false" filter; an empty list almost always means a caller bug, not "silently match everything or nothing" (which is itself ambiguous).
- **``None`` in the values** raises ``TypeError``. NULL matching isn't a membership test; use ``eq(None)`` explicitly.
- **Single-element list** collapses to a plain equality predicate — no disjunction overhead in the URL.
- **Generators / iterators** are exhausted into a list internally so callers don't have to materialise upfront.
- **N>1 values** produce ``_DisjunctionPredicate`` over N equality predicates — same shape ``|`` (``_Predicate.__or__``) builds from two predicates today.

Part of the audit-driven series following [deriva-ml#104](https://github.com/informatics-isi-edu/deriva-ml/pull/104). With this in place, deriva-ml can replace two ``reduce(operator.or_, ...)`` blocks with one-line ``col.in_(values)`` calls.

## Test plan

- [x] ``InPredicateConstructionTests`` (unit, no live catalog): 6 cases — multi-value shape, single-value collapse, empty raises, None raises, generator support, URL fragment shape. All pass locally.
- [x] ``test_filter_in_*`` (live catalog, matches the existing live-test pattern in this file): 4 cases — multi-value match count, single-value collapse, empty/None error contract. Needs ``DERIVA_PY_TEST_HOSTNAME`` to run.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-243"></a>

## PR #243 — feat(bag): hatrac_url_for helper for canonical asset URLs

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/243
- **Head ref:** `carl/bag-hatrac-url-helper`
- **State at squash:** MERGED
- **Merged at:** 2026-05-13T00:20:09Z

## Summary

The convention ``/hatrac/{table}/{md5}.{filename}`` is implicit but fixed across deriva-py's upload pipeline and deriva-ml's bag-commit path. Multiple callers — the per-execution bag commit, the localization fallback, the upload-spec defaults — currently hand-roll the same f-string. This PR promotes it to a public ``hatrac_url_for(table, md5, filename)`` in ``deriva.bag.builder`` so a future tweak (URI-encoding, hostname prefix, scheme switch) happens in one place.

The shape mirrors the default ``hatrac_uri`` template used by ``GenericUploader`` via the upload-spec dict.

Companion to [#241](https://github.com/informatics-isi-edu/deriva-py/pull/241) (loader nest_asyncio fallback) and [#242](https://github.com/informatics-isi-edu/deriva-py/pull/242) (datapath ``_ColumnWrapper.in_``) — same audit-driven series following [deriva-ml#104](https://github.com/informatics-isi-edu/deriva-ml/pull/104).

## Test plan

- [x] ``test_hatrac_url_for_canonical_shape`` — basic table/md5/filename.
- [x] ``test_hatrac_url_for_preserves_filename_dots`` — ``uv.lock`` and similar dotted names round-trip cleanly. The convention puts MD5 first, filename second, joined by ``.``, and filenames with their own internal dots must pass through unchanged so the upload's content-disposition reconstructs the original name.
- [x] ``test_hatrac_url_for_table_with_underscores`` — asset-table names like ``Execution_Asset`` don't get URL-encoded.

All 3 new tests pass. Full ``test_builder.py`` suite: 32 pass, 1 skipped (pre-existing).

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-244"></a>

## PR #244 — docs: scoping note for column-construction dedup

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/244
- **Head ref:** `carl/design-column-construction-dedup`
- **State at squash:** MERGED
- **Merged at:** 2026-05-13T00:20:13Z

## Summary

Adds ``docs/design/column-construction-dedup.md`` — a scoping note that maps the three places in ``deriva.bag`` that turn an ERMrest column JSON into a SQLAlchemy ``Column`` (``SchemaBuilder``, ``BagDatabase``, ``schema_io.ermrest_json_to_metadata``), identifies what's shared, what's intentionally different, and what's accidental drift.

Recommends promoting the type map and ``_is_key_column`` to a shared module (small upstream-friendly refactor, defensive correctness fix), but explicitly says **not** to unify the column-creation loop — the ``col.info``-stashing path in ``schema_io`` is materially different and the three call sites have different orchestration concerns.

Originally drafted on the deriva-ml side during an audit that ran while the bag-based ``commit_execution`` work was landing. Moved here because the implementation lives in this repo and the doc should travel with the code it scopes.

No code changes.

## Test plan

- [x] Docs-only — nothing to test.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-245"></a>

## PR #245 — feat(bag): FKTraversalPolicy.match_by_columns

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/245
- **Head ref:** `carl/bag-match-by-columns`
- **State at squash:** MERGED
- **Merged at:** 2026-05-13T00:25:39Z

## Summary

Generalises the existing vocabulary match-by-``Name`` path to non-vocabulary tables that have a content-addressed unique key (e.g. asset tables whose ``URL`` is hash-derived and stable across executions). Callers can now say "for this table, if a row with these column values already exists on the destination, skip the insert and remap the source RID to the existing RID."

Three pieces:

1. **``FKTraversalPolicy.match_by_columns: dict[(schema, table), list[str]]``** — per-table caller-supplied unique-key rule. A validator rejects empty column lists (caller bug; drop the entry instead of supplying ``[]``).

2. **``_TableClass.MATCH_BY_COLUMNS``** — new classifier variant. ``_classify_table`` checks the policy dict first; it takes precedence over the structural vocab check, since explicit caller intent overrides implicit classification.

3. **``BagCatalogLoader._load_match_by_columns_table``** + helper ``_fetch_existing_by_columns``. Same shape as the vocab pair (``_load_vocabulary_table`` + ``_fetch_existing_vocab_by_name``), parameterised by the caller's column list. Row's match-key is a tuple over those columns; rows whose key has any ``None`` component fall through to insert (composite-NULL semantics are ambiguous — let any destination unique constraint fire on its own terms).

   The remap shape (``_rid_remap[(schema, table)][src_rid] = dst_rid``) is identical to the vocab path, so existing ``_rewrite_fks`` handles child-row rewriting with no changes.

``TableLoadStats.rows_matched_by_columns`` reports the count parallel to ``rows_matched_by_name``.

Companion to [#241](https://github.com/informatics-isi-edu/deriva-py/pull/241), [#242](https://github.com/informatics-isi-edu/deriva-py/pull/242), [#243](https://github.com/informatics-isi-edu/deriva-py/pull/243), [#244](https://github.com/informatics-isi-edu/deriva-py/pull/244). Together they obsolete ~200 LoC of URL-dedup helpers in deriva-ml's ``bag_commit.py`` ([deriva-ml#104](https://github.com/informatics-isi-edu/deriva-ml/pull/104)).

## Test plan

- [x] ``test_classify_table_routes_through_match_by_columns`` — policy override of structural vocab classification.
- [x] ``test_match_by_columns_matches_existing_and_records_remap`` — end-to-end: existing row matched, new row inserted, child FK rewritten through remap. Mirrors the vocab e2e test.
- [x] ``test_match_by_columns_composite_key`` — multi-column match key works.
- [x] ``test_match_by_columns_null_in_key_falls_through`` — NULL in any match column → insert, not match.
- [x] ``test_match_by_columns_policy_rejects_empty_column_list`` — validator catches the caller bug.

All 5 new tests pass. Full ``test_catalog_loader.py`` + ``test_traversal.py``: 63/63 pass.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-246"></a>

## PR #246 — fix(bag): _rewrite_fks before insert in vocab + match_by_columns paths

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/246
- **Head ref:** `carl/bag-loader-rewrite-fks-in-vocab-and-match`
- **State at squash:** MERGED
- **Merged at:** 2026-05-13T00:39:51Z

## Summary

``_load_vocabulary_table`` and ``_load_match_by_columns_table`` both build a row remap (source RID → destination RID for matched rows, identity for new rows) and then insert the non-matched rows. They were missing the ``_rewrite_fks`` pass that ``_load_content_table`` runs before its own insert, so any FK column on an inserted row carried its bag-side source RID verbatim instead of being remapped through the loader's table.

This was **latent on the vocab path** (vocab tables are typically FK-terminal in real schemas), but **bit hard on ``match_by_columns``** because the common use case is association tables (e.g. ``{Asset}_Asset_Type``) whose rows FK into asset tables that were themselves deduped on the same load. The association row's FK column needs the same rewrite the content path applies — otherwise the destination's FK constraint fires with 409.

**Surfaced from a deriva-ml integration test** — the bag-commit refactor's cleanup pass started routing ``{Asset}_Asset_Type`` association tables through ``match_by_columns`` (so the loader handles the ``(asset_rid, Asset_Type)`` composite-key dedup instead of a hand-rolled pre-flight in deriva-ml), and the missing FK rewrite immediately showed up as ``Key (Execution_Metadata)=(4AT) is not present in table "Execution_Metadata"``.

## Fix

Both paths run ``[self._rewrite_fks(table, row) for row in new_rows]`` immediately before ``_insert_rows``, matching the content path's behaviour.

## Test plan

- [x] New ``test_match_by_columns_rewrites_fks_on_inserted_rows`` regression test: sets up an Image table deduped by URL (one match → one remap) and a Widget table also deduped by ``match_by_columns`` whose composite match-key forces every row to insert. Without the fix the Widget row's ``Image`` FK arrives as ``I-SRC-A`` (bag-side) and ERMrest 409s. With the fix the rewrite turns it into ``I-DST-A``.
- [x] Full bag suite: 258 pass, 2 skip.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-247"></a>

## PR #247 — fix(bag): _rewrite_fks before match query in _load_match_by_columns_table

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/247
- **Head ref:** `carl/bag-match-by-columns-rewrite-before-match`
- **State at squash:** MERGED
- **Merged at:** 2026-05-13T00:45:22Z

## Summary

Follow-up to [#246](https://github.com/informatics-isi-edu/deriva-py/pull/246). ``_load_match_by_columns_table`` ran the match query using the bag-side row's FK column values verbatim, then ran ``_rewrite_fks`` only on the unmatched rows that fell through to insert.

That mis-ordering broke the common case where the match key includes an FK column whose target table was deduped on the **same load**:

- Asset table deduped — ``Image.URL`` matches; remap is ``I-SRC-A → I-DST-A``.
- ``{Asset}_Asset_Type`` deduped — match key is ``(Image, Asset_Type)``. The bag row carries ``(I-SRC-A, Type-X)``. The match query asked "is there a row with ``Image=I-SRC-A``?" — no (the destination has the same semantic row under ``Image=I-DST-A``). The bag row fell through to insert.
- Insert: ``_rewrite_fks`` turned the FK into ``I-DST-A``, then ERMrest 409'd on the already-existing composite key ``(I-DST-A, Type-X)``.

## Fix

One line: ``rows = [self._rewrite_fks(table, row) for row in rows]`` at the **top** of the loop. The match query then asks the right question (against destination RIDs).

## Test plan

- [x] New ``test_match_by_columns_rewrites_fks_before_match_query``: exercises the exact mis-match scenario above. Without the fix the match misses; with the fix ``rows_matched_by_columns`` goes up by 1.
- [x] All 7 ``match_by_columns`` tests still pass.
- [x] Full bag suite: 259 pass, 2 skip.

Surfaced from the same deriva-ml integration test that produced #246. Together they make ``match_by_columns`` usable on association tables whose FK targets were themselves deduped on the same load.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-248"></a>

## PR #248 — refactor(bag): column-construction dedup — single source of truth

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/248
- **Head ref:** `carl/bag-column-types-shared`
- **State at squash:** MERGED
- **Merged at:** 2026-05-13T01:31:28Z

## Summary

Implements the column-construction dedup that [#244](https://github.com/informatics-isi-edu/deriva-py/pull/244)'s scoping note recommended. Three places in ``deriva.bag`` (``SchemaBuilder``, ``BagDatabase``, ``schema_io.ermrest_json_to_metadata``) used to carry their own copy of the ERMrest-typename → SQLAlchemy-type map and their own ``_is_key_column`` rule. ``schema_io`` had the 19-entry superset; the other two had 12-entry subsets that silently downgraded ``text`` / ``longtext`` / ``markdown`` / ``ermrest_*`` typenames to plain ``String``.

The design note explicitly recommended **promoting the type map and PK rule to a shared module without unifying the column-creation loop** — the ``col.info``-stashing path in ``schema_io`` is materially different and the three call sites have different orchestration concerns. This PR does exactly that.

## Changes

**New module ``deriva/bag/_column_types.py``** — single source of truth for:

- Five CSV-to-typed-value decorator classes (``ERMRestBoolean``, ``StringToFloat``, ``StringToInteger``, ``StringToDateTime``, ``StringToDate``). Moved from ``database.py``; that module re-exports them so existing callers (including the historical ``deriva.core.bag_database`` shim) keep working.
- ``ERMREST_TO_SQL`` — the 19-entry typename → type map. ``schema_io`` re-exports the symbol for back-compat.
- ``sql_type_for_ermrest(deriva_type)`` — map lookup with ``String`` fallback.
- ``is_key_column(column, table)`` — the ``RID``-is-PK rule.

**Three call sites switched**:

- ``SchemaBuilder``: dropped ``_TYPE_MAP``, ``_sql_type``, ``_is_key_column`` (~50 LoC). Public API unchanged.
- ``BagDatabase``: dropped the five inline decorator classes (~60 LoC moved upstream), the inline type dict, and ``_is_key_column``. Re-exports the decorators.
- ``schema_io``: ``ERMREST_TO_SQL`` is now the same object as the canonical map (verified by a cross-module identity test).

## Behaviour change

``SchemaBuilder`` and ``BagDatabase`` now recognise six additional typenames they previously downgraded to ``String``: ``text``, ``longtext``, ``markdown``, ``ermrest_rid``, ``ermrest_rct``/``rmt``/``rcb``/``rmb``. No backwards-incompatible effect — the new types still serialize the same bytes through the CSV round-trip; the difference shows up only when a caller inspects the SQLAlchemy column's type class.

## Test plan

- [x] New ``tests/deriva/bag/test_column_types.py`` — 11 tests covering: 19-typename coverage, decorator routing, text/JSON plain types, unknown-type ``String`` fallback, ``is_key_column`` True for RID-keyed columns and False otherwise, plus three cross-module identity tests pinning that re-exports point at the canonical objects (no accidental duplicate classes).
- [x] Full bag suite: 270 pass, 2 skip (was 259 — +11 new tests).
- [x] Full deriva-py sweep (excluding env-broken ``test_hatrac_store.py`` and ``test_pre_allocated_rid.py``, both pre-existing ``distutils``/``hatrac`` import errors): 442 pass, 200 env-skipped, 9 env-blocked errors. None touch the bag pipeline.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---

<a id="pr-249"></a>

## PR #249 — refactor(bag): audit cleanup sprint 2026-05 (13 of 15 actions)

- **GitHub:** https://github.com/informatics-isi-edu/deriva-py/pull/249
- **Head ref:** `carl/bag-audit-cleanup-2026-05`
- **State at squash:** OPEN
- **Merged at:** (unmerged at squash time)

## Summary

Single PR consolidating the senior-engineer audit cleanup sprint for the `deriva.bag` package. Covers **13 of 15** ranked actions from `docs/design/bag-package-audit-2026-05.md`. Mechanical / low-risk only — Action 11 (BagDatabase row-load refactor) and Action 13 (asset-uploader extraction) are deferred to dedicated follow-up sessions per the audit's classification.

Branch was cut after #248 merged. Four commits:

| Commit | Scope |
|---|---|
| `9b4ee81` docs(audit) | The audit report itself (612-line markdown) |
| `2277c28` actions 1–7 | Dead imports, docstring fixes, schema_io privatization, `_class_prefix` removal, `_SYSTEM_SCHEMAS` alias removal, `_validate_anchors` datapath migration |
| `9e16434` actions 8–10 | Composite-FK warn-once, cycle-break raises on depth exhaust, shared `_orm_helpers` module |
| `ece3f2e` actions 12, 14, 15 | `Model(...)` direct construction (no tempfile), `_compute_reached_tables` worked example, `FKTraversalPolicy` field-interactions subsection |

## Actions performed

- **A1** — Dead imports stripped from `database.py` and `schema.py`. Intentional re-exports keep `# noqa: F401` annotations with an explanatory comment.
- **A2** — `terminal_tables` docstring corrected: outbound FKs ARE followed; only inbound are blocked.
- **A3** — `_localize_asset_row` and the `asset_map` threading through `_load_data` / `_insert_rows_in_order` / `_insert_csv` removed from `database.py`. `_localize_asset_row`, `_build_asset_map`, `_is_asset_table`, and `model=` / `asset_localization=` ctor params removed from `sources.py`. Back-compat `**legacy_kwargs` accepts the old kwargs with `DeprecationWarning`.
- **A4** — `schema_io.ermrest_model_to_metadata` and `metadata_to_typed_schema_defs` renamed to `_…` (now module-private).
- **A5** — `class_prefix` parameter dropped from `SchemaORM.__init__`; `SchemaBuilder.build()` call site updated.
- **A6** — `_SYSTEM_SCHEMAS` alias removed from `catalog_builder.py`; replaced with `DEFAULT_EXCLUDE_SCHEMAS` at use site.
- **A7** — `_validate_anchors` migrated off raw ERMrest URL to datapath `pb.schemas[s].tables[t].filter(t.RID.in_(rids)).attributes(t.RID).fetch()`. Tests added.
- **A8** — Composite-FK skipping in `_rewrite_fks` now emits a single `DeprecationWarning`-style warning per FK via `_warn_composite_fk_skipped` (was silent). Tests added.
- **A9** — `ForeignKeyOrderer._break_cycles_and_sort` now raises `RuntimeError` on depth exhaust instead of silently returning `list(graph.keys())`. Test added.
- **A10** — `is_association_table` and `get_association_class` extracted to new `_orm_helpers.py` module. `database.py` and `schema.py` keep thin delegating wrappers.
- **A12** — `BagBuilder._metadata_as_model` no longer round-trips ERMrest JSON through a tempfile. `Model(\"file-system\", doc)` accepts the dict directly.
- **A14** — `_compute_reached_tables` docstring gained a worked single-anchor multi-path example (Subject → Image directly + via Dataset_Image). `_load_table` docstring documents the `MATCH_BY_COLUMNS` ▸ `VOCABULARY` ▸ `CONTENT` dispatch precedence.
- **A15** — `FKTraversalPolicy` docstring gained a \"Field interactions\" subsection covering the orthogonal pairings: `dangling_fk_strategy` × `preserve_provenance`, `match_by_columns` × `content_on_conflict`, `match_by_columns` × `preserve_provenance`, `asset_mode` × everything else.

## Deferred

- **A11** — BagDatabase row-load refactor (~−300 LoC). Audit classified this as *design-level not mechanical* — needs a careful look at the inheritance interface. Separate PR.
- **A13** — Pull asset-upload out of `catalog_loader.py` into `asset_uploader.py`. +1 file with no net LoC change; flagged deferrable in the audit. Separate PR.

## Test plan

- [x] `pytest tests/deriva/bag/` — **275 pass, 2 skip** (was 270 before the sprint; +5 regression tests for A7/A8/A9)
- [x] `pytest tests/` excluding `tests/deriva/bag/` and `tests/deriva/core/test_hatrac_store.py` — **181 pass, 198 skip** (no regressions; skips are catalog-required)
- [ ] Integration check against deriva-ml on a real catalog (next session, not blocking)

🤖 Generated with [Claude Code](https://claude.com/claude-code)

---
