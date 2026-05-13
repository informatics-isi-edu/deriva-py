# `deriva.bag` Package Audit — 2026-05

Reviewed all 15 modules under `deriva/bag/` (~9.2 KLoC) at the tip of
the `deriva-ml` branch (HEAD `8b19eaf`, PRs #224–#248), plus the
`_ColumnWrapper.in_` operator added by #242 in
`deriva/core/datapath.py` and the legacy shim at
`deriva/core/bag_database.py`. External-consumer checks were run
against `DerivaML/deriva-ml/src/deriva_ml/`. Findings below are
concrete: file/line citation, what's wrong, what to do, estimated
risk, estimated LoC delta. Items not raised here either survived
scrutiny or are minor enough to not warrant a maintainer's attention
right now.

---

## 1. Dead code

### 1.1 `BagDatabase._localize_asset_row` is a no-op with five vestigial parameters

`deriva/bag/database.py:408-435` — the method body returns
`tuple(row)` and the docstring explicitly says every other argument is
"unused." The call site at `database.py:655-674` still computes
`asset_indexes`, `rid_index`, and threads `asset_map` through
`_insert_csv`, all of which die in the no-op. The `_build_asset_map`
call on line 520 inside `_load_data` is then redundant for the CSV-load
path — the only live consumer of the asset map is
`resolve_asset_local_path` (which calls `_build_asset_map` on demand
itself).

**Fix:** delete `_localize_asset_row`, drop the `asset_map` thread
through `_load_data`/`_insert_rows_in_order`/`_insert_csv`, drop the
asset-column-index lookup in `_insert_csv`, drop the asset_map arg of
`_build_asset_map` from `_load_data`. **Risk: low** (pure dead-code
removal; no external callers — confirmed via grep across deriva-ml).
**LoC: −60 to −80, mechanical.**

### 1.2 `BagDataSource._localize_asset_row` has a known no-op bug it preserves "for parity"

`deriva/bag/sources.py:221-237` — the docstring openly admits the
lookup checks `url in self._asset_map` while the map is keyed by
`urlparse(url).path`, so the rewrite "silently does nothing for any
real asset row." The comment says "parity with `BagDatabase`" — but
`BagDatabase`'s version is also a no-op (1.1 above), so the parity is
"two broken implementations." Since (a) the consumer side runs through
`BagDatabase`, not `BagDataSource`, and (b) the docstring claims fix
"will land as part of the deriva-ml migration PR" — that migration has
already landed. Either fix it (key map by full URL too, or check both
forms) or delete the method and the `asset_localization` flag entirely.

**Recommendation:** delete `_localize_asset_row`, `_build_asset_map`,
`_is_asset_table`, and the `model=` / `asset_localization=` ctor
params. `BagDataSource` is consumed only via `DataLoader → CSVSink`
(producer path) and the loader doesn't care about asset-row
localization — that's `BagCatalogLoader._upload_assets`'s job, which
already routes through `BagDatabase.resolve_asset_local_path`. **Risk:
low-medium** (deriva-ml's `test_data_sources.py` constructs the source
with `asset_localization=False`, so removing the flag is a tiny API
change; grep also shows no caller setting it to `True`). **LoC: −70,
mostly mechanical.**

### 1.3 Duplicated `is_association_table` / `get_association_class`

`deriva/bag/database.py:803-934` is byte-identical (up to comments and
docstring wording) to `deriva/bag/schema.py:291-455`. Both are static
methods. `BagDatabase` doesn't inherit from `SchemaORM`. The
`SchemaORM` copy is the one the new `BagBuilder` uses; the
`BagDatabase` copy is the one deriva-ml's `DatabaseModel` re-exposes
(via inheritance) to its own callers. Two copies = two places to
update for any future fix.

**Fix:** delete the `BagDatabase` copies; have `BagDatabase` call
`SchemaORM.is_association_table(...)` directly (the method is
`@staticmethod`, so no orm instance is required). Or factor both into
a tiny helper module. **Risk: low** (the method is static-pure;
`DatabaseModel` re-exports via inheritance, so a delegating
implementation keeps the public surface identical). **LoC: −135,
mechanical.**

### 1.4 `BagCatalogBuilder._SYSTEM_SCHEMAS` is a back-compat alias used only inside the file

`deriva/bag/catalog_builder.py:77` defines `_SYSTEM_SCHEMAS =
DEFAULT_EXCLUDE_SCHEMAS` with an "exists for back-compat" comment, but
nothing imports the symbol from outside the module (it's underscored
and there's no `__all__` re-export). Just use
`DEFAULT_EXCLUDE_SCHEMAS` at the two `_is_excluded_schema` callsites.
**Risk: low. LoC: −3.**

### 1.5 Unused imports

- `deriva/bag/database.py:53,54,57,60` — `Session`, `TypeEngine`,
  `DerivaColumn`, `DerivaType` are imported and never used in this
  file (the type-decorator definitions that referenced them have moved
  to `_column_types.py`).
- `deriva/bag/schema.py:51,54,56,71` — same: `DerivaColumn`,
  `DerivaType`, `JSON`, `TypeEngine` are imported but never used.
- `deriva/bag/anchors.py:37` — `Annotated`, `Literal`, `Union`,
  `field_validator` are imported and all are used; no issue.

**Fix:** drop the dead imports. **Risk: trivial. LoC: −10.**

### 1.6 `_class_prefix` on `SchemaORM` is stored but never read

`deriva/bag/schema.py:155` — `self._class_prefix = class_prefix` is
stashed on `SchemaORM` but no method consumes it. The prefix is only
used during build (inside `SchemaBuilder._create_tables`'
`classname_for_table` closure). The constructor argument is passed by
`SchemaBuilder.build()` only to be discarded. **Fix:** drop the
parameter from `SchemaORM.__init__` and the corresponding `class_prefix=`
in `SchemaBuilder.build()` at line 657. **Risk: low** (public ctor, but
no external caller — search of deriva-ml shows none construct
`SchemaORM` directly). **LoC: −3.**

### 1.7 `metadata_to_typed_schema_defs` and `ermrest_model_to_metadata` have no callers

`deriva/bag/schema_io.py:599, 727` — grep across deriva-py and
deriva-ml shows zero callers outside this file's own docs.
`typed_schema_def_to_metadata` is used (once, from `BagBuilder`).
`metadata_to_typed_schema_defs` is documented as "useful when
`BagCatalogLoader` needs to create destination tables from a bag's
schema" but that codepath doesn't exist — the loader assumes the
destination already has the schema (precondition documented in
`ContentConflictStrategy` and the create_ml_schema rules from
deriva-ml's CLAUDE.md). Likewise `ermrest_model_to_metadata` is a
plausible utility that nothing currently invokes.

**Fix:** mark both as private (`_metadata_to_typed_schema_defs`,
`_ermrest_model_to_metadata`) or delete. Since `typed_schema_def_to_metadata`
itself has a comment admitting it's "a placeholder pending the typed-module
API check" (`schema_io.py:670-676`), now is a good time to revisit
whether the typed-module bridge is needed at all. **Risk: low** (no
external callers). **LoC: −60 if both removed plus the placeholder
notes.**

### 1.8 Outdated comments referencing superseded behavior

- `deriva/bag/catalog_builder.py:233-234` — `_validate_anchors`
  docstring says "Issues one ?RID=any(...) query per RIDAnchor table
  directly against ERMrest (the datapath `_ColumnWrapper` doesn't
  expose `in_`)". As of PR #242 (`a91a6b4`) the datapath
  `_ColumnWrapper` *does* expose `in_`. The comment is now wrong and
  the impl can be migrated to datapath. (See §2.1.)
- `deriva/bag/catalog_builder.py:74-77` — "Backwards-compat alias for
  the canonical default-exclude set" — see §1.4.
- `deriva/bag/schema.py:30-42` — module docstring says the decorators
  are "imported from `deriva.bag.database`" — they've since moved to
  `_column_types.py` (PR #248). The actual `from ... import` block at
  line 76 correctly reads `_column_types`, but the prose docstring
  hasn't been updated.

**Fix:** update each docstring. **Risk: zero. LoC: ±0.**

---

## 2. Existing deriva-py interfaces being properly used

### 2.1 `_validate_anchors` rolls its own ERMrest URL after `.in_` shipped

`deriva/bag/catalog_builder.py:234-277` constructs
`f"/attribute/{schema_name}:{table_name}/RID=any({rid_list})/RID"` via
string concatenation and calls `self.catalog.get(path)`. Since #242
(`a91a6b4`), `_ColumnWrapper` supports `column.in_(values)`, which is
exactly the call this should be making — and the docstring at line
233-234 still says datapath has no `in_`. The PR title "_validate_anchors
uses ERMrest GET (datapath has no .in_)" (commit 495dfc3) is now
stale, and the workaround is the only remaining caller for ad-hoc URL
construction in the package.

**Fix:** rewrite as

```python
pb = self.catalog.getPathBuilder()
results = pb.schemas[schema_name].tables[table_name].filter(
    pb.schemas[schema_name].tables[table_name].RID.in_(anchor.rids)
).attributes(...).fetch()
```

or use the catalog's path-builder fluent API. **Risk: low-medium**
(needs a small integration test against a real catalog; the unit
tests in `test_catalog_builder.py` mock the catalog response). **LoC:
−15.**

### 2.2 `BagCatalogLoader._fetch_existing_vocab_by_name` and friends use raw URL strings

`catalog_loader.py:680-691, 793-810, 925-933` all build ERMrest
attribute-group / attribute paths by string concatenation. The
patterns are stable (just `/attributegroup/{s}:{t}/<cols>;RID` and
`/attribute/{s}:{t}/RID`), so this is not buggy — but the package's
own README-equivalent (the API-priority note in deriva-ml's
CLAUDE.md) says "prefer datapath over raw ERMrest URLs." For
consistency with the rest of the package and to get URI-escaping for
free, these could use `pb.schemas[s].tables[t].attributegroup(...)` or
the underlying typed query builders. Not urgent — but every "build a
URL string" site is one more place where adding e.g. a hyphen-in-table-
name would surprise.

**Recommendation:** track as a follow-up cleanup, not a blocker.
**Risk: medium** (small refactor surface area but every change must
preserve the exact wire format the loader's HTTP path expects). **LoC:
neutral.**

### 2.3 `urllib.parse.urlparse` for hatrac path extraction is fine

`catalog_loader.py:1318-1350` (`_hatrac_path_for`) and
`builder.py:107-143` (`hatrac_url_for`) — both reuse the same
`/hatrac/{table}/{md5}.{filename}` convention. Now that
`hatrac_url_for` is the canonical builder (#243), `_hatrac_path_for`
is the canonical *parser*. They live in different modules but they
work together; no duplication.

One small inconsistency: `_hatrac_path_for` uses `urlparse` while
`hatrac_url_for` just f-strings the path. Both are correct, but it
means there's no single place that owns the
`/hatrac/{Table}/{md5}.{filename}` URI template — if it changes (e.g.
URL-encoding the filename), both functions need an edit. Consider
sharing the template via a constant in `profile.py`.

**Risk: low. LoC: ±0.**

### 2.4 `BagCatalogLoader` calls `catalog.post/get/put` directly — fine

These are the public surface of `ErmrestCatalog`. No private symbol
abuse. The async wrapping via `asyncio.to_thread` is the right
approach for the existing sync HTTP client.

### 2.5 `BagCatalogLoader._dest_hatrac_store` reaches `catalog._credentials`

`catalog_loader.py:1314` and `catalog_builder.py:858` both pass
`self.catalog._credentials` into another component. Private attribute
of `ErmrestCatalog`. `HatracStore` and `GenericDownloader` both
require a credentials dict; `ErmrestCatalog` doesn't expose
credentials publicly. Not a bag-package bug — but if
`ErmrestCatalog.get_credentials()` ever appears upstream, these two
lines should switch immediately. Worth tagging with a TODO comment.

---

## 3. FK traversal and path generation: uniformity

The package has **six** sites that walk or apply FK information.
Reviewed for consistency:

### 3.1 FK column identification — broadly consistent

All six sites identify FK columns via `table.foreign_keys` (the
outbound list) and use `fk.foreign_key_columns[0].name` for the local
column, `fk.pk_table` / `fk.referenced_columns[0]` for the target.
Consistent across:

- `loader.py:118,140,166` (`ForeignKeyOrderer`)
- `catalog_builder.py:421,434` (forward-and-backward walk —
  `table.referenced_by` for the inbound side)
- `catalog_loader.py:896,971-1000` (`_rewrite_fks`,
  `_apply_dangling_fk_strategy`)
- `database.py:256,312` (SchemaBuilder mirror)
- `schema.py:775,856` (SchemaBuilder)
- `schema_io.py:249-270` (`ermrest_json_to_metadata`)

**One inconsistency:** `_rewrite_fks` and the dangling-FK check both
silently `continue` on composite FKs (`if len(fk.foreign_key_columns)
!= 1: continue` — `catalog_loader.py:897-900`). The rest of the bag
walk doesn't have that limit. For deriva-ml catalogs this is
harmless (composite FKs are rare), but for a general-purpose
clone-via-bag tool the silent skip is a footgun. **Recommendation:**
either log a warning when a composite FK is skipped, or raise — but
don't drop silently. **Risk: low. LoC: +5.**

### 3.2 Cycle detection — three different implementations

- `loader.py:213-264` (`ForeignKeyOrderer._break_cycles_and_sort`):
  `graphlib.TopologicalSorter` + edge-removal recursion + a separate
  `find_cycles()` DFS for diagnostics.
- `catalog_builder.py:498-503`: simple-path guard — `if key in
  prefix_path: return` during BFS expansion.
- `schema.py`/`database.py`: no cycle detection — SQLAlchemy
  resolves at create_all time.

The first two are deliberately different (one builds an order, the
other guards a BFS), but they don't share any helpers. If the project
ever needs "what cycles does this schema have," `find_cycles` in
`loader.py:347-399` is the answer — but `catalog_builder`'s walk could
in theory hit a many-FK cycle and exhaust `max_paths`. The two would
benefit from a shared comment cross-referencing each other.

### 3.3 Dangling-FK strategy is only applied in one place

`DanglingFKStrategy` is honored exclusively by
`BagCatalogLoader._apply_dangling_fk_strategy`. Producers
(`CatalogBagBuilder`, `BagBuilder`) write whatever the source has, and
`BagDatabase` turns `foreign_keys=OFF` at SQLite-load time to admit
dangling refs (per the doc at `database.py:489-517`). This is the
correct division of responsibilities — bag is a snapshot; FK policy is
a load-time concern. The docstrings of `DanglingFKStrategy`
(`traversal.py:99-137`) and `BagDatabase._load_data` both spell this
out clearly.

**No drift.** The one improvement: `DanglingFKStrategy.PRESERVE` was
added (#228) and `preserve_provenance` (#229) — these orthogonal flags
are now both on `FKTraversalPolicy`. The `_load_content_table`
docstring mentions PRESERVE only via the short-circuit at line 962. A
maintainer reading just the doc on `policy.preserve_provenance` vs
`policy.dangling_fk_strategy` may not see how they interact (they
don't — but the *naming* invites confusion). Worth a sentence in
`FKTraversalPolicy`'s class docstring noting they are independent.

### 3.4 Outbound vs inbound terminology — well-documented now

`catalog_builder.py:404-433` has an excellent multi-paragraph comment
explaining the inbound/outbound distinction for terminal tables. This
is the kind of thing that bit us in the past (the comment exists
because PR #226 fixed exactly this misunderstanding). Good.

`traversal.py:210-221` (the `terminal_tables` docstring) doesn't
mention the same distinction. It says "the walker emits rows ... but
does not follow its outbound or inbound FKs to discover further
tables" — which is *wrong* per the implementation (terminal tables do
follow outbound, do not follow inbound). The docstring contradicts
the code. **Fix:** update the docstring to match
`catalog_builder.py:404-433`'s explanation. **Risk: zero (doc-only).
LoC: +5.**

---

## 4. Patch-series drift

Each finding here corresponds to a pair of PRs that left stratification
in the code:

### 4.1 `_localize_asset_row`'s parameter wedge (PRs #218, #230, #231)

The history: #218 / #230 / #231 each touched asset-row localization
incrementally; the parameters `asset_indexes` / `asset_map` /
`rid_index` accreted before being made redundant by `resolve_asset_local_path`
(now the only "real" path-resolution surface). The vestigial
parameters at `database.py:408-435` are textbook patch-series drift.
See §1.1 for the fix.

### 4.2 `preserve_provenance` flag + RCB/RMB strip logic (PR #229, #237)

The `_insert_rows` method (`catalog_loader.py:1140-1206`) carries
the two-branch logic inline. The branches don't share any code; the
"strip system columns" pass at line 1171-1175 fires only when
`preserve_provenance=False`. The `_SYSTEM_COLUMNS` tuple is a local
constant inside the function. Lifting it to module scope (and giving
it a docstring) would make the two-mode logic discoverable.

**Risk: low. LoC: −3 net.**

### 4.3 Two `BagDatabase` and `SchemaORM` mirror-table constructors are 95% identical

`BagDatabase._create_tables` (`database.py:196-352`) and
`SchemaBuilder._create_tables` (`schema.py:679-907`) build the same
SQLite mirror with the same nullability rules, the same FK
constraints, and the same cross-schema relationship pass. The
difference is (a) `SchemaBuilder` can be `:memory:` (uses underscore
naming), and (b) `BagDatabase` is always file-based. PR #248 already
dedup'd the type-decorator side; the next round should dedup the
table-construction side too.

A reasonable refactor: have `BagDatabase` call `SchemaBuilder` for
Phase 1 (the table creation + automap) and add only its own
post-build behaviour (`_load_data` reading CSVs). This collapses ~150
lines and removes the "if it's wrong here, fix it twice" hazard. **Risk:
medium** (touches both consumers; `BagDatabase.engine` and
`SchemaORM.engine` are the same kind of object, but `DatabaseModel`
in deriva-ml inherits from `BagDatabase` and accesses attributes
directly — needs a careful look at the inheritance interface). **LoC:
−150, design-level not mechanical.**

### 4.4 Two parameter sets on `FKTraversalPolicy` that interlock — undocumented

The policy carries (a) `dangling_fk_strategy` (#228), (b)
`preserve_provenance` (#229), (c) `match_by_columns` (#245), and (d)
`content_on_conflict` (#225-ish — content tables). All four can in
principle interact; only the `match_by_columns` × vocabulary
interaction is documented (line 87-91 of `_TableClass` in
`catalog_loader.py`). A reader trying to predict the behavior of
`preserve_provenance=False` + `dangling_fk_strategy=PRESERVE` +
`match_by_columns=...` won't find that combination called out.
**Recommendation:** add a small "interactions" subsection in
`FKTraversalPolicy`'s class docstring. **Risk: zero (doc-only). LoC:
+15.**

### 4.5 `_break_cycles_and_sort` recursion depth has a defensive guard, but no test

`loader.py:213-264` — the `_depth` recursion guard at line 222 says
"can't have more cycles than edges." If the bound is hit the function
returns an arbitrary order via `list(graph.keys())`. This is
correctness-preserving (the destination's FK enforcement will surface
the error), but it's a silent failure mode. With the cycle-broken-edges
machinery for two-phase insert (#223) we now actually *care* about
the broken-edge list being accurate, and an arbitrary-order fallback
would produce an empty broken-edge list — so a downstream cycle
would fail with "FK constraint" rather than a clear "could not break
cycles" error.

**Fix:** at the depth-exhausted branch, raise rather than warn-and-return.
A schema with more cycles than edges is a bug, not a recoverable
state. **Risk: low (currently impossible to hit on real schemas).
LoC: +3.**

---

## 5. Simplification opportunities

(Beyond the dead-code section.)

### 5.1 `BagDatabase._load_data` → call `DataLoader + BagDataSource + SQLiteSink`

`database.py:489-685` reimplements the CSV-walk → FK-order → insert
pipeline by hand (rglob, `csv.reader`, `sqlite_insert`,
`on_conflict_do_nothing`). The same job is exactly what
`DataLoader(orm, BagDataSource(bag_path), sink=SQLiteSink(orm,
on_conflict="ignore"))` does — and the loader/source/sink trio was
*built* for this (see `loader.py:1-29` module docstring). The
hand-rolled version exists for historical reasons (it predates the
loader split in PR #224-ish).

**Refactor:** replace `_load_data` + `_insert_rows_in_order` +
`_insert_csv` with a single `DataLoader` invocation. The pragma
toggling around FK enforcement (lines 559-594) stays at the
`BagDatabase` level (it's specific to the CSV-load step), but the row
movement becomes one call. **Risk: medium** (changes the connection
boundary semantics — `DataLoader` opens its own engine context;
`BagDatabase` currently does a single big transaction with the pragma
flip). **LoC: −110, design-level.**

### 5.2 `BagBuilder._metadata_as_model` round-trips through a temp file

`builder.py:712-728` writes the MetaData to a tempfile JSON to feed
`Model.fromfile("file-system", path)`. The comment admits this is
"mildly wasteful." A direct converter would land in `schema_io.py`
alongside the existing `ermrest_json_to_metadata` (just take the dict
from `metadata_to_ermrest_json` and call `Model.fromjson(dict)` if
that exists, or factor out the json→Model bit from `fromfile`).

**Risk: low. LoC: −15, mechanical-ish.**

### 5.3 `BagBuilder._make_bdbag` pre-seeds bagit scaffolding to dodge bdbag's "fresh dir" branch

`builder.py:740-808` writes stub `bagit.txt`, `bag-info.txt`, and
empty `manifest-{md5,sha256}.txt` files solely to push bdbag down its
"update existing" branch (the comment at line 760-779 explains).
This works but is fragile against bdbag version changes. The
deriva-py code already has `bdb.make_bag(..., update=True,
idempotent=True)` — the `update=True` should mean "treat as existing
bag" without the stub files. If a bdbag version bump lets us drop
the stubs, that'd be a clean simplification.

Not actionable in this audit (it's an upstream bdbag question), but
worth a comment linking the bdbag-version dependency.

### 5.4 `BagCatalogLoader.run()` dual sync/async wrapper is reasonable but has subtle nest_asyncio surface

`catalog_loader.py:315-347` — the loop-detection-plus-`nest_asyncio`
dance is correct but the failure mode in a non-notebook async
context (no running loop *but the caller wanted async*) is awkward.
The docs at line 332 advise calling `arun` directly. Consider adding
a `RuntimeError` re-raise that points at `arun` for callers who get
the "cannot nest" error in a non-IPython context.

**Risk: low. LoC: +5.**

### 5.5 `BagCatalogLoader._apply_deferred_fk_updates` uses default-arg-binding for the closure

`catalog_loader.py:492-496`:

```python
def _do_put(payload=payload, url=url) -> None:
    response = self.catalog.put(url, json=payload)
```

The default-arg trick captures the loop variable, which is idiomatic
but obscured here because the closure is one-shot (the function is
called immediately on the next line via `asyncio.to_thread`). For
clarity, `functools.partial(...)` or even just inlining would make
the intent more obvious. Cosmetic. **LoC: −2.**

---

## 6. Maintainability

### 6.1 Public API surface vs. actual usage

Tracking actual external usage (grep across `deriva_ml/src/`):

| Symbol                                | Status                 |
|---------------------------------------|------------------------|
| `BagBuilder`, `CatalogBagBuilder`     | Used                   |
| `BagCatalogLoader`, `LoadReport`      | Used                   |
| `BagDatabase`, `BagCacheIndex`        | Used                   |
| `FKTraversalPolicy`, `Anchor`, `RIDAnchor`, all enums | Used   |
| `DataLoader`, `CSVSink`, `SQLiteSink`, `ForeignKeyOrderer` | Used through `deriva_ml.model.*` re-export shims |
| `BagDataSource`, `CatalogDataSource`, `DataFrameDataSource`, `IterableDataSource`, `LocalDBDataSource` | Re-exported by deriva-ml shim; `DataFrame`/`Iterable` not used by deriva-ml itself |
| `SchemaBuilder`, `SchemaORM`          | Used                   |
| `hatrac_url_for`                      | Used (`deriva_ml.execution.bag_commit`) |
| `BAG_SCHEMA_VERSION`, `BAGIT_PROFILE_IDENTIFIER` | Not used outside |
| `SchemaVersionError`, `create_wal_engine`, `ensure_schema_meta` | Used via `deriva_ml.local_db.sqlite_helpers` shim |
| `metadata_to_ermrest_json` | Used (`deriva_ml.execution.bag_commit`) |
| `ermrest_json_to_metadata` | Used (`deriva_ml.execution.bag_commit`) |
| `ermrest_model_to_metadata`, `metadata_to_typed_schema_defs`, `typed_schema_def_to_metadata` | Only `typed_schema_def_to_metadata` used internally |
| `PathAnchor`, `TableAnchor`           | Defined and exported, but only `RIDAnchor` is used externally |
| `Anchor` (union type)                 | Used as type hint     |
| `LocalDBDataSource`, `DataFrameDataSource`, `IterableDataSource` | Re-exported but used only by `BagBuilder._write_pending_rows` internally |

**Findings:** `metadata_to_typed_schema_defs` and
`ermrest_model_to_metadata` (see §1.7) have zero external callers
despite being in `schema_io`'s public surface. `PathAnchor` and
`TableAnchor` are public-API "available but not yet used" — fine if
they're load-bearing for future producers, otherwise candidates for
later cleanup.

### 6.2 Test-coverage gaps where a refactor would be risky without tests first

- **§3.1 — composite FK skip**: no test covers a composite FK in
  `_rewrite_fks`. Before changing behavior, add a test that asserts
  the current silent-skip behavior so any change is intentional.
- **§5.1 — `_load_data` refactor**: tests exist for both
  `BagDatabase` (`test_database.py`) and `DataLoader`
  (`test_loader.py`), but the test that exercises the *combined*
  pragma-OFF transaction-rollback behavior of `_load_data` is in
  `test_database.py`. Before unifying, ensure the rollback case (CSV
  with bad data → cleanup happens, FK pragma restored) survives.
- **§4.5 — `_break_cycles_and_sort` depth exhaustion**: no test
  hits the bound. Add one (a synthetic many-cycles schema) before
  changing the fallback to raise.

### 6.3 Comment quality — overall good, two soft spots

- The "deriva-bag profile" concept is well-explained in `profile.py`
  and in the package `__init__`. A new maintainer can land in this
  package and understand it.
- The `FKTraversalPolicy` docstring is excellent — it explains every
  field, its default, and the rationale.
- **Soft spot 1:** the multi-path emission rule in
  `catalog_builder.py:_compute_reached_tables` (lines 339-365) and the
  `_table_path_set` / `_table_paths` distinction is correct but
  *dense*. A diagram showing the BFS for a small example (Subject →
  Image direct vs. Subject → Dataset_Subject → Dataset → Dataset_Image
  → Image) would make this section much more approachable.
- **Soft spot 2:** `_TableClass` precedence (vocabulary vs.
  match_by_columns vs. content) is documented inside the enum
  (`catalog_loader.py:63-94`), but the precedence at the dispatch
  site (`_load_table`) is implicit in if/elif order. A one-line
  pointer at the dispatch site would help.

### 6.4 Type-hint coverage and consistency

Coverage is high — most public methods have hints. A few rough edges:

- `BagDatabase.__del__` and `SchemaORM.__del__` have hints, but the
  `Type[Any]` returned by association lookup methods could be
  tightened to the AutomapBase class type.
- `BagDataSource._localize_asset_row` returns `dict[str, Any]` while
  `BagCatalogLoader._coerce_empty_to_null` returns the same and uses
  the same name pattern; if §1.2's deletion happens, ensure the
  remaining `_coerce_*` helpers all share the same `row: dict[str,
  Any] → dict[str, Any]` shape.
- `_TableClass` is defined inside `catalog_loader.py` (line 63-98),
  but it's an internal enum. If callers ever need to inspect the
  classification, it'd need to move to a public location.

### 6.5 Naming consistency

| Concept           | Forms used                                | Issue?             |
|-------------------|-------------------------------------------|--------------------|
| Qualified table   | `qname`, `schema.table`, `(schema, table)` tuple | Both `qname` (string) and `(schema, table)` (tuple) are used. The package generally uses the tuple internally and `qname` for log/print/dict-key purposes. Consistent enough. |
| Table object      | `table` (deriva-py), `sql_table` (SQLAlchemy) | Good — consistent disambiguation. |
| ORM class         | `table_class`, `referenced_class`, `cls`  | OK. |
| Bag path          | `bag_path`, `bag_dir`, `output_dir`       | Three names for very similar things. `BagBuilder.output_dir` ≠ `BagCatalogLoader.bag_path`. The split makes sense (output vs. input), but a docstring at the top of each class noting "this is where the bag lives after `build()`" would help. |

### 6.6 File-size outliers

- `catalog_loader.py` (1402 lines) is the only outlier. Two subsystems
  live in it cleanly: the row pipeline (vocab/match-by-columns/content
  paths, dangling-FK handling, deferred-FK two-phase insert) and the
  asset upload pipeline (Hatrac store, HEAD-and-PUT, path
  extraction). The asset side is ~200 lines (lines 1208-1380); pulling
  it into a `deriva.bag.asset_uploader` module would (a) drop
  `catalog_loader.py` to ~1200 lines, (b) make the row vs. byte
  responsibilities explicit, (c) let asset-upload retry / progress
  callbacks evolve independently. **Risk: low** (clean seam: the asset
  uploader takes the policy and the dest catalog and works on rows).
  **LoC delta: ±0 net, +1 module file.**

- `database.py` (934) and `schema.py` (948) are at the boundary. If
  §1.3, §4.3, §5.1 land, both drop to ~700 — fine.

---

## Recommended actions (ranked)

| # | Action | Risk | LoC | Impact |
|---|--------|------|-----|--------|
| 1 | **§1.5 / §1.8** Delete unused imports + update stale comments (`_validate_anchors` claim that datapath has no `.in_`; `schema.py` module docstring; `_SYSTEM_SCHEMAS` alias) | trivial | −15 | low cost, removes confusion for new readers |
| 2 | **§3.4** Fix `terminal_tables` docstring in `traversal.py:210-221` — it currently says outbound FKs are *not* followed, but the code does follow them | zero | +5 | doc-bug; matters for anyone reading just the policy doc |
| 3 | **§1.1 + §1.2** Delete `BagDatabase._localize_asset_row` no-op + `BagDataSource._localize_asset_row` no-op (with `asset_localization=` flag and `model=` ctor arg) | low | −150 | removes a known-broken codepath that's only there for "parity" |
| 4 | **§1.7** Delete or privatize `metadata_to_typed_schema_defs` + `ermrest_model_to_metadata` (no external callers) | low | −60 | shrinks API surface to what's actually used |
| 5 | **§1.6** Drop `_class_prefix` from `SchemaORM.__init__` (stored, never read) | low | −3 | cleans up the constructor signature |
| 6 | **§1.4** Replace `_SYSTEM_SCHEMAS` alias with direct `DEFAULT_EXCLUDE_SCHEMAS` use | low | −3 | one fewer "back-compat alias" with no callers |
| 7 | **§2.1** Migrate `_validate_anchors` from raw URL to `.in_()` (datapath) — the comment claiming `.in_` doesn't exist is now wrong | low-medium | −15 | brings the validation in line with `API priority` rule from CLAUDE.md |
| 8 | **§3.1** Add log/raise instead of silent skip on composite FK in `_rewrite_fks` (+ regression test) | low | +5 | prevents silent data-corruption on non-deriva-ml schemas |
| 9 | **§4.5** Make `_break_cycles_and_sort` raise on depth exhaustion (+ test) | low | +5 (test +20) | depth-exhaust is currently a silent arbitrary-order fallback |
| 10 | **§1.3** Dedupe `is_association_table`/`get_association_class` between `BagDatabase` and `SchemaORM` (delegate from the former to the latter) | low | −135 | removes the "fix it in two places" footgun |
| 11 | **§6.6 + §1.3 + §5.1** Refactor `BagDatabase._create_tables` to call `SchemaBuilder._create_tables` (Phase 1 reuse); refactor `BagDatabase._load_data` to call `DataLoader+SQLiteSink` | medium | −300 | the biggest single LoC win; collapses two parallel pipelines that are 95% the same |
| 12 | **§5.2** Add direct `MetaData → Model` converter in `schema_io.py`; replace `BagBuilder._metadata_as_model` tempfile round-trip | low | −15 | removes the "mildly wasteful" hack |
| 13 | **§6.6** Pull asset-upload pipeline (1208-1380 of `catalog_loader.py`) into a separate `asset_uploader.py` module | low | ±0 | improves readability; lowers `catalog_loader.py` LoC |
| 14 | **§6.3** Add a diagram + example to `_compute_reached_tables` and a one-line precedence pointer at `_load_table`'s dispatch | zero | +20 | future-maintainer ergonomics |
| 15 | **§4.4** Document interactions between `FKTraversalPolicy` fields (`preserve_provenance` × `dangling_fk_strategy` × `match_by_columns`) | zero | +15 | doc-only |

Items 1–6 are mechanical and could go in one batch; 7–10 each want a
small test plan; 11 is the design-level change to plan separately;
12–15 are polish.
