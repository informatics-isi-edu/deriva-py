# FK-cycle warning dedupe + intentional-cycle allowlist (F5)

**Status:** Proposed
**Date:** 2026-05-16
**Source:** `/Users/carl/GitHub/DerivaML/deriva-ml-model-template/docs/findings/2026-05-16-phase-1-improvements.md` §F5.

## Problem

Two warnings fire on every bag-pipeline schema export that touches a cyclic FK:

1. SQLAlchemy `SAWarning: Cannot correctly sort tables ...` — emitted by `metadata.sorted_tables` in [schema_io.py:414](../../deriva/bag/schema_io.py:414). SQLAlchemy's sort doesn't handle cycles; the warning announces that fact. deriva-py doesn't use the sort result for cyclic graphs (it has its own cycle-breaker in `ForeignKeyOrderer`), so the warning is announcing an internal-only mismatch the consumer can't act on.
2. `logger.warning("Breaking cycle in FK dependencies: A -> B -> A")` from [loader.py:248-250](../../deriva/bag/loader.py:248). This *is* a real signal: deriva-py is making a unilateral decision about which FK edge to drop. The user should know. But:
   - It fires once per `ForeignKeyOrderer._break_cycles_and_sort` call, not once per cycle per loader instance — during one `load-cifar10` invocation it repeats ~6×.
   - There's no mechanism for a downstream consumer (deriva-ml) to declare "this cycle is intentional in my schema; please log at DEBUG rather than WARNING."

## Decisions

Settled during grilling:

1. **The SAWarning is pure internal noise.** Always suppress at the `sorted_tables` call site. deriva-py owns the cycle handling; SQLAlchemy's complaint is about an algorithm deriva-py isn't using.
2. **The "Breaking cycle" log line is real signal — keep it, but dedupe.** Log each unique cycle at most once per loader instance. Repetition during a single multi-bag operation is the actual user pain.
3. **Allow downstream consumers to mark cycles as intentional.** Add an opt-in allowlist threaded through `FKTraversalPolicy` and into `ForeignKeyOrderer`. Cycles in the allowlist log at DEBUG; cycles not in the allowlist still log at WARNING. deriva-py stays free of deriva-ml schema knowledge — deriva-ml declares its own known-OK cycles.
4. **Cycle identity is direction-agnostic.** Use `frozenset(cycle[:-1])` as the lookup key (drop the trailing repeat-of-start). Two distinct cycles over the same node set would collide; for any realistic schema (and certainly for 2-node cycles like `Dataset ↔ Dataset_Version`) this is fine.

## Out of scope

- Schema-validation pass for accidentally-cyclic user schemas. If schema linting matters, it deserves a separate ticket with a dedicated entry point — not a side-effect of bag export.
- Auto-discovery of intentional cycles. The allowlist is explicit.
- Changing the cycle-breaker algorithm itself. `_break_cycles_and_sort` already records every dropped edge in `_cycle_broken_edges` for two-phase load consumers; that contract stays.

---

## Implementation

### Files

- [`deriva/bag/schema_io.py`](../../deriva/bag/schema_io.py) — wrap the `metadata.sorted_tables` loop in `warnings.catch_warnings()`. Add `warnings` and `sqlalchemy.exc.SAWarning` to module imports.
- [`deriva/bag/loader.py`](../../deriva/bag/loader.py) — `ForeignKeyOrderer.__init__` accepts `intentional_cycles: frozenset[frozenset[str]] = frozenset()`. `_break_cycles_and_sort` logs at WARNING vs DEBUG based on cycle identity. Add per-instance `_reported_cycles: set[frozenset[str]]` to dedupe. Update the `DataLoader` constructor at [line 711](../../deriva/bag/loader.py:711) to pass through `intentional_cycles` (default empty).
- [`deriva/bag/traversal.py`](../../deriva/bag/traversal.py) — `FKTraversalPolicy` gains an `intentional_cycles` field, default `frozenset()`.
- [`deriva/bag/catalog_loader.py`](../../deriva/bag/catalog_loader.py) — at the `ForeignKeyOrderer` construction site ([line 428](../../deriva/bag/catalog_loader.py:428)), pass `self.policy.intentional_cycles`.
- Tests under `tests/deriva/bag/` (file TBD — see test plan below).

### `schema_io.py` change

```python
import warnings
from sqlalchemy.exc import SAWarning

# ... (existing module body) ...

# At the existing loop on line ~414:
with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        category=SAWarning,
        message="Cannot correctly sort tables",
    )
    for sql_table in metadata.sorted_tables:
        # ... existing body ...
```

The `message=` filter is intentionally narrow — only the cycle-sort warning is silenced; other `SAWarning`s (deprecated APIs, ambiguous joins, etc.) still surface.

### `ForeignKeyOrderer` change

```python
class ForeignKeyOrderer:
    def __init__(
        self,
        model: Model,
        schemas: list[str],
        intentional_cycles: frozenset[frozenset[str]] = frozenset(),
    ):
        self.model = model
        self.schemas = set(schemas)
        self._intentional_cycles = intentional_cycles
        # Cycles already announced from this instance — used to
        # dedupe per-instance log spam when multiple bag operations
        # rebuild the dependency graph.
        self._reported_cycles: set[frozenset[str]] = set()
        # ... existing init body ...

    def _break_cycles_and_sort(self, graph, error, _depth=0):
        # ... existing recursion-bound check ...

        cycle = list(error.args[1]) if len(error.args) > 1 else []
        if cycle:
            # Cycle identity is direction-agnostic. Drop the trailing
            # repeat of the start node before keying.
            cycle_key = frozenset(cycle[:-1]) if len(cycle) > 1 else frozenset(cycle)
            if cycle_key not in self._reported_cycles:
                self._reported_cycles.add(cycle_key)
                if cycle_key in self._intentional_cycles:
                    logger.debug(
                        f"Breaking known-intentional cycle in FK dependencies: "
                        f"{' -> '.join(cycle)}"
                    )
                else:
                    logger.warning(
                        f"Breaking cycle in FK dependencies: {' -> '.join(cycle)}"
                    )
            # ... existing edge-removal logic stays ...
```

### `FKTraversalPolicy` change

Add to the existing Pydantic model (next to `exclude_schemas` / `exclude_tables`):

```python
intentional_cycles: frozenset[frozenset[str]] = Field(
    default_factory=frozenset,
    description=(
        "FK cycles that the schema owner has marked as intentional. "
        "Each entry is a frozenset of fully-qualified table names "
        "(``{schema}.{table}``) participating in the cycle. The "
        "loader still breaks these cycles (otherwise it can't sort), "
        "but logs the cycle-break at DEBUG rather than WARNING. "
        "Cycles not in this set continue to log at WARNING — "
        "callers should opt in deliberately, not blanket-silence."
    ),
)
```

Pydantic frozenset serialization handles JSON round-trips out of the box for `frozenset[frozenset[str]]` once the `BaseModel` config allows `arbitrary_types_allowed` (which it likely does already; check during implementation).

### Callsite changes

- [loader.py:711](../../deriva/bag/loader.py:711) — `DataLoader.__init__` constructs `ForeignKeyOrderer` with no policy in scope. Two choices: (a) add an optional `intentional_cycles` kwarg to `DataLoader.__init__`, default empty, and pass it through; (b) leave `DataLoader` with no allowlist (the bag-builder side doesn't typically hit `Dataset ↔ Dataset_Version` since it's a deriva-ml schema concern, but the SAWarning suppression already covers most of its noise). **Pick (a)** — symmetric API, future-proof, no behavior change at the default.
- [catalog_loader.py:428](../../deriva/bag/catalog_loader.py:428) — already has `self.policy` in scope. Pass `intentional_cycles=self.policy.intentional_cycles`.

---

## Tests

All server-free, follow the existing bag test pattern. Three test classes:

### `schema_io` SAWarning suppression

1. `test_sorted_tables_does_not_emit_sawarning_for_cycle` — build a minimal `MetaData` with a 2-node FK cycle, call the metadata→ermrest-json conversion, assert no `SAWarning` was emitted (use `pytest.warns(None)` or `warnings.catch_warnings(record=True)`).
2. `test_sorted_tables_still_emits_unrelated_sawarnings` — emit a fake unrelated `SAWarning` from within the suppressed block; confirm it propagates. Guards against an overly-broad filter.

### `ForeignKeyOrderer` dedupe + allowlist

3. `test_first_cycle_break_logs_at_warning` — build a 2-table cyclic model, run `get_insertion_order`, assert one `WARNING` log line containing "Breaking cycle".
4. `test_repeated_cycle_break_does_not_re_log` — same instance, call `get_insertion_order` twice. Only one WARNING line emitted total.
5. `test_intentional_cycle_logs_at_debug_not_warning` — same model, but constructor receives `intentional_cycles=frozenset({frozenset({"s.A", "s.B"})})`. Assert no WARNING, one DEBUG.
6. `test_unknown_cycle_still_logs_at_warning_when_allowlist_nonempty` — construct with an allowlist that names a different cycle; assert the real cycle still WARNs.
7. `test_cycle_identity_is_direction_agnostic` — declare `intentional_cycles=frozenset({frozenset({"s.A", "s.B"})})`. Construct a model where the cycle goes `A -> B -> A` and a separate one where it goes `B -> A -> B`. Both log at DEBUG.
8. `test_cycle_broken_edges_still_populated_when_silenced` — the `_cycle_broken_edges` list (used by two-phase-insert consumers) is independent of the logging decision. Intentional-cycle case still records the dropped edge.

### `FKTraversalPolicy` round-trip

9. `test_policy_intentional_cycles_default_is_empty` — `FKTraversalPolicy().intentional_cycles == frozenset()`.
10. `test_policy_intentional_cycles_roundtrips_through_json` — `FKTraversalPolicy(intentional_cycles=frozenset({frozenset({"s.A", "s.B"})}))` survives `.model_dump_json()` → `.model_validate_json()`. Tests Pydantic frozenset handling; if this fails the policy field may need a custom serializer.

### `BagCatalogLoader` integration

11. `test_catalog_loader_threads_intentional_cycles_to_orderer` — instantiate `BagCatalogLoader` with `policy=FKTraversalPolicy(intentional_cycles=...)`, mock the orderer construction at [catalog_loader.py:428](../../deriva/bag/catalog_loader.py:428), assert the kwarg flowed through.

---

## Verification checklist

- [ ] All 11 new tests pass.
- [ ] Existing `tests/deriva/bag/test_loader.py` and `test_catalog_loader.py` still pass.
- [ ] Manual smoke: run `load-cifar10 --hostname localhost --create-catalog X --num-images 50` with the deriva-ml side updated to pass the allowlist. Confirm the SAWarning is gone and the "Breaking cycle" line either disappears (allowlist active → DEBUG) or fires exactly once instead of ~6×.
- [ ] `cycle_broken_edges` still populated correctly — verify any two-phase-insert consumer still receives the same edge list.

---

## Companion deriva-ml change (separate PR, separate repo)

Wherever deriva-ml constructs a `BagCatalogLoader`, pass:

```python
loader = BagCatalogLoader(
    catalog=catalog,
    bag=bag_path,
    policy=FKTraversalPolicy(
        intentional_cycles=frozenset({
            frozenset({"deriva-ml.Dataset", "deriva-ml.Dataset_Version"}),
        }),
        # ... other policy fields ...
    ),
)
```

The constant should live in one place in deriva-ml (probably `deriva_ml.bag` or `deriva_ml.core.constants`). Cite this design doc from the deriva-ml PR for context.

---

## Follow-ups (not in this change)

1. **Schema-linting pass.** If "catch accidental cycles in user schemas" becomes a real ask, add a separate `validate_schema()` entry point with structured errors. Don't reanimate the cycle warnings for this purpose.
2. **Enrich the "Breaking cycle" log.** Today it logs the cycle path. Could log the *dropped edge* too: `"Breaking FK cycle (Dataset -> Dataset_Version -> Dataset) by dropping edge Dataset_Version -> Dataset; consumers using two-phase-insert will load Dataset rows first with the FK nulled."` Optional, can come later.
