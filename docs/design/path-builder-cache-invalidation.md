# Path-builder cache invalidation (F1 + F2)

**Status:** Proposed
**Date:** 2026-05-16
**Source:** `/Users/carl/GitHub/DerivaML/deriva-ml-model-template/docs/findings/2026-05-16-phase-1-improvements.md` §F1, §F2.
**Prior art:** `de19aaf` (bag-loader passes `refresh=True` defensively) — this plan lets that defensive pass become redundant.

## Problem

`ErmrestCatalog.getPathBuilder()` ([deriva/core/ermrest_catalog.py:388](../../deriva/core/ermrest_catalog.py:388)) caches the wrapper on the instance and only rebuilds when the caller passes `refresh=True`. Two real failure modes today:

1. **Same-instance staleness.** A caller mutates the schema (`POST /schema/Foo/table`) through the same `ErmrestCatalog`, then reads via `getPathBuilder()` — gets the pre-mutation cache. Today every consumer pays the discipline of remembering to refresh. The bag-loader fix (`de19aaf`) is one such defensive `refresh=True`; future consumers will keep stepping on this.
2. **Cross-instance staleness.** Common in async deployments where each handler holds its own `ErmrestCatalog`. `AsyncErmrestCatalog` (see [deriva/core/asyncio/async_catalog.py:73-82](../../deriva/core/asyncio/async_catalog.py:73)) lazily creates a private `_sync_catalog` per instance, so N async catalogs against one server = N independent path-builder caches. Mutating through one doesn't help the others.

## Plan

Two coordinated changes, intended to ship together.

### F1 — auto-invalidate on schema mutation (same-instance)

Override `post`/`put`/`delete` on `ErmrestCatalog`. After a 2xx response whose `path` starts with `/schema`, clear `self._path_builder_cache = None`. Same hook on `AsyncErmrestCatalog.post_async`/`put_async`/`delete_async`, clearing the wrapped `_sync_catalog._path_builder_cache`.

### F2 — cheap `if_stale` freshness check (cross-instance / cross-process)

Extend `getPathBuilder` to `getPathBuilder(refresh=False, if_stale=False)`. When `if_stale=True`, do a `GET /` (returns `{"snaptime": ...}`, a few hundred bytes — see [ermrest_catalog.py:371-377](../../deriva/core/ermrest_catalog.py:371)) and compare to the cached snaptime. Rebuild only on mismatch. Store the snaptime alongside the cache (`self._path_builder_snap`).

Together: F1 eliminates the *defensive* `refresh=True` pattern for callers using one instance. F2 gives callers a cheap correctness check when they can't guarantee single-instance use (async handlers, long-lived MCP servers, notebook kernels).

## Out of scope

- **Thread/async lock around the cache.** Existing `getPathBuilder` already has the same race window with no protection; adding one here without auditing the rest of the catalog state would be inconsistent. Worst case is one wasted `/schema` walk. Documented but not fixed.
- **Sharing one `_sync_catalog` across `AsyncErmrestCatalog` instances** keyed by `(server, catalog_id, creds)`. Bigger refactor, separate change.
- **F3 (auth self-diagnosis), F4 (`InsecureRequestWarning` suppression).** Discussed in the same finding doc but explicitly deferred.

---

## Implementation

### Files

- [`deriva/core/ermrest_catalog.py`](../../deriva/core/ermrest_catalog.py) — F1 verb overrides, F2 `getPathBuilder` extension.
- [`deriva/core/asyncio/async_catalog.py`](../../deriva/core/asyncio/async_catalog.py) — F1 async verb overrides.
- [`tests/deriva/core/test_ermrest_catalog.py`](../../tests/deriva/core/test_ermrest_catalog.py) — new tests for both behaviors. Follows the existing stub pattern (no server required).
- [`deriva/bag/catalog_loader.py`](../../deriva/bag/catalog_loader.py) — remove the now-redundant `refresh=True` from `de19aaf`, *or* leave it (cheap belt-and-suspenders). See "Follow-ups" below.

### F1: `ErmrestCatalog` verb overrides

`ErmrestCatalog` inherits `post`/`put`/`delete` from `DerivaBinding` and currently only overrides `delete` (to guard the whole-catalog deletion case, [ermrest_catalog.py:758-775](../../deriva/core/ermrest_catalog.py:758)). Add overrides for `post` and `put` and extend the existing `delete` override:

```python
def _invalidate_path_builder_if_schema_mutation(self, path: str, response) -> None:
    """Clear the path-builder cache after a successful schema mutation.

    A 2xx non-GET to ``/schema/...`` means the catalog model has changed;
    any cached path-builder wrapper is now stale.
    """
    if path.startswith("/schema") and 200 <= response.status_code < 300:
        self._path_builder_cache = None
        self._path_builder_snap = None

def post(self, path, data=None, json=None, headers=DEFAULT_HEADERS):
    r = DerivaBinding.post(self, path, data=data, json=json, headers=headers)
    self._invalidate_path_builder_if_schema_mutation(path, r)
    return r

def put(self, path, data=None, json=None, headers=DEFAULT_HEADERS, guard_response=None):
    r = DerivaBinding.put(self, path, data=data, json=json, headers=headers,
                          guard_response=guard_response)
    self._invalidate_path_builder_if_schema_mutation(path, r)
    return r

def delete(self, path, headers=DEFAULT_HEADERS, guard_response=None):
    if path == "/":
        raise DerivaPathError(
            'See self.delete_ermrest_catalog() if you really want to destroy this catalog.'
        )
    r = DerivaBinding.delete(self, path, headers=headers, guard_response=guard_response)
    self._invalidate_path_builder_if_schema_mutation(path, r)
    return r
```

Notes:

- Check `status_code` on the returned response; don't trust dispatch alone. A 4xx schema mutation means nothing changed on the server — keep the cache.
- `DerivaBinding.post/put` already call `raise_for_status` internally on some error classes (see [deriva_binding.py:295-340](../../deriva/core/deriva_binding.py:295)); the invalidate check still runs because we inspect `response.status_code` after the underlying method returns. Confirm during implementation that the `raise_for_status` path doesn't prevent reaching the invalidation line for a 2xx; spot-check by reading the parent class.
- `_path_builder_snap` is reset to `None` too so the next `if_stale=True` call will re-probe rather than comparing to a now-orphaned snaptime.

### F2: `getPathBuilder` with `if_stale`

Final implementation contract:

- **Default cold build** (no `refresh`, no `if_stale`): builds the wrapper. **No `GET /` probe.** Strictly backward-compatible — the default code path is zero-network beyond what the existing implementation did.
- **`refresh=True`**: rebuilds unconditionally. No probe. `_path_builder_snap` set to `None`.
- **`if_stale=True`**: probes `GET /`, compares to `_path_builder_snap`. If the snap matches, returns cached wrapper. If the snap differs (which includes the cold-built case where `_path_builder_snap is None`), rebuilds. The probe result is recorded as the new `_path_builder_snap`.

The first `if_stale=True` call after a cold build always rebuilds (because `current_snap != None`). After that, `if_stale=True` settles into a normal compare. Wasteful exactly once per cache generation per instance — acceptable.

```python
def getPathBuilder(self, refresh=False, if_stale=False):
    cache_present = getattr(self, "_path_builder_cache", None) is not None
    cached_snap = getattr(self, "_path_builder_snap", None)
    current_snap = None

    if refresh:
        rebuild = True
    elif not cache_present:
        rebuild = True
    elif if_stale:
        current_snap = self.get('/').json().get('snaptime')
        rebuild = current_snap != cached_snap
        if not rebuild:
            return self._path_builder_cache
    else:
        return self._path_builder_cache

    self._path_builder_cache = datapath.from_catalog(self)
    # Snap is only recorded when the if_stale probe paid for it.
    self._path_builder_snap = current_snap
    return self._path_builder_cache
```

Implementation notes:

- Cold builds and `refresh=True` builds don't probe `GET /`, so the default is zero-network beyond what `from_catalog` already does.
- When `if_stale=True` rebuilds, `current_snap` is the snap we *saw* before the rebuild — the rebuild's `from_catalog` walk may see an even newer snap if another mutator races us. Recording `current_snap` is fine pragmatically (worst case is one extra rebuild on the next call). No lock.
- The first `if_stale=True` after a cold build always rebuilds — one wasted rebuild per cache generation per instance, in exchange for keeping the default cold path zero-network and the API simple.

### F1 (async): `AsyncErmrestCatalog` verb overrides

`AsyncErmrestCatalog.post_async`/`put_async`/`delete_async` ([async_catalog.py:108-136](../../deriva/core/asyncio/async_catalog.py:108)) currently just rewrite the path and delegate to the parent. Add the same status-checked invalidation against `self._sync_catalog._path_builder_cache`. Care: `_sync_catalog` is lazy-initialized via a property ([async_catalog.py:67-82](../../deriva/core/asyncio/async_catalog.py:67)); we don't want the invalidation hook to force-instantiate a sync catalog that the caller never asked for.

```python
async def post_async(self, path, data=None, json_data=None, headers=None):
    response = await super().post_async(self._catalog_uri(path), data, json_data, headers)
    self._invalidate_sync_path_builder_if_schema_mutation(path, response)
    return response

# put_async, delete_async: same pattern.

def _invalidate_sync_path_builder_if_schema_mutation(self, path: str, response) -> None:
    # Use the private attribute, NOT the .sync_catalog property — we don't
    # want to lazy-create a sync catalog just to clear a cache it never had.
    if (
        path.startswith("/schema")
        and self._sync_catalog is not None
        and 200 <= response.status_code < 300
    ):
        self._sync_catalog._path_builder_cache = None
        self._sync_catalog._path_builder_snap = None
```

The httpx response object exposes `.status_code` the same way `requests` does, so the check is uniform.

---

## Tests

All new tests follow the existing pattern in [test_ermrest_catalog.py](../../tests/deriva/core/test_ermrest_catalog.py) — no server required, `datapath.from_catalog` stubbed via `unittest.mock.patch`. For the verb-override tests we also need to stub the underlying `DerivaBinding.post/put/delete` since we're not hitting a real server.

### F1 tests (sync)

1. `test_post_to_schema_invalidates_path_builder_cache` — call `getPathBuilder()`, then `catalog.post("/schema/foo", json={...})` returning a stubbed 200 response. Next `getPathBuilder()` rebuilds.
2. `test_post_to_entity_does_not_invalidate_path_builder_cache` — same but the path is `/entity/foo:bar`. Cache survives.
3. `test_failed_schema_post_does_not_invalidate_path_builder_cache` — stubbed 400/500 response. Cache survives. (Mocking will need to bypass `raise_for_status`; document the assumption.)
4. `test_put_to_schema_invalidates_path_builder_cache` — same shape as #1 for `put`.
5. `test_delete_to_schema_invalidates_path_builder_cache` — same shape as #1 for `delete`. Verify the existing `path == "/"` guard still fires (separate assertion).

### F2 tests

6. `test_get_path_builder_if_stale_does_not_rebuild_when_snaptime_matches` — stub `GET /` to return the same snaptime twice. Cache survives.
7. `test_get_path_builder_if_stale_rebuilds_when_snaptime_advances` — stub `GET /` to return a newer snaptime on the second call. Cache rebuilds; `from_catalog` called again.
8. `test_get_path_builder_records_snaptime_on_cold_build` — first call probes `GET /` once and stores `_path_builder_snap`.
9. `test_get_path_builder_refresh_overrides_if_stale` — passing both flags goes straight to the rebuild path with no `GET /` probe? Actually re-reading the code: with `refresh=True`, the `if_stale` branch is skipped and we probe fresh in the snap-capture line. Test that one `GET /` happens (the snap capture), not two.

### F1 tests (async)

10. `test_async_post_to_schema_invalidates_sync_path_builder_cache` — instantiate `AsyncErmrestCatalog`, access `.sync_catalog` to force the lazy init, prime the path-builder cache, then `await catalog.post_async("/schema/foo", ...)` with a stubbed 200. Cache cleared.
11. `test_async_post_to_schema_skips_invalidation_if_sync_catalog_uninitialized` — `_sync_catalog is None`, post a schema mutation, confirm we don't trigger the property accessor (e.g. assert `_sync_catalog` still `None` afterward).

Stubbing httpx responses for async tests will use the existing async test scaffolding — peek at the current async test suite for the pattern before writing.

---

## Verification checklist

Before declaring done:

- [ ] All new tests pass.
- [ ] Existing `test_ermrest_catalog.py` and `test_datapath.py` tests still pass.
- [ ] Run `tests/deriva/core/test_async_catalog.py` (or equivalent) to confirm async-side changes don't regress.
- [ ] Sanity-check with the deriva-ml bag-loader: confirm the `de19aaf` `refresh=True` becomes truly redundant — call `BagCatalogLoader` *without* the defensive refresh on a fresh catalog and verify it still works.
- [ ] The docstring for `getPathBuilder` reflects the new `if_stale` parameter and the auto-invalidation behavior; future callers shouldn't be surprised.
- [ ] No mention of "thread safety" or "atomicity" claims in the docstring — we explicitly didn't add a lock.

---

## Follow-ups (not in this change)

1. **Revisit `de19aaf` (bag-loader unconditional `refresh=True`).** Once F1 is in, the bag-loader's defensive refresh is redundant for the common case. Two options: (a) remove it for the cleanliness; (b) leave it and document that the F1 work obviates it. Recommend (a) on a separate PR, after F1 has soaked in main for a release cycle.
2. **deriva-ml E1** — the `WARNING:deriva_ml.core.base:schema cache is at snapshot X` after `refresh_schema()` lives in deriva-ml and uses a different cache (`getCatalogModel`/`getCatalogSchema`, not `getPathBuilder`). F1 + F2 do not fix it. Worth coordinating when E1 is addressed: if deriva-ml's cache invalidation pattern can be modeled on F1, the two stories stay consistent.
3. **`AsyncErmrestCatalog` instance sharing.** If "private catalog per async handler" turns into a real performance problem (N `_sync_catalog` instances per process), revisit with an instance-pool keyed on `(server, catalog_id, creds)`. Out of scope here.
4. **Documenting the `if_stale` use case.** Once F2 lands, the README / API docs should mention which use cases want `if_stale=True` vs. accepting the default same-instance invalidation. Concrete examples: long-lived MCP servers, async handlers with private catalog instances, notebook kernels left running across schema edits.
