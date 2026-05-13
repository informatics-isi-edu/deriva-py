"""FK-graph traversal policy shared by producers and the loader.

:class:`FKTraversalPolicy` is the single contract that
:class:`CatalogBagBuilder` (catalog → bag) and
:class:`BagCatalogLoader` (bag → catalog) share. It collects every
decision that matters for either direction of the walk: which
schemas/tables are in scope, how far to walk, how vocabulary
content gets exported, how assets get handled at load time, and
how dangling FKs are resolved.

The policy is deliberately minimal — seven fields, all with
sensible defaults so ``FKTraversalPolicy()`` is a working value
for every producer case we've identified. Things it does *not*
have:

- *No ``direction`` field.* Walks are always bidirectional; no
  real producer case wants single-direction-only. (See ADR-0006.)
- *No ``vocab_direction`` field.* Vocabulary tables are always
  traversed inbound-only by the walker — the rule that prevents
  the Subject → Species → every-other-Subject explosion. It's
  hardcoded because every producer wants the same behavior.
- *No ``force_include``.* "Always include this table" duplicates
  either ``TableAnchor`` (every row) or "let the walk reach it
  via FK-following." Domain-layer wrappers (``DatasetBagBuilder``)
  construct appropriate anchors instead.

Enums use ``StrEnum`` so their values serialize as plain strings
into the bag's ``metadata/`` provenance file and parse back
without ambiguity. The same enum types are imported by
:class:`BagCatalogLoader` — they're contractual values, not
loader-internal constants.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field, field_validator


# =============================================================================
# Enums — contractual values shared between policy and consumers
# =============================================================================


class VocabExport(StrEnum):
    """How vocabulary tables are exported into the bag.

    The walker always traverses *into* vocab tables (pulling the
    referenced terms) and never *out of* them (avoiding loop-back
    to every table that uses the vocab). This enum decides what
    ends up in the bag *after* the walk:

    - ``REFERENCED_ONLY``: only terms the walk actually pulled in
      land in the bag's CSV. Default.
    - ``FULL``: after the walk, a separate pass copies *every*
      term in each reachable vocab table. Used by dataset bags
      whose consumers need the complete controlled vocabulary
      (today's ``CatalogGraph._export_vocabulary`` behavior).
    """

    REFERENCED_ONLY = "referenced_only"
    FULL = "full"


class AssetMode(StrEnum):
    """How :class:`BagCatalogLoader` handles asset tables.

    All modes delegate per-asset work to deriva-py's existing
    ``DerivaUpload._uploadAsset`` recipe, which already does
    Hatrac byte-dedupe (HEAD + MD5), catalog row reconciliation,
    pre-allocated RID handling, and transfer-state resumption.

    - ``ROWS_ONLY``: insert/reconcile asset rows with the bag's
      ``URL`` column unchanged. No bytes transferred. Destination
      depends on the source's Hatrac being reachable.
    - ``UPLOAD_IF_MISSING``: insert/reconcile rows; for each
      asset, Hatrac is HEAD-checked and bytes are uploaded only
      when the destination's MD5 differs. The right semantic for
      round-trip clone, re-import, and any idempotent re-run.
      **Default.**
    - ``UPLOAD_FORCE``: insert/reconcile rows; bytes re-uploaded
      unconditionally (Hatrac ``force=True``). Reserved for
      "source bytes are authoritative; overwrite" scenarios.

    "Don't include this asset table at all" is not an
    ``AssetMode`` value — use
    :attr:`FKTraversalPolicy.exclude_tables` for that. Keeping
    scope and behavior in separate fields avoids the "I included
    X but set mode to SKIP — which wins?" ambiguity.
    """

    ROWS_ONLY = "rows_only"
    UPLOAD_IF_MISSING = "upload_if_missing"
    UPLOAD_FORCE = "upload_force"


class DanglingFKStrategy(StrEnum):
    """How the loader resolves orphan rows.

    An *orphan* (or *dangling-FK row*) is a row in the bag whose
    FK reference points at a parent not in the bag — usually
    because of incoherent row-level policies at the source
    catalog (the child row was visible to the cloning user but
    the parent wasn't).

    - ``FAIL``: abort the load when orphans are detected.
      Conservative; the caller adjusts scope or fixes the source
      catalog. **Default.**
    - ``DELETE``: drop orphan rows entirely. The destination ends
      up with fewer rows than the bag.
    - ``NULLIFY``: set the dangling FK column to ``NULL`` (only
      legal when the column is nullable). The row stays; only
      the broken reference disappears.
    - ``PRESERVE``: skip the bag-side parent-row check entirely
      and trust the destination catalog's FK constraint to be
      authoritative. Bag rows are sent verbatim; if the
      destination doesn't have the referenced parent, ERMrest's
      insert-time FK check fires (HTTP 409). Use when the bag
      legitimately references parents that exist at the
      destination but were never in the bag — e.g., the
      end-of-execution upload, where output Image rows reference
      Subject parents that were created in an earlier execution
      and already live in the destination catalog.

    Detection happens at *bag-read time* in
    :class:`BagDatabase`; application happens at *load time* in
    :class:`BagCatalogLoader`. :class:`CatalogBagBuilder` itself
    is orphan-agnostic — it writes the walk's output as-is.
    """

    FAIL = "fail"
    DELETE = "delete"
    NULLIFY = "nullify"
    PRESERVE = "preserve"


class ContentConflictStrategy(StrEnum):
    """How the loader resolves RID collisions on **content** tables.

    Content tables (everything that isn't a vocabulary table or a
    system schema) are addressed by RID end-to-end. The destination
    is expected to be empty for these tables; a colliding RID means
    one of two things, and the strategy lets the caller declare
    which:

    - ``FAIL``: abort on any content RID collision. **Default.**
      The destination already had a row at our RID, which usually
      means a prior partial load; re-running blindly would mask
      data loss. Conservative.
    - ``SKIP_BY_RID``: silently skip rows whose RID already exists
      on the destination. The explicit "I know there's a partial
      load, keep going" signal — used for resumable loads.

    Vocabulary tables have their own conflict policy
    (:class:`~deriva.bag.catalog_loader.BagCatalogLoader`'s
    match-by-name + RID remap path); this strategy does **not**
    apply to them.

    Per ADR-0001, the default of ``FAIL`` matches the precondition
    that a destination prepared via ``create_ml_catalog`` has the
    schema and system vocabulary, but the data tables are empty.
    """

    FAIL = "fail"
    SKIP_BY_RID = "skip_by_rid"


# =============================================================================
# FKTraversalPolicy
# =============================================================================


#: Default schemas excluded from every walk.
#:
#: These are ERMrest's structural / administrative schemas; they
#: never carry domain content and pulling them in would inflate
#: bag size and risk leaking access-control rows.
DEFAULT_EXCLUDE_SCHEMAS: frozenset[str] = frozenset(
    {"public", "_acl_admin", "WWW"}
)


class FKTraversalPolicy(BaseModel):
    """Configure how producers walk a catalog and how the loader writes one.

    The policy is the single object that travels through the
    pipeline: producers consume it to drive their FK walk;
    :class:`BagCatalogLoader` consumes it to drive asset handling
    + dangling-FK resolution. The same instance can be serialized
    into the bag's ``metadata/`` for round-trip provenance.

    Every field has a default. ``FKTraversalPolicy()`` is a valid
    value that captures the most common producer case (a
    bidirectional walk across the source catalog's domain
    schemas, with sensible asset and orphan defaults).

    Attributes:
        schemas: ERMrest schema allow-list. ``None`` (default)
            means "every schema reachable from the anchors except
            those in :attr:`exclude_schemas`."
        exclude_schemas: ERMrest schemas to skip during the walk.
            Defaults to :data:`DEFAULT_EXCLUDE_SCHEMAS` (``public``,
            ``_acl_admin``, ``WWW``).
        exclude_tables: Specific tables to skip, as
            ``{(schema, table), ...}`` tuples. Useful for
            domain-specific filtering (e.g., dataset associations
            for element types with no members).
        terminal_tables: Tables the walker enters but only
            partially exits, as ``{(schema, table), ...}`` tuples.
            The walker emits rows for the table (so other rows'
            FKs into it resolve at load time) and follows its
            **outbound** FKs (so the rows the terminal table
            *references* land in the slice), but does **not**
            follow its **inbound** FKs (the FKs other tables
            declare *at* the terminal table). That asymmetry is
            the whole point: inbound traversal is what aggregates
            cross-anchor state. From a terminal ``Execution`` row,
            inbound goes to every ``*_Execution`` association and
            from there to every other anchor scope sharing the
            ``Execution`` — exactly the over-fetch this rule
            exists to prevent.

            Applied to non-vocab "provenance" tables that
            aggregate cross-anchor state — e.g. ``Execution``
            and ``Workflow`` in the deriva-ml schema. Empty by
            default; callers opt in based on schema-domain
            knowledge. See ``catalog_builder.py:_expand_table``
            for the implementation of the outbound-only rule.
        max_depth: Maximum FK hops from the anchor set. ``None``
            (default) means unbounded.
        vocab_export: How vocabularies are exported. See
            :class:`VocabExport`. Default ``REFERENCED_ONLY``.
        asset_mode: How the loader handles asset tables. See
            :class:`AssetMode`. Default ``UPLOAD_IF_MISSING``.
        dangling_fk_strategy: How the loader resolves orphan
            rows. See :class:`DanglingFKStrategy`. Default
            ``FAIL``.
        content_on_conflict: How the loader resolves RID collisions
            on **content** tables (non-vocabulary, non-system).
            See :class:`ContentConflictStrategy`. Default ``FAIL``.
            Vocabulary tables use match-by-name regardless of this
            setting; tables listed in :attr:`match_by_columns` use
            match-by-supplied-columns regardless of this setting.
        match_by_columns: Per-table caller-supplied "reconcile by
            these columns" rule. Maps ``(schema_name, table_name)``
            to a list of column names that uniquely identify a row
            on the destination. For each bag row in such a table,
            the loader queries the destination by those columns;
            if a match is found, the bag row is **not** inserted
            and the source RID is remapped to the destination's
            RID (so child rows that FK-reference it get rewritten
            to the destination's RID at insert time). If no match,
            the bag row is inserted normally and an identity remap
            entry is recorded.

            This generalises the vocabulary match-by-``Name``
            behaviour to non-vocabulary tables that nevertheless
            have a content-addressed unique key (e.g. asset tables
            whose ``URL`` is hash-derived and stable across
            executions). Empty dict (default) leaves all
            non-vocabulary tables on the standard content path.

            Empty column lists are rejected at validation time —
            a table either has a match rule or it doesn't.
        preserve_provenance: Whether to preserve the bag's source
            audit columns (``RCT`` creation time, ``RCB`` creating
            user) at insert time. ``True`` (default) sends the bag
            row's values verbatim using ERMrest's
            ``?nondefaults=RID,RCT,RCB`` — the clone-style
            semantics where audit data from the source catalog is
            real history worth keeping. Set ``False`` when the bag
            carries *new* rows the destination is generating
            (e.g. end-of-execution commit): only ``RID`` is
            preserved, ``RCT`` and ``RCB`` are server-defaulted to
            the current timestamp and the current user. Without
            this, new rows whose bag carries no ``RCB`` value get
            sent with NULL ``RCB`` and ERMrest rejects them with
            ``Image_RCB_fkey`` constraint failures.

    Example:
        >>> # Default — works for any producer case.
        >>> p = FKTraversalPolicy()
        >>> p.asset_mode
        <AssetMode.UPLOAD_IF_MISSING: 'upload_if_missing'>
        >>> p.dangling_fk_strategy
        <DanglingFKStrategy.FAIL: 'fail'>

        >>> # Customized: include the dataset's full vocabulary,
        >>> # only re-upload missing assets, allow orphan deletion.
        >>> from deriva.bag.traversal import (
        ...     AssetMode,
        ...     DanglingFKStrategy,
        ...     VocabExport,
        ... )
        >>> p = FKTraversalPolicy(
        ...     vocab_export=VocabExport.FULL,
        ...     asset_mode=AssetMode.UPLOAD_IF_MISSING,
        ...     dangling_fk_strategy=DanglingFKStrategy.DELETE,
        ... )
        >>> p.vocab_export
        <VocabExport.FULL: 'full'>
    """

    schemas: set[str] | None = None
    exclude_schemas: set[str] = Field(
        default_factory=lambda: set(DEFAULT_EXCLUDE_SCHEMAS)
    )
    exclude_tables: set[tuple[str, str]] = Field(default_factory=set)
    terminal_tables: set[tuple[str, str]] = Field(default_factory=set)
    max_depth: int | None = None
    vocab_export: VocabExport = VocabExport.REFERENCED_ONLY
    asset_mode: AssetMode = AssetMode.UPLOAD_IF_MISSING
    dangling_fk_strategy: DanglingFKStrategy = DanglingFKStrategy.FAIL
    content_on_conflict: ContentConflictStrategy = ContentConflictStrategy.FAIL
    match_by_columns: dict[tuple[str, str], list[str]] = Field(
        default_factory=dict
    )
    preserve_provenance: bool = True

    @field_validator("max_depth")
    @classmethod
    def _max_depth_nonnegative(cls, v: int | None) -> int | None:
        # ``None`` is the unbounded sentinel; numeric values must be
        # ≥ 0 (a depth of 0 means "only the anchored rows themselves").
        if v is not None and v < 0:
            raise ValueError(
                "max_depth must be a non-negative integer or None"
            )
        return v

    @field_validator("match_by_columns")
    @classmethod
    def _match_by_columns_non_empty(
        cls,
        v: dict[tuple[str, str], list[str]],
    ) -> dict[tuple[str, str], list[str]]:
        # An empty column list for a table means "match by nothing",
        # which is meaningless — every existing row would match.
        # Make the caller delete the entry instead.
        for key, cols in v.items():
            if not cols:
                raise ValueError(
                    f"match_by_columns[{key!r}] is empty; drop the "
                    f"entry rather than supply an empty column list."
                )
        return v

    def validate_with_bag_state(self, *, holey: bool) -> None:
        """Reject the ``holey + UPLOAD_*`` combinations.

        Called by :class:`BagCatalogLoader` before any rows are
        inserted. A holey bag has no local asset bytes; the
        ``UPLOAD_IF_MISSING`` and ``UPLOAD_FORCE`` modes require
        bytes to transfer, so we fail fast with a clear pointer at
        the workaround (materialize the bag first, or switch to
        ``ROWS_ONLY``).

        Args:
            holey: ``True`` if the bag has a non-empty
                ``fetch.txt`` whose referenced files have not been
                materialized.

        Raises:
            ValueError: If ``holey`` and the asset mode requires
                local bytes.
        """
        if holey and self.asset_mode in (
            AssetMode.UPLOAD_IF_MISSING,
            AssetMode.UPLOAD_FORCE,
        ):
            raise ValueError(
                f"asset_mode={self.asset_mode.value!r} is incompatible "
                f"with a holey bag (fetch.txt references not yet "
                f"materialized). Either run bdb.materialize(bag) first, "
                f"or switch to asset_mode={AssetMode.ROWS_ONLY.value!r}."
            )


__all__ = [
    "AssetMode",
    "ContentConflictStrategy",
    "DanglingFKStrategy",
    "DEFAULT_EXCLUDE_SCHEMAS",
    "FKTraversalPolicy",
    "VocabExport",
]
