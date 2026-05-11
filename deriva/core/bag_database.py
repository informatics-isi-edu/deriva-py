"""Re-export shim for the legacy ``deriva.core.bag_database`` path.

The actual implementation now lives in :mod:`deriva.bag.database`.
This module re-exports it so that existing callers (deriva-ml's
``DatabaseModel``, scripts that imported from the old location) keep
working without modification.

New code should import directly from :mod:`deriva.bag.database` or
from :mod:`deriva.bag` (once the public API stabilizes there). The
shim is expected to be removed after the deriva-ml migration PR
lands and any other in-tree consumers have updated their imports.
"""

from __future__ import annotations

from deriva.bag.database import (  # noqa: F401  (re-export)
    ASSET_COLUMNS,
    BagDatabase,
    ERMRestBoolean,
    StringToDate,
    StringToDateTime,
    StringToFloat,
    StringToInteger,
)

__all__ = [
    "ASSET_COLUMNS",
    "BagDatabase",
    "ERMRestBoolean",
    "StringToDate",
    "StringToDateTime",
    "StringToFloat",
    "StringToInteger",
]
