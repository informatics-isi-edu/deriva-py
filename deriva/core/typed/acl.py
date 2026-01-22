"""Typed ACL (Access Control List) definitions for ERMrest resources.

This module provides dataclass-based typed interfaces for defining access
control lists and ACL bindings, replacing the dict-based ACL configurations
used throughout `deriva.core.ermrest_model`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from deriva.core.typed.types import AclMode


@dataclass
class Acl:
    """Access Control List for ERMrest resources.

    ACLs control who can perform various operations on catalogs, schemas,
    tables, columns, and foreign keys. Each ACL mode can have a list of
    authorized roles/identifiers.

    Attributes:
        owner: Roles with full ownership rights (includes all other modes).
        enumerate: Roles that can see the resource exists.
        select: Roles that can read data.
        insert: Roles that can insert new data.
        update: Roles that can modify existing data.
        delete: Roles that can delete data.
        write: Roles with combined insert, update, and delete permissions.

    Special Values:
        - "*": Public access (any authenticated or anonymous user)
        - Empty list []: No access for this mode
        - None: Inherit from parent resource

    Example:
        >>> acl = Acl(
        ...     owner=["admin"],
        ...     enumerate=["*"],
        ...     select=["researchers", "analysts"],
        ...     insert=["data_entry"],
        ... )
        >>> acl.to_dict()
        {'owner': ['admin'], 'enumerate': ['*'], 'select': ['researchers', 'analysts'], ...}

        >>> # Read-only for everyone, write for admins
        >>> acl = Acl.read_only(owner=["admin"])
    """

    owner: list[str] | None = None
    enumerate: list[str] | None = None
    select: list[str] | None = None
    insert: list[str] | None = None
    update: list[str] | None = None
    delete: list[str] | None = None
    write: list[str] | None = None

    def to_dict(self) -> dict[str, list[str]]:
        """Convert to the dict format expected by ERMrest API.

        Only includes modes with non-None values.

        Returns:
            A dictionary mapping ACL modes to lists of roles.
        """
        result = {}
        if self.owner is not None:
            result["owner"] = self.owner
        if self.enumerate is not None:
            result["enumerate"] = self.enumerate
        if self.select is not None:
            result["select"] = self.select
        if self.insert is not None:
            result["insert"] = self.insert
        if self.update is not None:
            result["update"] = self.update
        if self.delete is not None:
            result["delete"] = self.delete
        if self.write is not None:
            result["write"] = self.write
        return result

    @classmethod
    def from_dict(cls, d: dict[str, list[str]]) -> Acl:
        """Create an Acl from a dictionary representation.

        Args:
            d: A dictionary mapping ACL modes to role lists.

        Returns:
            An Acl instance with the parsed values.
        """
        return cls(
            owner=d.get("owner"),
            enumerate=d.get("enumerate"),
            select=d.get("select"),
            insert=d.get("insert"),
            update=d.get("update"),
            delete=d.get("delete"),
            write=d.get("write"),
        )

    @classmethod
    def public_read(cls, owner: list[str] | None = None) -> Acl:
        """Create ACL with public read access.

        Args:
            owner: Optional owner roles.

        Returns:
            An Acl allowing anyone to read but restricting writes.
        """
        return cls(
            owner=owner,
            enumerate=["*"],
            select=["*"],
        )

    @classmethod
    def read_only(cls, owner: list[str] | None = None) -> Acl:
        """Create a read-only ACL.

        Args:
            owner: Optional owner roles.

        Returns:
            An Acl preventing all modifications.
        """
        return cls(
            owner=owner,
            enumerate=["*"],
            select=["*"],
            insert=[],
            update=[],
            delete=[],
        )

    @classmethod
    def restricted(cls, owner: list[str], allowed: list[str]) -> Acl:
        """Create a restricted ACL for specific roles.

        Args:
            owner: Owner roles.
            allowed: Roles allowed to read and write.

        Returns:
            An Acl restricted to the specified roles.
        """
        return cls(
            owner=owner,
            enumerate=allowed,
            select=allowed,
            insert=allowed,
            update=allowed,
            delete=allowed,
        )


@dataclass
class AclBinding:
    """Dynamic ACL binding for fine-grained access control.

    ACL bindings allow dynamic access control based on data values within
    rows. For example, granting users access only to their own records.

    Attributes:
        projection: Column or path expression to evaluate for access check.
            This should resolve to a value that can be matched against the
            current user's identity.
        projection_type: Type of the projection result.
        types: List of binding types (e.g., "owner", "select").
        scope_acl: Optional ACL modes this binding applies to.

    Example:
        >>> # Users can only see their own records
        >>> binding = AclBinding(
        ...     projection="Owner",
        ...     projection_type="acl",
        ...     types=["owner"],
        ... )

        >>> # Access based on group membership
        >>> binding = AclBinding(
        ...     projection=[{"outbound": ["schema", "fkey"]}, "Group"],
        ...     projection_type="acl",
        ...     types=["select", "update"],
        ... )
    """

    projection: str | list[Any]
    projection_type: Literal["acl"] = "acl"
    types: list[str] = field(default_factory=lambda: ["owner"])
    scope_acl: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the dict format expected by ERMrest API.

        Returns:
            A dictionary with the ACL binding configuration.
        """
        result: dict[str, Any] = {
            "projection": self.projection,
            "projection_type": self.projection_type,
            "types": self.types,
        }
        if self.scope_acl is not None:
            result["scope_acl"] = self.scope_acl
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> AclBinding:
        """Create an AclBinding from a dictionary representation.

        Args:
            d: A dictionary with ACL binding fields.

        Returns:
            An AclBinding instance with the parsed values.
        """
        return cls(
            projection=d.get("projection", ""),
            projection_type=d.get("projection_type", "acl"),
            types=d.get("types", ["owner"]),
            scope_acl=d.get("scope_acl"),
        )

    @classmethod
    def self_service(cls, owner_column: str = "RCB") -> AclBinding:
        """Create a self-service binding where users can manage their own records.

        Args:
            owner_column: Column containing the owner identity.
                Defaults to "RCB" (record created by).

        Returns:
            An AclBinding granting owner access to record creators.
        """
        return cls(
            projection=owner_column,
            projection_type="acl",
            types=["owner"],
        )


@dataclass
class AclBindings:
    """Collection of named ACL bindings for a resource.

    Each binding has a unique name and defines dynamic access rules.

    Attributes:
        bindings: Dictionary mapping binding names to AclBinding instances.

    Example:
        >>> bindings = AclBindings({
        ...     "self_service": AclBinding.self_service(),
        ...     "group_access": AclBinding(
        ...         projection=[{"outbound": ["domain", "user_group_fkey"]}, "Group"],
        ...         types=["select"],
        ...     ),
        ... })
    """

    bindings: dict[str, AclBinding] = field(default_factory=dict)

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """Convert to the dict format expected by ERMrest API.

        Returns:
            A dictionary mapping binding names to binding configurations.
        """
        return {name: binding.to_dict() for name, binding in self.bindings.items()}

    @classmethod
    def from_dict(cls, d: dict[str, dict[str, Any]]) -> AclBindings:
        """Create AclBindings from a dictionary representation.

        Args:
            d: A dictionary mapping binding names to binding configurations.

        Returns:
            An AclBindings instance with the parsed bindings.
        """
        return cls(
            bindings={name: AclBinding.from_dict(config) for name, config in d.items()}
        )

    def add(self, name: str, binding: AclBinding) -> AclBindings:
        """Return a new AclBindings with an additional binding.

        Args:
            name: Name for the binding.
            binding: The ACL binding configuration.

        Returns:
            A new AclBindings with the binding added.
        """
        new_bindings = dict(self.bindings)
        new_bindings[name] = binding
        return AclBindings(bindings=new_bindings)


# Convenience functions for foreign key ACLs


def fkey_default_acls() -> Acl:
    """Create the default ACL for foreign keys.

    By default, foreign keys allow insert and update for all users,
    which permits creating references to the referenced table.

    Returns:
        An Acl with insert=["*"] and update=["*"].
    """
    return Acl(insert=["*"], update=["*"])
