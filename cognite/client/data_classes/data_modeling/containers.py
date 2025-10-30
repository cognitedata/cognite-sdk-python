from __future__ import annotations

from abc import ABC, abstractmethod
from builtins import type as type_
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteObject,
    CogniteResourceList,
    UnknownCogniteObject,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.core import DataModelingSchemaResource
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelation,
    PropertyType,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.utils._text import to_camel_case

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

_T_ContainerCore = TypeVar("_T_ContainerCore", bound="ContainerCore")
_T_ContainerPropertyCore = TypeVar("_T_ContainerPropertyCore", bound="ContainerPropertyCore")


@dataclass(frozen=True)
class ContainerCore(DataModelingSchemaResource["ContainerApply"], ABC):
    """Represent the physical storage of data. This is the base class for the read and write version.

    Args:
        space (str): The workspace for the container, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the container.
        properties (Mapping[str, ContainerPropertyCore]): We index the property by a local unique identifier.
        description (str | None): Textual description of the container
        name (str | None): Human readable name for the container.
        constraints (Mapping[str, ConstraintCore] | None): Set of constraints to apply to the container
        indexes (Mapping[str, IndexCore] | None): Set of indexes to apply to the container.
    """

    space: str
    external_id: str
    properties: Mapping[str, ContainerPropertyCore]
    description: str | None = None
    name: str | None = None
    constraints: Mapping[str, ConstraintCore] = field(default_factory=dict)
    indexes: Mapping[str, IndexCore] = field(default_factory=dict)

    @staticmethod
    def _load_base(
        resource: dict[str, Any],
        container_class: type_[_T_ContainerCore],
        property_loader: Callable[[dict[str, Any]], ContainerPropertyCore],
        constraint_loader: Callable[[dict[str, Any]], ConstraintCore],
        index_loader: Callable[[dict[str, Any]], IndexCore],
        **extra_kwargs: Any,
    ) -> _T_ContainerCore:
        """Load common container data and construct container class instance."""
        properties = {k: property_loader(v) for k, v in resource["properties"].items()}
        constraints = {k: constraint_loader(v) for k, v in resource.get("constraints", {}).items()}
        indexes = {k: index_loader(v) for k, v in resource.get("indexes", {}).items()}
        return container_class(
            space=resource["space"],
            external_id=resource["externalId"],
            properties=properties,
            description=resource.get("description"),
            name=resource.get("name"),
            constraints=constraints,
            indexes=indexes,
            **extra_kwargs,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.constraints:
            output["constraints"] = {k: v.dump(camel_case) for k, v in self.constraints.items()}
        if self.indexes:
            output["indexes"] = {k: v.dump(camel_case) for k, v in self.indexes.items()}
        if self.properties:
            output["properties"] = {k: v.dump(camel_case) for k, v in self.properties.items()}

        return output

    def as_id(self) -> ContainerId:
        return ContainerId(self.space, self.external_id)

    def as_property_ref(self, property: str) -> tuple[str, str, str]:
        return self.as_id().as_property_ref(property)


@dataclass(frozen=True)
class ContainerApply(ContainerCore):
    """Represent the physical storage of data. This is the write format of the container

    Args:
        space (str): The workspace for the container, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the container.
        properties (Mapping[str, ContainerPropertyApply]): We index the property by a local unique identifier.
        description (str | None): Textual description of the container
        name (str | None): Human readable name for the container.
        used_for (Literal['node', 'edge', 'all'] | None): Should this operation apply to nodes, edges or both.
        constraints (Mapping[str, ConstraintApply] | None): Set of constraints to apply to the container
        indexes (Mapping[str, IndexApply] | None): Set of indexes to apply to the container.
    """

    properties: Mapping[str, ContainerPropertyApply]
    constraints: Mapping[str, ConstraintApply] = field(default_factory=dict)
    indexes: Mapping[str, IndexApply] = field(default_factory=dict)
    used_for: Literal["node", "edge", "all"] | None = None

    def __post_init__(self) -> None:
        validate_data_modeling_identifier(self.space, self.external_id)

    @classmethod
    def _load(cls, resource: dict, cognite_client: AsyncCogniteClient | None = None) -> ContainerApply:
        return cls._load_base(
            resource,
            container_class=cls,
            property_loader=ContainerPropertyApply._load,
            constraint_loader=ConstraintApply._load,
            index_loader=IndexApply._load,
            used_for=resource.get("usedFor"),
        )

    def as_write(self) -> ContainerApply:
        """Returns this ContainerApply instance."""
        return self


@dataclass(frozen=True, kw_only=True)
class Container(ContainerCore):
    """Represent the physical storage of data. This is the read format of the container

    Args:
        space (str): The workspace for the container, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the container.
        properties (Mapping[str, ContainerProperty]): We index the property by a local unique identifier.
        is_global (bool): Whether this is a global container, i.e., one of the out-of-the-box models.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description (str | None): Textual description of the container
        name (str | None): Human readable name for the container.
        used_for (Literal['node', 'edge', 'all']): Should this operation apply to nodes, edges or both.
        constraints (Mapping[str, Constraint] | None): Set of constraints to apply to the container
        indexes (Mapping[str, Index] | None): Set of indexes to apply to the container.
    """

    properties: Mapping[str, ContainerProperty]
    constraints: Mapping[str, Constraint]
    indexes: Mapping[str, Index]
    is_global: bool
    last_updated_time: int
    created_time: int
    used_for: Literal["node", "edge", "all"]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls._load_base(
            resource,
            container_class=cls,
            property_loader=ContainerProperty._load,
            constraint_loader=Constraint._load,
            index_loader=Index._load,
            is_global=resource["isGlobal"],
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
            used_for=resource["usedFor"],
        )

    def as_apply(self) -> ContainerApply:
        return ContainerApply(
            space=self.space,
            external_id=self.external_id,
            properties={k: p.as_apply() for k, p in self.properties.items()},
            description=self.description,
            name=self.name,
            used_for=self.used_for,
            constraints={k: c.as_apply() for k, c in self.constraints.items()},
            indexes={k: i.as_apply() for k, i in self.indexes.items()},
        )

    def as_write(self) -> ContainerApply:
        return self.as_apply()


class ContainerApplyList(CogniteResourceList[ContainerApply]):
    _RESOURCE = ContainerApply

    def as_ids(self) -> list[ContainerId]:
        """Convert to a container id list.

        Returns:
            list[ContainerId]: The container id list.
        """
        return [v.as_id() for v in self]


class ContainerList(WriteableCogniteResourceList[ContainerApply, Container]):
    _RESOURCE = Container

    def as_apply(self) -> ContainerApplyList:
        """Convert to a ContainerApply list.

        Returns:
            ContainerApplyList: The container apply list.
        """
        return ContainerApplyList(resources=[v.as_apply() for v in self])

    def as_ids(self) -> list[ContainerId]:
        """Convert to a container id list.

        Returns:
            list[ContainerId]: The container id list.
        """
        return [v.as_id() for v in self]

    def as_write(self) -> ContainerApplyList:
        return self.as_apply()


class _ContainerFilter(CogniteFilter):
    def __init__(self, space: str | None = None, include_global: bool = False) -> None:
        self.space = space
        self.include_global = include_global


@dataclass(frozen=True)
class PropertyConstraintState(CogniteObject):
    nullability: Literal["current", "failed", "pending"] | None = None
    max_list_size: Literal["current", "failed", "pending"] | None = None
    max_text_size: Literal["current", "failed", "pending"] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            nullability=resource.get("nullability"),
            max_list_size=resource.get("maxListSize"),
            max_text_size=resource.get("maxTextSize"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, str]:
        output = {}
        for key in ["nullability", "max_list_size", "max_text_size"]:
            if (value := getattr(self, key)) is not None:
                output[to_camel_case(key) if camel_case else key] = value
        return output


@dataclass(frozen=True)
class ContainerPropertyCore(CogniteObject, ABC):
    """Base class for container properties with shared functionality."""

    type: PropertyType
    nullable: bool = True
    auto_increment: bool = False
    name: str | None = None
    default_value: str | int | float | bool | dict | None = None
    description: str | None = None
    immutable: bool = False

    def __post_init__(self) -> None:
        # We allow passing e.g. Int32 instead of Int32():
        if not isinstance(self.type, PropertyType):
            object.__setattr__(self, "type", self.type())

    @staticmethod
    def _load_base(
        resource: dict[str, Any], property_class: type_[_T_ContainerPropertyCore], **extra_kwargs: Any
    ) -> _T_ContainerPropertyCore:
        """Load common property data and construct property class instance."""
        if "type" not in resource:
            raise ValueError("Type not specified")
        if resource["type"].get("type") == "direct":
            type_: PropertyType = DirectRelation._load(resource["type"])
        else:
            type_ = PropertyType._load(resource["type"])
        return property_class(
            type=type_,
            nullable=resource.get("nullable", True),
            auto_increment=resource.get("autoIncrement", False),
            name=resource.get("name"),
            default_value=resource.get("defaultValue"),
            description=resource.get("description"),
            immutable=resource.get("immutable", False),
            **extra_kwargs,
        )

    @abstractmethod
    def as_apply(self) -> ContainerPropertyApply:
        """Return the write version of this property."""
        raise NotImplementedError

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if self.type:
            output["type"] = self.type.dump(camel_case)
        output["immutable"] = self.immutable
        for key in ["nullable", "auto_increment", "name", "default_value", "description"]:
            if (value := getattr(self, key)) is not None:
                output[to_camel_case(key) if camel_case else key] = value
        return output


@dataclass(frozen=True)
class ContainerPropertyApply(ContainerPropertyCore):
    """Write version of container property."""

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls._load_base(resource, property_class=cls)

    def as_apply(self) -> ContainerPropertyApply:
        """Return this ContainerPropertyApply instance."""
        return self


@dataclass(frozen=True)
class ContainerProperty(ContainerPropertyCore):
    """Read version of container property with state information."""

    constraint_state: PropertyConstraintState = field(default_factory=PropertyConstraintState)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        constraint_state = PropertyConstraintState._load(resource["constraintState"])
        return cls._load_base(resource, property_class=cls, constraint_state=constraint_state)

    def as_apply(self) -> ContainerPropertyApply:
        return ContainerPropertyApply(
            type=self.type,
            nullable=self.nullable,
            auto_increment=self.auto_increment,
            name=self.name,
            default_value=self.default_value,
            description=self.description,
            immutable=self.immutable,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["constraintState" if camel_case else "constraint_state"] = self.constraint_state.dump(camel_case)
        return output


@dataclass(frozen=True)
class ConstraintCore(CogniteObject, ABC):
    """Base class for constraints with shared functionality."""

    @abstractmethod
    def as_apply(self) -> ConstraintApply:
        """Return the write version of this constraint."""
        raise NotImplementedError

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class ConstraintApply(ConstraintCore, ABC):
    """Write version of constraint without state information."""

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> ConstraintApply:
        match resource["constraintType"]:
            case "requires":
                return RequiresConstraintApply._load(resource)
            case "uniqueness":
                return UniquenessConstraintApply._load(resource)
            case _:
                return cast(ConstraintApply, UnknownCogniteObject(resource))

    def as_apply(self) -> ConstraintApply:
        """Return this ConstraintApply instance."""
        return self


@dataclass(frozen=True)
class Constraint(ConstraintCore, ABC):
    """Read version of constraint with state information."""

    state: Literal["current", "failed", "pending"]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Constraint:
        match resource["constraintType"]:
            case "requires":
                return RequiresConstraint._load(resource, cognite_client)
            case "uniqueness":
                return UniquenessConstraint._load(resource, cognite_client)
            case _:
                return cast(Constraint, UnknownCogniteObject(resource))


@dataclass(frozen=True)
class RequiresConstraintApply(ConstraintApply):
    """Write version of requires constraint."""

    require: ContainerId

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        require = ContainerId.load(resource["require"])
        return cls(require=require)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"require": self.require.dump(camel_case)}
        key = "constraintType" if camel_case else "constraint_type"
        output[key] = "requires"
        return output


@dataclass(frozen=True, kw_only=True)
class RequiresConstraint(Constraint):
    """Read version of requires constraint with state information."""

    require: ContainerId

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        require = ContainerId.load(resource["require"])
        state = resource["state"]
        return cls(require=require, state=state)

    def as_apply(self) -> RequiresConstraintApply:
        return RequiresConstraintApply(require=self.require)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"require": self.require.dump(camel_case)}
        output["state"] = self.state
        key = "constraintType" if camel_case else "constraint_type"
        output[key] = "requires"
        return output


@dataclass(frozen=True)
class UniquenessConstraintApply(ConstraintApply):
    """Write version of uniqueness constraint."""

    properties: list[str]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        properties = resource["properties"]
        return cls(properties=properties)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"properties": self.properties}
        key = "constraintType" if camel_case else "constraint_type"
        output[key] = "uniqueness"
        return output


@dataclass(frozen=True)
class UniquenessConstraint(Constraint):
    """Read version of uniqueness constraint with state information."""

    properties: list[str]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        properties = resource["properties"]
        state = resource["state"]
        return cls(properties=properties, state=state)

    def as_apply(self) -> UniquenessConstraintApply:
        return UniquenessConstraintApply(properties=self.properties)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"properties": self.properties}
        output["state"] = self.state
        key = "constraintType" if camel_case else "constraint_type"
        output[key] = "uniqueness"
        return output


@dataclass(frozen=True)
class IndexCore(CogniteObject, ABC):
    """Base class for indexes with shared functionality."""

    @abstractmethod
    def as_apply(self) -> IndexApply:
        """Return the write version of this index."""
        raise NotImplementedError

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class IndexApply(IndexCore, ABC):
    """Write version of index without state information."""

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> IndexApply:
        match resource["indexType"]:
            case "btree":
                return BTreeIndexApply._load(resource)
            case "inverted":
                return InvertedIndexApply._load(resource)
            case _:
                return cast(IndexApply, UnknownCogniteObject(resource))

    def as_apply(self) -> IndexApply:
        """Return this IndexApply instance."""
        return self


@dataclass(frozen=True)
class Index(IndexCore, ABC):
    """Read version of index with state information."""

    state: Literal["current", "failed", "pending"]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Index:
        match resource["indexType"]:
            case "btree":
                return BTreeIndex._load(resource, cognite_client)
            case "inverted":
                return InvertedIndex._load(resource, cognite_client)
            case _:
                return cast(Index, UnknownCogniteObject(resource))


@dataclass(frozen=True)
class BTreeIndexApply(IndexApply):
    """Write version of btree index."""

    properties: list[str]
    cursorable: bool = False

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        properties = resource["properties"]
        cursorable = resource.get("cursorable", False)
        return cls(properties=properties, cursorable=cursorable)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped: dict[str, Any] = {"properties": self.properties}
        if self.cursorable is not None:
            dumped["cursorable"] = self.cursorable
        dumped["indexType" if camel_case else "index_type"] = "btree"
        return dumped


@dataclass(frozen=True, kw_only=True)
class BTreeIndex(Index):
    """Read version of btree index with state information."""

    properties: list[str]
    cursorable: bool

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        properties = resource["properties"]
        cursorable = resource.get("cursorable", False)
        state = resource["state"]
        return cls(properties=properties, cursorable=cursorable, state=state)

    def as_apply(self) -> BTreeIndexApply:
        return BTreeIndexApply(properties=self.properties, cursorable=self.cursorable)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped: dict[str, Any] = {"properties": self.properties}
        if self.cursorable is not None:
            dumped["cursorable"] = self.cursorable
        if self.state is not None:
            dumped["state"] = self.state
        dumped["indexType" if camel_case else "index_type"] = "btree"
        return dumped


@dataclass(frozen=True)
class InvertedIndexApply(IndexApply):
    """Write version of inverted index."""

    properties: list[str]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        properties = resource["properties"]
        return cls(properties=properties)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped: dict[str, Any] = {"properties": self.properties}
        dumped["indexType" if camel_case else "index_type"] = "inverted"
        return dumped


@dataclass(frozen=True, kw_only=True)
class InvertedIndex(Index):
    """Read version of inverted index with state information."""

    properties: list[str]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        properties = resource["properties"]
        state = resource["state"]
        return cls(properties=properties, state=state)

    def as_apply(self) -> InvertedIndexApply:
        return InvertedIndexApply(properties=self.properties)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped: dict[str, Any] = {"properties": self.properties}
        dumped["indexType" if camel_case else "index_type"] = "inverted"
        if self.state is not None:
            dumped["state"] = self.state
        return dumped
