from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteObject,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.core import DataModelingResource
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelation,
    PropertyType,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.utils._text import convert_all_keys_to_camel_case_recursive

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class ContainerCore(DataModelingResource):
    """Represent the physical storage of data. This is the base class for the read and write version.

    Args:
        space (str): The workspace for the container, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        properties (dict[str, ContainerProperty]): We index the property by a local unique identifier.
        description (str | None): Textual description of the view
        name (str | None): Human readable name for the view.
        used_for (Literal["node", "edge", "all"] | None): Should this operation apply to nodes, edges or both.
        constraints (dict[str, Constraint] | None): Set of constraints to apply to the container
        indexes (dict[str, Index] | None): Set of indexes to apply to the container.
        **_ (Any): No description.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        properties: dict[str, ContainerProperty],
        description: str | None = None,
        name: str | None = None,
        used_for: Literal["node", "edge", "all"] | None = None,
        constraints: dict[str, Constraint] | None = None,
        indexes: dict[str, Index] | None = None,
        **_: Any,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.description = description
        self.name = name
        self.used_for = used_for
        self.properties = properties
        self.constraints = constraints
        self.indexes = indexes

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        if "constraints" in resource:
            resource["constraints"] = {k: Constraint.load(v) for k, v in resource["constraints"].items()} or None
        if "indexes" in resource:
            resource["indexes"] = {k: Index.load(v) for k, v in resource["indexes"].items()} or None
        if "properties" in resource:
            resource["properties"] = {k: ContainerProperty.load(v) for k, v in resource["properties"].items()} or None
        return super()._load(resource, cognite_client)

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


class ContainerApply(ContainerCore):
    """Represent the physical storage of data. This is the write format of the container

    Args:
        space (str): The workspace for the container, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        properties (dict[str, ContainerProperty]): We index the property by a local unique identifier.
        description (str | None): Textual description of the view
        name (str | None): Human readable name for the view.
        used_for (Literal["node", "edge", "all"] | None): Should this operation apply to nodes, edges or both.
        constraints (dict[str, Constraint] | None): Set of constraints to apply to the container
        indexes (dict[str, Index] | None): Set of indexes to apply to the container.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        properties: dict[str, ContainerProperty],
        description: str | None = None,
        name: str | None = None,
        used_for: Literal["node", "edge", "all"] | None = None,
        constraints: dict[str, Constraint] | None = None,
        indexes: dict[str, Index] | None = None,
    ) -> None:
        validate_data_modeling_identifier(space, external_id)
        super().__init__(space, external_id, properties, description, name, used_for, constraints, indexes)

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> ContainerApply:
        return ContainerApply(
            space=resource["space"],
            external_id=resource["externalId"],
            properties={k: ContainerProperty.load(v) for k, v in resource["properties"].items()},
            description=resource.get("description"),
            name=resource.get("name"),
            used_for=resource.get("usedFor"),
            constraints={k: Constraint.load(v) for k, v in resource["constraints"].items()}
            if "constraints" in resource
            else None,
            indexes={k: Index.load(v) for k, v in resource["indexes"].items()} or None
            if "indexes" in resource
            else None,
        )


class Container(ContainerCore):
    """Represent the physical storage of data. This is the read format of the container

    Args:
        space (str): The workspace for the container, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        properties (dict[str, ContainerProperty]): We index the property by a local unique identifier.
        is_global (bool): Whether this is a global container, i.e., one of the out-of-the-box models.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description (str | None): Textual description of the view
        name (str | None): Human readable name for the view.
        used_for (Literal["node", "edge", "all"]): Should this operation apply to nodes, edges or both.
        constraints (dict[str, Constraint] | None): Set of constraints to apply to the container
        indexes (dict[str, Index] | None): Set of indexes to apply to the container.
        **_ (Any): No description.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        properties: dict[str, ContainerProperty],
        is_global: bool,
        last_updated_time: int,
        created_time: int,
        description: str | None = None,
        name: str | None = None,
        used_for: Literal["node", "edge", "all"] = "node",
        constraints: dict[str, Constraint] | None = None,
        indexes: dict[str, Index] | None = None,
        **_: Any,
    ) -> None:
        super().__init__(space, external_id, properties, description, name, used_for, constraints, indexes)
        self.is_global = is_global
        self.last_updated_time = last_updated_time
        self.created_time = created_time

    def as_apply(self) -> ContainerApply:
        return ContainerApply(
            space=self.space,
            external_id=self.external_id,
            properties=self.properties,
            description=self.description,
            name=self.name,
            used_for=self.used_for,
            constraints=self.constraints,
            indexes=self.indexes,
        )


class ContainerList(CogniteResourceList[Container]):
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


class ContainerApplyList(CogniteResourceList[ContainerApply]):
    _RESOURCE = ContainerApply

    def as_ids(self) -> list[ContainerId]:
        """Convert to a container id list.

        Returns:
            list[ContainerId]: The container id list.
        """
        return [v.as_id() for v in self]


class ContainerFilter(CogniteFilter):
    """Represent the filter arguments for the list endpoint.

    Args:
        space (str | None): The space to query
        include_global (bool): Whether the global containers should be included.
    """

    def __init__(self, space: str | None = None, include_global: bool = False) -> None:
        self.space = space
        self.include_global = include_global


@dataclass(frozen=True)
class ContainerProperty(CogniteObject):
    type: PropertyType
    nullable: bool = True
    auto_increment: bool = False
    name: str | None = None
    default_value: str | int | dict | None = None
    description: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if "type" not in resource:
            raise ValueError("Type not specified")
        if resource["type"].get("type") == "direct":
            type_: PropertyType = DirectRelation.load(resource["type"])
        else:
            type_ = PropertyType.load(resource["type"])
        return cls(
            type=type_,
            # If nothing is specified, we will pass through null values
            nullable=resource.get("nullable"),  # type: ignore[arg-type]
            auto_increment=resource.get("autoIncrement"),  # type: ignore[arg-type]
            name=resource.get("name"),
            default_value=resource.get("defaultValue"),
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, str | dict]:
        output: dict[str, str | dict] = {}
        if self.type:
            output["type"] = self.type.dump(camel_case)
        for key in ["nullable", "auto_increment", "name", "default_value", "description"]:
            if (value := getattr(self, key)) is not None:
                output[key] = value
        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output


@dataclass(frozen=True)
class Constraint(CogniteObject, ABC):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if resource["constraintType"] == "requires":
            return cast(Self, RequiresConstraint.load(resource))
        elif resource["constraintType"] == "uniqueness":
            return cast(Self, UniquenessConstraint.load(resource))
        raise ValueError(f"Invalid constraint type {resource['constraintType']}")

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, str | dict]:
        raise NotImplementedError


@dataclass(frozen=True)
class RequiresConstraint(Constraint):
    require: ContainerId

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(require=ContainerId.load(resource["require"]))

    def dump(self, camel_case: bool = True) -> dict[str, str | dict]:
        as_dict = asdict(self)
        output = convert_all_keys_to_camel_case_recursive(as_dict) if camel_case else as_dict
        if "require" in output and isinstance(output["require"], dict):
            output["require"] = self.require.dump(camel_case)
        key = "constraintType" if camel_case else "constraint_type"
        output[key] = "requires"
        return output


@dataclass(frozen=True)
class UniquenessConstraint(Constraint):
    properties: list[str]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(properties=resource["properties"])

    def dump(self, camel_case: bool = True) -> dict[str, str | dict]:
        as_dict = asdict(self)
        output = convert_all_keys_to_camel_case_recursive(as_dict) if camel_case else as_dict
        key = "constraintType" if camel_case else "constraint_type"
        output[key] = "uniqueness"
        return output


@dataclass(frozen=True)
class Index(CogniteObject, ABC):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if resource["indexType"] == "btree":
            return cast(Self, BTreeIndex.load(resource))
        if resource["indexType"] == "inverted":
            return cast(Self, InvertedIndex.load(resource))
        raise ValueError(f"Invalid index type {resource['indexType']}")

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, str | dict]:
        raise NotImplementedError


@dataclass(frozen=True)
class BTreeIndex(Index):
    properties: list[str]
    cursorable: bool = False

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(properties=resource["properties"], cursorable=resource.get("cursorable"))  # type: ignore[arg-type]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped: dict[str, Any] = {"properties": self.properties}
        if self.cursorable is not None:
            dumped["cursorable"] = self.cursorable
        dumped["indexType" if camel_case else "index_type"] = "btree"
        return convert_all_keys_to_camel_case_recursive(dumped) if camel_case else dumped


@dataclass(frozen=True)
class InvertedIndex(Index):
    properties: list[str]

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(properties=resource["properties"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped: dict[str, Any] = {"properties": self.properties}
        dumped["indexType" if camel_case else "index_type"] = "inverted"
        return convert_all_keys_to_camel_case_recursive(dumped) if camel_case else dumped
