from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Literal

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling._core import DataModelingResource
from cognite.client.data_classes.data_modeling._validation import validate_data_modeling_identifier
from cognite.client.data_classes.data_modeling.data_types import (
    DirectRelation,
    PropertyType,
)
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.utils._text import convert_all_keys_to_camel_case_recursive


class ContainerCore(DataModelingResource):
    """Represent the physical storage of data. This is the base class for the read and write version.

    Args:
        space (str): The workspace for the view, a unique identifier for the space.
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
    def load(cls, resource: dict | str) -> ContainerCore:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "constraints" in data:
            data["constraints"] = {k: Constraint.load(v) for k, v in data["constraints"].items()} or None
        if "indexes" in data:
            data["indexes"] = {k: Index.load(v) for k, v in data["indexes"].items()} or None
        if "properties" in data:
            data["properties"] = {k: ContainerProperty.load(v) for k, v in data["properties"].items()} or None
        return super().load(data)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
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
        space (str): The workspace for the view, a unique identifier for the space.
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


class Container(ContainerCore):
    """Represent the physical storage of data. This is the read format of the container

    Args:
        space (str): The workspace for the view, a unique identifier for the space.
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
class ContainerProperty:
    type: PropertyType
    nullable: bool = True
    auto_increment: bool = False
    name: str | None = None
    default_value: str | int | dict | None = None
    description: str | None = None

    @classmethod
    def load(cls, data: dict[str, Any]) -> ContainerProperty:
        if "type" not in data:
            raise ValueError("Type not specified")
        if data["type"].get("type") == "direct":
            type_: PropertyType = DirectRelation.load(data["type"])
        else:
            type_ = PropertyType.load(data["type"])
        return cls(
            type=type_,
            nullable=data["nullable"],
            auto_increment=data["autoIncrement"],
            name=data.get("name"),
            default_value=data.get("defaultValue"),
            description=data.get("description"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        output = asdict(self)
        if "type" in output and isinstance(output["type"], dict):
            output["type"] = self.type.dump(camel_case)
        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output


@dataclass(frozen=True)
class Constraint(ABC):
    @classmethod
    def load(cls, data: dict) -> RequiresConstraint | UniquenessConstraintDefinition:
        if data["constraintType"] == "requires":
            return RequiresConstraint.load(data)
        elif data["constraintType"] == "uniqueness":
            return UniquenessConstraintDefinition.load(data)
        raise ValueError(f"Invalid constraint type {data['constraintType']}")

    @abstractmethod
    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        raise NotImplementedError


@dataclass(frozen=True)
class RequiresConstraint(Constraint):
    require: ContainerId

    @classmethod
    def load(cls, data: dict) -> RequiresConstraint:
        return cls(require=ContainerId.load(data["require"]))

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
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
    def load(cls, data: dict) -> UniquenessConstraint:
        return cls(properties=data["properties"])

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        as_dict = asdict(self)
        output = convert_all_keys_to_camel_case_recursive(as_dict) if camel_case else as_dict
        key = "constraintType" if camel_case else "constraint_type"
        output[key] = "uniqueness"
        return output


# Type aliases for backwards compatibility after renaming
# TODO: Remove in some future major version
RequiresConstraintDefinition = RequiresConstraint
UniquenessConstraintDefinition = UniquenessConstraint


@dataclass(frozen=True)
class Index(ABC):
    @classmethod
    def load(cls, data: dict) -> Index:
        if data["indexType"] == "btree":
            return BTreeIndex.load(data)
        if data["indexType"] == "inverted":
            return InvertedIndex.load(data)
        raise ValueError(f"Invalid index type {data['indexType']}")

    @abstractmethod
    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        raise NotImplementedError


@dataclass(frozen=True)
class BTreeIndex(Index):
    properties: list[str]
    cursorable: bool = False

    @classmethod
    def load(cls, data: dict[str, Any]) -> BTreeIndex:
        return cls(properties=data["properties"], cursorable=data["cursorable"])

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        as_dict = asdict(self)
        as_dict["indexType" if camel_case else "index_type"] = "btree"
        return convert_all_keys_to_camel_case_recursive(as_dict) if camel_case else as_dict


@dataclass(frozen=True)
class InvertedIndex(Index):
    properties: list[str]

    @classmethod
    def load(cls, data: dict[str, Any]) -> InvertedIndex:
        return cls(properties=data["properties"])

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        as_dict = asdict(self)
        as_dict["indexType" if camel_case else "index_type"] = "inverted"
        return convert_all_keys_to_camel_case_recursive(as_dict) if camel_case else as_dict
