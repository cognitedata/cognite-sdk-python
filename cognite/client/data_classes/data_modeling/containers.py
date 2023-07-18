from __future__ import annotations

import json
from abc import ABC
from dataclasses import asdict, dataclass
from typing import Any, Literal, Optional, cast

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
from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._text import convert_all_keys_to_camel_case_recursive, convert_all_keys_to_snake_case


class ContainerCore(DataModelingResource):
    """Represent the physical storage of data. This is the base class for the read and write version.

    Args:
        space (str): The workspace for the view, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        description (str): Textual description of the view
        name (str): Human readable name for the view.
        used_for (Literal['node', 'edge', 'all']): Should this operation apply to nodes, edges or both.
        properties (dict[str, ContainerProperty]): We index the property by a local unique identifier.
        constraints (dict[str, Constraint]): Set of constraints to apply to the container
        indexes (dict[str, Index]): Set of indexes to apply to the container.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        properties: dict[str, ContainerProperty],
        description: Optional[str] = None,
        name: Optional[str] = None,
        used_for: Optional[Literal["node", "edge", "all"]] = None,
        constraints: Optional[dict[str, Constraint]] = None,
        indexes: Optional[dict[str, Index]] = None,
        **_: dict,
    ):
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
        return cast(ContainerCore, super().load(data))

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
        description (str): Textual description of the view
        name (str): Human readable name for the view.
        used_for (Literal['node', 'edge', 'all']): Should this operation apply to nodes, edges or both.
        properties (dict[str, ContainerProperty]): We index the property by a local unique identifier.
        constraints (dict[str, Constraint]): Set of constraints to apply to the container
        indexes (dict[str, Index]): Set of indexes to apply to the container.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        properties: dict[str, ContainerProperty],
        description: Optional[str] = None,
        name: Optional[str] = None,
        used_for: Optional[Literal["node", "edge", "all"]] = None,
        constraints: Optional[dict[str, Constraint]] = None,
        indexes: Optional[dict[str, Index]] = None,
    ):
        validate_data_modeling_identifier(space, external_id)
        super().__init__(space, external_id, properties, description, name, used_for, constraints, indexes)


class Container(ContainerCore):
    """Represent the physical storage of data. This is the read format of the container

    Args:
        space (str): The workspace for the view, a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        description (str): Textual description of the view
        name (str): Human readable name for the view.
        is_global (bool): Whether this is a global container, i.e., one of the out-of-the-box models.
        used_for (Literal['node', 'edge', 'all']): Should this operation apply to nodes, edges or both.
        properties (dict[str, ContainerProperty]): We index the property by a local unique identifier.
        constraints (dict[str, Constraint]): Set of constraints to apply to the container
        indexes (dict[str, Index]): Set of indexes to apply to the container.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        properties: dict[str, ContainerProperty],
        is_global: bool,
        last_updated_time: int,
        created_time: int,
        description: Optional[str] = None,
        name: Optional[str] = None,
        used_for: Literal["node", "edge", "all"] = "node",
        constraints: Optional[dict[str, Constraint]] = None,
        indexes: Optional[dict[str, Index]] = None,
    ):
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


class ContainerApplyList(CogniteResourceList[ContainerApply]):
    _RESOURCE = ContainerApply

    def as_apply(self) -> ContainerApplyList:
        """Convert to a container an apply list.

        Returns:
            ContainerApplyList: The container apply list.
        """
        return ContainerApplyList(resources=[v.as_apply() for v in self.items])


class ContainerFilter(CogniteFilter):
    """Represent the filter arguments for the list endpoint.

    Args:
        space (str): The space to query
        include_global (bool): Whether the global containers should be included.
    """

    def __init__(self, space: Optional[str] = None, include_global: bool = False):
        self.space = space
        self.include_global = include_global


@dataclass
class ContainerProperty:
    type: PropertyType
    nullable: bool = True
    auto_increment: bool = False
    name: Optional[str] = None
    default_value: str | int | dict | None = None
    description: str | None = None

    @classmethod
    def load(cls, data: dict[str, Any]) -> ContainerProperty:
        if "type" not in data:
            raise ValueError("Type not specified")
        if data["type"].get("type") == "direct":
            data["type"] = DirectRelation.load(data["type"])
        else:
            data["type"] = PropertyType.load(data["type"])
        return cls(**convert_all_keys_to_snake_case(data))

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        output = asdict(self)
        if "type" in output and isinstance(output["type"], dict):
            output["type"] = self.type.dump(camel_case)
        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output


@dataclass
class Constraint(ABC):
    @classmethod
    def _load(cls, data: dict) -> Constraint:
        return cls(**convert_all_keys_to_snake_case(data))

    @classmethod
    def load(cls, data: dict) -> RequiresConstraintDefinition | UniquenessConstraintDefinition:
        if data["constraintType"] == "requires":
            return RequiresConstraintDefinition.load(data)
        elif data["constraintType"] == "uniqueness":
            return UniquenessConstraintDefinition.load(data)
        raise ValueError(f"Invalid constraint type {data['constraintType']}")

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        output = asdict(self)
        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output


@dataclass
class RequiresConstraintDefinition(Constraint):
    require: ContainerId

    @classmethod
    def load(cls, data: dict) -> RequiresConstraintDefinition:
        output = cast(
            RequiresConstraintDefinition, super()._load(rename_and_exclude_keys(data, exclude={"constraintType"}))
        )
        if "require" in data:
            output.require = ContainerId.load(data["require"])
        return output

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        output = super().dump(camel_case)
        if "require" in output and isinstance(output["require"], dict):
            output["require"] = self.require.dump(camel_case)
        key = "constraintType" if camel_case else "constraint_type"
        output[key] = "requires"
        return output


@dataclass
class UniquenessConstraintDefinition(Constraint):
    properties: list[str]

    @classmethod
    def load(cls, data: dict) -> UniquenessConstraintDefinition:
        return cast(
            UniquenessConstraintDefinition, super()._load(rename_and_exclude_keys(data, exclude={"constraintType"}))
        )

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        output = super().dump()
        key = "constraintType" if camel_case else "constraint_type"
        output[key] = "uniqueness"
        return output


@dataclass
class Index:
    properties: list[str]
    index_type: Literal["btree"] | str = "btree"

    @classmethod
    def load(cls, data: dict[str, Any]) -> Index:
        return cls(**convert_all_keys_to_snake_case(data))

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        output = asdict(self)
        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output
