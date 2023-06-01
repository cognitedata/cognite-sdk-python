from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResourceList,
)
from cognite.client.data_classes.data_modeling.shared import (
    CDF_TYPE_SET,
    DIRECT_TYPE,
    PRIMITIVE_TYPE_SET,
    TEXT_TYPE_SET,
    CDFExternalIdReference,
    DataModeling,
    DirectNodeRelation,
    PrimitiveProperty,
    TextProperty,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case_recursive, convert_all_keys_to_snake_case
from cognite.client.utils._validation import validate_data_modeling_identifier

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Container(DataModeling):
    """Represent the physical storage of data.

    Args:
        space (str): The workspace for the view.a unique identifier for the space.
        external_id (str): Combined with the space is the unique identifier of the view.
        description (str): Textual description of the view
        name (str): Human readable name for the view.
        is_global (bool): Whether this is a global container, i.e., one of the out-of-the-box models.
        used_for (Literal['node', 'edge', 'all']): Should this operation apply to nodes, edges or both.
        properties (dict[str, ContainerPropertyIdentifier]): We index the property by a local unique identifier.
        constraints (dict[str, ConstraintIdentifier]): Set of constraints to apply to the container
        indexes (dict[str, IndexIdentifier]): Set of indexes to apply to the container.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        space: str = None,
        external_id: str = None,
        description: str = None,
        name: str = None,
        is_global: bool = False,
        used_for: Literal["node", "edge", "all"] = None,
        properties: dict[str, ContainerPropertyIdentifier] = None,
        constraints: dict[str, ConstraintIdentifier] = None,
        indexes: dict[str, IndexIdentifier] = None,
        last_updated_time: int = None,
        created_time: int = None,
        cognite_client: CogniteClient = None,
    ):
        validate_data_modeling_identifier(space, external_id)
        self.space = space
        self.external_id = external_id
        self.description = description
        self.name = name
        self.used_for = used_for
        self.is_global = is_global
        self.properties = properties
        self.constraints = constraints
        self.indexes = indexes
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient = None) -> Container:
        data = json.loads(resource) if isinstance(resource, str) else resource
        if "properties" in data:
            data["properties"] = {k: ContainerPropertyIdentifier.load(v) for k, v in data["properties"].items()} or None
        if "constraints" in data:
            data["constraints"] = {k: load_constraint_identifier(v) for k, v in data["constraints"].items()} or None
        if "indexes" in data:
            data["indexes"] = {k: IndexIdentifier.load(v) for k, v in data["indexes"].items()} or None

        return cast(Container, super()._load(data, cognite_client))

    def dump(self, camel_case: bool = False, exclude_not_supported_by_apply_endpoint: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        for field in ["properties", "constraints", "indexes"]:
            if field not in output:
                continue
            output[field] = {
                k: convert_all_keys_to_camel_case_recursive(asdict(v)) if camel_case else asdict(v)
                for k, v in output[field].items()
            }

        if exclude_not_supported_by_apply_endpoint:
            for exclude in [
                "isGlobal",
                "lastUpdatedTime",
                "createdTime",
                "is_global",
                "last_updated_time",
                "created_time",
            ]:
                output.pop(exclude, None)
        return output


class ContainerList(CogniteResourceList):
    _RESOURCE = Container


class ContainerFilter(CogniteFilter):
    """Represent the filer arguments for the list endpoint.

    Args:
        space (str): The space to query
        include_global (bool): Whether the global containers should be included.
    """

    def __init__(self, space: str = None, include_global: bool = False):
        self.space = space
        self.include_global = include_global


@dataclass
class ContainerDirectNodeRelation(DirectNodeRelation):
    @classmethod
    def load(cls, data: dict) -> ContainerDirectNodeRelation:
        return cast(ContainerDirectNodeRelation, super().load(data))


@dataclass
class ContainerPropertyIdentifier:
    type: TextProperty | PrimitiveProperty | CDFExternalIdReference | ContainerDirectNodeRelation
    nullable: bool = True
    auto_increment: bool = False
    name: Optional[str] = None
    default_value: str | int | dict | None = None
    description: str | None = None

    @classmethod
    def load(cls, data: dict[str, Any]) -> ContainerPropertyIdentifier:
        if "type" not in data:
            raise ValueError("Type not specified")
        type_ = data["type"]["type"]
        if type_ in PRIMITIVE_TYPE_SET:
            data["type"] = PrimitiveProperty(**data["type"])
        elif type_ in TEXT_TYPE_SET:
            data["type"] = TextProperty(**data["type"])
        elif type_ in CDF_TYPE_SET:
            data["type"] = CDFExternalIdReference(**data["type"])
        elif type_ in DIRECT_TYPE:
            data["type"] = ContainerDirectNodeRelation.load(data["type"])
        else:
            raise ValueError(
                f"Invalid {cls.__name__}.type {type_}. Must be {PRIMITIVE_TYPE_SET | TEXT_TYPE_SET | CDF_TYPE_SET | DIRECT_TYPE}"
            )
        return cls(**convert_all_keys_to_snake_case(data))


@dataclass
class RequiresConstraintDefinition:
    space: str
    external_id: str
    constraint_type: Literal["requires"] = "requires"


@dataclass
class UniquenessConstraintDefinition:
    properties: ContainerPropertyIdentifier
    constraint_type: Literal["uniqueness"] = "uniqueness"


@dataclass
class IndexIdentifier:
    properties: ContainerPropertyIdentifier
    index_type: Literal["btree"] | str = "btree"

    @classmethod
    def load(cls, data: dict[str, Any]) -> IndexIdentifier:
        if "properties" not in data:
            raise ValueError(f"{cls.__name__} requires properties.")
        data["properties"] = ContainerPropertyIdentifier.load(data["properties"])
        return cls(**convert_all_keys_to_snake_case(data))


ConstraintIdentifier = Union[IndexIdentifier, UniquenessConstraintDefinition]


def load_constraint_identifier(data: dict[str, Any]) -> ConstraintIdentifier:
    if "properties" not in data:
        raise ValueError("ConstraintIdentifier requires properties.")
    data["properties"] = ContainerPropertyIdentifier.load(data["properties"])
    if "indexType" in data:
        return IndexIdentifier(**convert_all_keys_to_snake_case(data))
    elif "constraintType" in data:
        return UniquenessConstraintDefinition(**convert_all_keys_to_snake_case(data))

    raise ValueError("Invalid ConstraintIdentifier, needs to specify indexType or constraintType")
