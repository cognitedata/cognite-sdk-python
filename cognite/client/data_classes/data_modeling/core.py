from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Type, Union

from cognite.client.data_classes._base import CogniteResource, T_CogniteResource
from cognite.client.utils._text import to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient

PrimitiveType = Literal["boolean", "float32", "float64", "int32", "int64", "timestamp", "date", "json"]
CDFType = Literal["timeseries", "file", "sequence"]
TextType = Literal["text"]
DirectType = Literal["direct"]


@dataclass
class Container:
    space: str
    external_id: str
    type: str = "container"


@dataclass
class TextProperty:
    type: TextType = "text"
    list: bool = False
    collation: str = "ucs_basic"


@dataclass
class PrimitiveProperty:
    type: PrimitiveType
    list: bool = True


@dataclass
class CDFExternalIdReference:
    type: CDFType | str
    list: bool = False


@dataclass
class DirectNodeRelation:
    container: Container
    type: DirectType = "direct"


@dataclass
class ContainerPropertyIdentifier:
    nullable: bool
    auto_increment: bool
    name: str
    type: TextProperty | PrimitiveProperty | CDFExternalIdReference | DirectNodeRelation
    default_value: str | int | dict | None = None
    description: str | None = None


# class ConstraintIdentifier:


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


ConstraintIdentifier = Union[UniquenessConstraintDefinition, IndexIdentifier]


class DataModelingResource(CogniteResource):
    @classmethod
    def _load(
        cls: Type[T_CogniteResource], resource: dict | str, cognite_client: CogniteClient = None
    ) -> T_CogniteResource:
        data = json.loads(resource) if isinstance(resource, str) else resource

        return cls(**{to_snake_case(k): v for k, v in data.items()}, cognite_client=cognite_client)
