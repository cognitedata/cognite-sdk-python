from __future__ import annotations

import typing
from dataclasses import dataclass
from typing import Any, Literal, Optional, Union

from cognite.client.data_classes.data_modeling.ids import DataModelingId
from cognite.client.utils._text import convert_all_keys_to_snake_case

PrimitiveType = Literal["boolean", "float32", "float64", "int32", "int64", "timestamp", "date", "json"]
CDFType = Literal["timeseries", "file", "sequence"]
TextType = Literal["text"]
DirectType = Literal["direct"]

PRIMITIVE_TYPE_SET = set(typing.get_args(PrimitiveType))
CDF_TYPE_SET = set(typing.get_args(CDFType))
TEXT_TYPE_SET = set(typing.get_args(TextType))
DIRECT_TYPE = set(typing.get_args(DirectType))


@dataclass
class Container(DataModelingId):
    type: str = "container"


@dataclass
class TextProperty:
    type: TextType = "text"
    list: bool = False
    collation: str = "ucs_basic"


@dataclass
class PrimitiveProperty:
    type: PrimitiveType
    list: bool = False


@dataclass
class CDFExternalIdReference:
    type: CDFType | str
    list: bool = False


@dataclass
class DirectNodeRelation:
    type: DirectType = "direct"
    container: Optional[Container] = None


@dataclass
class ContainerPropertyIdentifier:
    type: TextProperty | PrimitiveProperty | CDFExternalIdReference | DirectNodeRelation
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
            data["type"] = DirectNodeRelation(**data["type"])
        else:
            raise ValueError(
                f"Invalid {cls.__name__}.type {type_}. Must be {PRIMITIVE_TYPE_SET | TEXT_TYPE_SET | CDF_TYPE_SET | DIRECT_TYPE}"
            )
        return cls(**convert_all_keys_to_snake_case(data))


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
