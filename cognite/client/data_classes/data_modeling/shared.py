from __future__ import annotations

import typing
from dataclasses import asdict, dataclass
from typing import Any, Literal, Optional

from cognite.client.utils._text import convert_all_keys_to_camel_case_nested, convert_all_keys_to_snake_case

PrimitiveType = Literal["boolean", "float32", "float64", "int32", "int64", "timestamp", "date", "json"]
CDFType = Literal["timeseries", "file", "sequence"]
TextType = Literal["text"]
DirectType = Literal["direct"]

PRIMITIVE_TYPE_SET = set(typing.get_args(PrimitiveType))
CDF_TYPE_SET = set(typing.get_args(CDFType))
TEXT_TYPE_SET = set(typing.get_args(TextType))
DIRECT_TYPE = set(typing.get_args(DirectType))


@dataclass
class DirectRelationReference:
    space: str
    external_id: str

    @classmethod
    def load(cls, data: dict) -> DirectRelationReference:
        return cls(**convert_all_keys_to_snake_case(data))


@dataclass
class Reference:
    space: str
    external_id: str

    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str]:
        output = asdict(self)

        return convert_all_keys_to_camel_case_nested(output) if camel_case else output


@dataclass
class ContainerReference(Reference):
    type: Literal["container"] = "container"

    @classmethod
    def load(cls, data: dict) -> ContainerReference:
        return cls(**convert_all_keys_to_snake_case(data))


@dataclass
class ViewReference(Reference):
    version: str
    type: Literal["view"] = "view"

    @classmethod
    def load(cls, data: dict) -> ViewReference:
        return cls(**convert_all_keys_to_snake_case(data))

    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str]:
        dumped = asdict(self)
        return convert_all_keys_to_camel_case_nested(dumped) if camel_case else dumped


def load_reference(data: dict) -> ContainerReference | ViewReference:
    if "type" not in data:
        raise ValueError("References are required to have a type")
    if data["type"] == "container":
        return ContainerReference.load(data)
    elif data["type"] == "view":
        return ViewReference.load(data)
    raise ValueError(f"Type {data['type']} is not supported.zs")


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
    container: Optional[ContainerReference] = None

    @classmethod
    def load(cls, data: dict) -> DirectNodeRelation:
        if isinstance(data.get("container"), dict):
            data["container"] = ContainerReference(**convert_all_keys_to_snake_case(data["container"]))
        return cls(**data)
