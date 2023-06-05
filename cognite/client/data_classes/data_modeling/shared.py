from __future__ import annotations

import typing
from abc import ABC
from dataclasses import asdict, dataclass
from typing import Any, Literal, cast

from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._text import convert_all_keys_to_camel_case_recursive, convert_all_keys_to_snake_case

PrimitiveType = Literal["boolean", "float32", "float64", "int32", "int64", "timestamp", "date", "json"]
CDFType = Literal["timeseries", "file", "sequence"]
TextType = Literal["text"]
DirectType = Literal["direct"]

PRIMITIVE_TYPE_SET = set(typing.get_args(PrimitiveType))
CDF_TYPE_SET = set(typing.get_args(CDFType))
TEXT_TYPE_SET = set(typing.get_args(TextType))
DIRECT_TYPE = set(typing.get_args(DirectType))

_PROPERTY_ALIAS = {"list": "isList"}
_PROPERTY_ALIAS_INV = {"isList": "list", "is_list": "list"}


class DataModeling(CogniteResource):
    def __repr__(self) -> str:
        args = []
        if hasattr(self, "space"):
            space = self.space
            args.append(f"{space=}")
        if hasattr(self, "external_id"):
            external_id = self.external_id
            args.append(f"{external_id=}")

        return f"{type(self).__name__}({', '.join(args)}) at {id(self):#x}"


@dataclass
class DirectRelationReference:
    space: str
    external_id: str

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        output = asdict(self)

        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output

    @classmethod
    def load(cls, data: dict) -> DirectRelationReference:
        return cls(**convert_all_keys_to_snake_case(rename_and_exclude_keys(data, exclude={"type"})))


@dataclass
class Reference(ABC):
    space: str
    external_id: str

    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str]:
        output = asdict(self)

        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output

    @classmethod
    def _load(cls, data: dict | None) -> Any:
        if data is None:
            return None
        return cls(**convert_all_keys_to_snake_case({k: v for k, v in data.items() if k != "type"}))

    @classmethod
    def load(cls, data: dict) -> ContainerReference | ViewReference:
        if "type" not in data:
            raise ValueError("References are required to have a type")

        if data["type"] == "container":
            return ContainerReference.load(data)
        elif data["type"] == "view":
            return ViewReference.load(data)
        raise ValueError(f"Type {data['type']} is not supported.")


@dataclass
class ContainerReference(Reference):
    @classmethod
    def load(cls, data: dict) -> ContainerReference:
        return cast(ContainerReference, super()._load(data))

    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str]:
        output = super().dump(camel_case)
        output["type"] = "container"
        return output


@dataclass
class ViewReference(Reference):
    version: str

    @classmethod
    def load(cls, data: dict) -> ViewReference:
        return cast(ViewReference, super()._load(data))

    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str]:
        output = super().dump(camel_case)
        output["type"] = "view"
        return output


@dataclass
class PropertyType(ABC):
    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str | dict]:
        output = asdict(self)
        return convert_all_keys_to_camel_case_recursive(output) if camel_case else output

    @classmethod
    def _load(cls, data: dict) -> PropertyType:
        return cls(**convert_all_keys_to_snake_case(data))

    @classmethod
    def load(
        cls, data: dict, direct_type_cls: typing.Type[DirectNodeRelation] = None
    ) -> Text | Primitive | CDFExternalIdReference | DirectNodeRelation:
        if "type" not in data:
            raise ValueError("Property types are required to have a type")

        type_ = data["type"]

        if type_ in PRIMITIVE_TYPE_SET:
            return Primitive.load(data)
        elif type_ in TEXT_TYPE_SET:
            return Text.load(data)
        elif type_ in CDF_TYPE_SET:
            return CDFExternalIdReference.load(data)
        elif type_ in DIRECT_TYPE:
            if direct_type_cls is None:
                raise NotImplementedError
            return direct_type_cls.load(rename_and_exclude_keys(data, exclude={"type"}))
        else:
            raise ValueError(
                f"Invalid type {type_}. Must be {PRIMITIVE_TYPE_SET | TEXT_TYPE_SET | CDF_TYPE_SET | DIRECT_TYPE}"
            )


@dataclass
class ListablePropertyType(PropertyType):
    is_list: bool = False


@dataclass
class Text(ListablePropertyType):
    collation: str = "ucs_basic"

    @classmethod
    def load(cls, data: dict, *_: Any, **__: Any) -> Text:
        return cast(Text, cls._load(rename_and_exclude_keys(data, aliases=_PROPERTY_ALIAS, exclude={"type"})))

    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str | dict]:
        output = rename_and_exclude_keys(super().dump(camel_case), aliases=_PROPERTY_ALIAS_INV)
        output["type"] = "text"
        return output


@dataclass
class Primitive(PropertyType):
    type: PrimitiveType
    is_list: bool = False

    @classmethod
    def load(cls, data: dict, *_: Any, **__: Any) -> Primitive:
        return cast(Primitive, cls._load(rename_and_exclude_keys(data, aliases=_PROPERTY_ALIAS)))

    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str | dict]:
        return rename_and_exclude_keys(super().dump(camel_case), aliases=_PROPERTY_ALIAS_INV)


@dataclass
class CDFExternalIdReference(PropertyType):
    type: CDFType
    is_list: bool = False

    @classmethod
    def load(cls, data: dict, *_: Any, **__: Any) -> CDFExternalIdReference:
        return cast(CDFExternalIdReference, cls._load(rename_and_exclude_keys(data, aliases=_PROPERTY_ALIAS)))

    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str | dict]:
        return rename_and_exclude_keys(super().dump(camel_case), aliases=_PROPERTY_ALIAS_INV)


@dataclass
class DirectNodeRelation(PropertyType):
    @classmethod
    def load(cls, data: dict, *_: Any, **__: Any) -> DirectNodeRelation:
        return cast(
            DirectNodeRelation, cls._load(rename_and_exclude_keys(data, aliases=_PROPERTY_ALIAS, exclude={"type"}))
        )

    def dump(self, camel_case: bool = False, *_: Any, **__: Any) -> dict[str, str | dict]:
        output = rename_and_exclude_keys(super().dump(camel_case), aliases=_PROPERTY_ALIAS_INV)
        output["type"] = "direct"
        return output
