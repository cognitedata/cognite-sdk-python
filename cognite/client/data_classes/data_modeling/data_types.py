from __future__ import annotations

from abc import ABC
from dataclasses import asdict, dataclass
from typing import Any, ClassVar

from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._text import convert_all_keys_recursive, convert_all_keys_to_snake_case

_PROPERTY_ALIAS = {"list": "isList"}
_PROPERTY_ALIAS_INV = {"isList": "list", "is_list": "list"}


@dataclass
class DirectRelationReference:
    space: str
    external_id: str

    def dump(self, camel_case: bool = False) -> dict[str, str | dict]:
        output = asdict(self)

        return convert_all_keys_recursive(output, camel_case)

    @classmethod
    def load(cls, data: dict | tuple[str, str]) -> DirectRelationReference:
        if isinstance(data, dict):
            return cls(**convert_all_keys_to_snake_case(rename_and_exclude_keys(data, exclude={"type"})))
        elif isinstance(data, tuple) and len(data) == 2:
            return cls(data[0], data[1])
        else:
            raise ValueError("Invalid data provided to load method. Must be dict or tuple with two elements.")

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


@dataclass
class PropertyType(ABC):
    _type: ClassVar[str]

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = asdict(self)
        output["type"] = self._type
        output = rename_and_exclude_keys(output, aliases=_PROPERTY_ALIAS_INV)
        return convert_all_keys_recursive(output, camel_case)

    @classmethod
    def load(cls, data: dict) -> PropertyType:
        if "type" not in data:
            raise ValueError("Property types are required to have a type")
        type_ = data["type"]
        data = convert_all_keys_to_snake_case(rename_and_exclude_keys(data, aliases=_PROPERTY_ALIAS, exclude={"type"}))

        if type_ == "text":
            return Text(**data)
        elif type_ == "boolean":
            return Boolean(**data)
        elif type_ == "float32":
            return Float32(**data)
        elif type_ == "float64":
            return Float64(**data)
        elif type_ == "int32":
            return Int32(**data)
        elif type_ == "int64":
            return Int64(**data)
        elif type_ == "timestamp":
            return Timestamp(**data)
        elif type_ == "date":
            return Date(**data)
        elif type_ == "json":
            return Json(**data)
        elif type_ == "timeseries":
            return TimeSeriesReference(**data)
        elif type_ == "file":
            return FileReference(**data)
        elif type_ == "sequence":
            return SequenceReference(**data)
        elif type_ == "direct":
            return DirectRelation(**data)

        raise ValueError(f"Invalid type {type_}.")


@dataclass
class ListablePropertyType(PropertyType):
    _type = "listable"
    is_list: bool = False


@dataclass
class Text(ListablePropertyType):
    _type = "text"
    collation: str = "ucs_basic"


@dataclass
class Primitive(ListablePropertyType):
    _type = "primitive"


@dataclass
class Boolean(ListablePropertyType):
    _type = "boolean"


@dataclass
class Float32(ListablePropertyType):
    _type = "float32"


@dataclass
class Float64(ListablePropertyType):
    _type = "float64"


@dataclass
class Int32(ListablePropertyType):
    _type = "int32"


@dataclass
class Int64(ListablePropertyType):
    _type = "int64"


@dataclass
class Timestamp(ListablePropertyType):
    _type = "timestamp"


@dataclass
class Date(ListablePropertyType):
    _type = "date"


@dataclass
class Json(ListablePropertyType):
    _type = "json"


@dataclass
class CDFExternalIdReference(ListablePropertyType):
    _type = "cdf_external_reference"


@dataclass
class TimeSeriesReference(CDFExternalIdReference):
    _type = "timeseries"


@dataclass
class FileReference(CDFExternalIdReference):
    _type = "file"


@dataclass
class SequenceReference(CDFExternalIdReference):
    _type = "sequence"


@dataclass
class DirectRelation(PropertyType):
    _type = "direct"
    container: ContainerId | None = None

    def dump(self, camel_case: bool = False) -> dict:
        output = super().dump(camel_case)
        if "container" in output and isinstance(output["container"], dict):
            output["container"]["type"] = "container"
        return output

    @classmethod
    def load(cls, data: dict) -> DirectRelation:
        output = cls(**convert_all_keys_to_snake_case(rename_and_exclude_keys(data, exclude={"type"})))
        if isinstance(data.get("container"), dict):
            output.container = ContainerId.load(data["container"])
        return output
