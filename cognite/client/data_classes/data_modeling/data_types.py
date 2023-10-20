from __future__ import annotations

import inspect
import logging
from abc import ABC
from dataclasses import asdict, dataclass
from typing import Any, ClassVar

from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._text import convert_all_keys_recursive, convert_all_keys_to_snake_case

logger = logging.getLogger(__name__)

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

        if type_cls := _TYPE_LOOKUP.get(type_):
            try:
                return type_cls(**data)
            except TypeError:
                not_supported = set(data).difference(inspect.signature(type_cls).parameters) - {"type"}
                logger.warning(
                    f"For '{type_cls.__name__}', the following properties are not yet supported in the SDK (ignored): "
                    f"{not_supported}. Try updating to the latest SDK version!"
                )
                return type_cls(**{k: v for k, v in data.items() if k not in not_supported})

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
class Timestamp(ListablePropertyType):
    _type = "timestamp"


@dataclass
class Date(ListablePropertyType):
    _type = "date"


@dataclass
class Json(ListablePropertyType):
    _type = "json"


@dataclass
class ListablePropertyTypeWithUnit(ListablePropertyType):
    _type = "listable_with_unit"
    unit: str | None = None


@dataclass
class Float32(ListablePropertyTypeWithUnit):
    _type = "float32"


@dataclass
class Float64(ListablePropertyTypeWithUnit):
    _type = "float64"


@dataclass
class Int32(ListablePropertyTypeWithUnit):
    _type = "int32"


@dataclass
class Int64(ListablePropertyTypeWithUnit):
    _type = "int64"


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


_TYPE_LOOKUP = {
    "text": Text,
    "boolean": Boolean,
    "float32": Float32,
    "float64": Float64,
    "int32": Int32,
    "int64": Int64,
    "timestamp": Timestamp,
    "date": Date,
    "json": Json,
    "timeseries": TimeSeriesReference,
    "file": FileReference,
    "sequence": SequenceReference,
    "direct": DirectRelation,
}
