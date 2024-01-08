from __future__ import annotations

import inspect
import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, ClassVar, cast

from typing_extensions import Self

from cognite.client.data_classes.data_modeling.ids import ContainerId, NodeId
from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._text import convert_all_keys_recursive, convert_all_keys_to_snake_case

logger = logging.getLogger(__name__)

_PROPERTY_ALIAS = {"list": "isList"}
_PROPERTY_ALIAS_INV = {"isList": "list", "is_list": "list"}


@dataclass(frozen=True)
class DirectRelationReference:
    space: str
    external_id: str

    def dump(self, camel_case: bool = True) -> dict[str, str | dict]:
        return {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
        }

    @classmethod
    def load(cls, data: dict | tuple[str, str]) -> DirectRelationReference:
        if isinstance(data, dict):
            return cls(
                space=data["space"],
                external_id=data["externalId"],
            )
        elif isinstance(data, tuple) and len(data) == 2:
            return cls(data[0], data[1])
        else:
            raise ValueError("Invalid data provided to load method. Must be dict or tuple with two elements.")

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


@dataclass
class PropertyType(ABC):
    _type: ClassVar[str]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = asdict(self)
        output["type"] = self._type
        for key in list(output.keys()):
            if output[key] is None:
                output.pop(key)
        output = rename_and_exclude_keys(output, aliases=_PROPERTY_ALIAS_INV)
        return convert_all_keys_recursive(output, camel_case)

    @classmethod
    def load(cls, data: dict) -> Self:
        if "type" not in data:
            raise ValueError("Property types are required to have a type")
        type_ = data["type"]
        data = convert_all_keys_to_snake_case(rename_and_exclude_keys(data, aliases=_PROPERTY_ALIAS, exclude={"type"}))

        if type_cls := _TYPE_LOOKUP.get(type_):
            if issubclass(type_cls, LoadablePropertyType):
                return cast(Self, type_cls.load(data))
            try:
                return cast(Self, type_cls(**data))
            except TypeError:
                not_supported = set(data).difference(inspect.signature(type_cls).parameters) - {"type"}
                logger.warning(
                    f"For '{type_cls.__name__}', the following properties are not yet supported in the SDK (ignored): "
                    f"{not_supported}. Try updating to the latest SDK version, or create an issue on Github!"
                )
                return cast(Self, type_cls(**rename_and_exclude_keys(data, exclude=not_supported)))

        raise ValueError(f"Invalid type {type_}.")


@dataclass
class LoadablePropertyType(ABC):
    @classmethod
    @abstractmethod
    def load(cls, data: dict) -> Self:
        ...


@dataclass
class ListablePropertyType(PropertyType, ABC):
    is_list: bool = False


@dataclass
class Text(ListablePropertyType):
    _type = "text"
    collation: str = "ucs_basic"


@dataclass
class Primitive(ListablePropertyType, ABC):
    ...


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
class ListablePropertyTypeWithUnit(ListablePropertyType, LoadablePropertyType, ABC):
    unit: NodeId | None = None

    @classmethod
    def load(cls, data: dict) -> Self:
        data = convert_all_keys_to_snake_case(rename_and_exclude_keys(data, aliases=_PROPERTY_ALIAS, exclude={"type"}))
        unit = None
        if (unit_raw := data.get("unit")) and isinstance(unit_raw, dict):
            unit = NodeId.load(unit_raw)
        elif unit_raw:
            unit = unit_raw
        return cls(
            is_list=data["is_list"],
            unit=unit,
        )


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
class CDFExternalIdReference(ListablePropertyType, ABC):
    ...


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
class DirectRelation(PropertyType, LoadablePropertyType):
    _type = "direct"
    container: ContainerId | None = None

    def dump(self, camel_case: bool = True) -> dict:
        output = super().dump(camel_case)
        if "container" in output:
            if isinstance(output["container"], dict):
                output["container"]["type"] = "container"
            elif output["container"] is None:
                output.pop("container")
        return output

    @classmethod
    def load(cls, data: dict) -> Self:
        return cls(container=ContainerId.load(container) if (container := data.get("container")) else None)


_TYPE_LOOKUP: dict[str, type[PropertyType]] = {
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
