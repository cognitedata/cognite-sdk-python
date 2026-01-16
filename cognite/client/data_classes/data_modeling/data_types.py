from __future__ import annotations

import logging
from abc import ABC
from dataclasses import asdict, dataclass
from typing import Any, ClassVar, TypeAlias, cast

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, UnknownCogniteResource
from cognite.client.data_classes.data_modeling.ids import ContainerId, NodeId
from cognite.client.utils._auxiliary import is_positive, rename_and_exclude_keys
from cognite.client.utils._text import convert_all_keys_recursive

logger = logging.getLogger(__name__)

_PROPERTY_ALIAS = {"isList": "list", "is_list": "list"}


@dataclass(frozen=True, slots=True)
class DirectRelationReference:
    space: str
    external_id: str

    def dump(self, camel_case: bool = True) -> dict[str, str | dict]:
        return {
            "space": self.space,
            "externalId" if camel_case else "external_id": self.external_id,
        }

    @classmethod
    def load(cls, data: dict[str, str] | tuple[str, str] | DirectRelationReference | NodeId) -> DirectRelationReference:
        match data:
            case {"space": space, "externalId": xid}:
                return cls(space, xid)
            case (space, xid):
                return cls(space, xid)
            case cls() | NodeId():  # type: ignore [misc]
                return cls(data.space, data.external_id)
            case _:
                raise ValueError("Invalid data provided to load method. Must be dict or tuple with two elements.")

    @classmethod
    def _load_if(
        cls, data: dict[str, str] | tuple[str, str] | DirectRelationReference | NodeId | None
    ) -> DirectRelationReference | None:
        if data is None:
            return None
        return cls.load(data)

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


@dataclass
class PropertyType(CogniteResource, ABC):
    _type: ClassVar[str]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = asdict(self)
        output["type"] = self._type
        for key in list(output.keys()):
            if output[key] is None:
                output.pop(key)
        output = rename_and_exclude_keys(output, aliases=_PROPERTY_ALIAS)
        return convert_all_keys_recursive(output, camel_case)

    @staticmethod
    def __load_unit_ref(data: dict) -> UnitReference | None:
        unit: UnitReference | None = None
        if (unit_raw := data.get("unit")) and isinstance(unit_raw, dict):
            unit = UnitReference.load(unit_raw)
        return unit

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> PropertyType:
        match type_ := resource["type"]:
            case "text":
                return Text(
                    is_list=resource["list"],
                    max_list_size=resource.get("maxListSize"),
                    collation=resource.get("collation", "ucs_basic"),
                    max_text_size=resource.get("maxTextSize"),
                )
            case "boolean":
                return Boolean(
                    is_list=resource["list"],
                    max_list_size=resource.get("maxListSize"),
                )
            case "float32" | "float64" | "int32" | "int64":
                cls_map: dict[str, type[PropertyTypeWithUnit]] = {
                    "float32": Float32,
                    "float64": Float64,
                    "int32": Int32,
                    "int64": Int64,
                }
                return cls_map[type_](
                    is_list=resource["list"],
                    max_list_size=resource.get("maxListSize"),
                    unit=cls.__load_unit_ref(resource),
                )
            case "timestamp" | "date" | "json" | "timeseries" | "file" | "sequence":
                cls_map: dict[str, type[ListablePropertyType]] = {  # type: ignore [no-redef]
                    "timestamp": Timestamp,
                    "date": Date,
                    "json": Json,
                    "timeseries": TimeSeriesReference,
                    "file": FileReference,
                    "sequence": SequenceReference,
                }
                return cls_map[type_](
                    is_list=resource["list"],
                    max_list_size=resource.get("maxListSize"),
                )
            case "direct":
                return DirectRelation(
                    container=ContainerId._load_if(resource.get("container")),
                    # The PropertyTypes are used as both read and write objects. The `list` was added later
                    # in the API for DirectRelations. Thus, we need to set the default value to False
                    # to avoid breaking changes. When used as a read object, the `list` will always be present.
                    is_list=resource.get("list", False),
                    max_list_size=resource.get("maxListSize"),
                )
            case "enum":
                values = {key: EnumValue._load(value) for key, value in resource["values"].items()}
                return Enum(values=values, unknown_value=resource.get("unknownValue"))
            case _:
                logger.warning(f"Unknown property type: {type_}")
                return cast(Self, UnknownCogniteResource(resource))


# Kept around for backwards compatibility
UnknownPropertyType: TypeAlias = UnknownCogniteResource


@dataclass
class ListablePropertyType(PropertyType, ABC):
    is_list: bool = False
    max_list_size: int | None = None

    def __post_init__(self) -> None:
        if is_positive(self.max_list_size) and not self.is_list:
            raise ValueError("is_list must be True if max_list_size is set")


@dataclass
class Text(ListablePropertyType):
    _type = "text"
    collation: str = "ucs_basic"
    max_text_size: int | None = None


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


@dataclass(frozen=True)
class UnitReference:
    external_id: str
    source_unit: str | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = {"externalId" if camel_case else "external_id": self.external_id}
        if self.source_unit:
            output["sourceUnit" if camel_case else "source_unit"] = self.source_unit
        return output

    @classmethod
    def load(cls, data: dict) -> UnitReference:
        return cls(
            external_id=data["externalId"],
            source_unit=data.get("sourceUnit"),
        )


@dataclass(frozen=True)
class UnitSystemReference:
    unit_system_name: str

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"unitSystemName" if camel_case else "unit_system_name": self.unit_system_name}

    @classmethod
    def load(cls, data: dict) -> UnitSystemReference:
        return cls(unit_system_name=data["unitSystemName"])


@dataclass
class PropertyTypeWithUnit(ListablePropertyType, ABC):
    unit: UnitReference | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.unit:
            output["unit"] = self.unit.dump(camel_case)
        return output


@dataclass
class Float32(PropertyTypeWithUnit):
    _type = "float32"


@dataclass
class Float64(PropertyTypeWithUnit):
    _type = "float64"


@dataclass
class Int32(PropertyTypeWithUnit):
    _type = "int32"


@dataclass
class Int64(PropertyTypeWithUnit):
    _type = "int64"


@dataclass
class CDFExternalIdReference(ListablePropertyType, ABC): ...


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
class DirectRelation(ListablePropertyType):
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


@dataclass
class EnumValue(CogniteResource):
    name: str | None = None
    description: str | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            name=resource.get("name"),
            description=resource.get("description"),
        )


@dataclass
class Enum(PropertyType):
    _type = "enum"
    values: dict[str, EnumValue]
    unknown_value: str | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = {
            "type": self._type,
            "values": {k: v.dump(camel_case) for k, v in self.values.items()},
        }
        if self.unknown_value is not None:
            output["unknownValue" if camel_case else "unknown_value"] = self.unknown_value
        return output
