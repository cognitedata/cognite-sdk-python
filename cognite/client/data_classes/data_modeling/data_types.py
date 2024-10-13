from __future__ import annotations

import logging
from abc import ABC
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, cast

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject, UnknownCogniteObject
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.utils._auxiliary import rename_and_exclude_keys
from cognite.client.utils._text import convert_all_keys_recursive

if TYPE_CHECKING:
    from cognite.client import CogniteClient

logger = logging.getLogger(__name__)

_PROPERTY_ALIAS = {"isList": "list", "is_list": "list"}


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
    def load(cls, data: dict | tuple[str, str] | DirectRelationReference) -> DirectRelationReference:
        if isinstance(data, dict):
            return cls(space=data["space"], external_id=data["externalId"])
        elif isinstance(data, tuple) and len(data) == 2:
            return cls(*data)
        elif isinstance(data, cls):
            return data
        else:
            raise ValueError("Invalid data provided to load method. Must be dict or tuple with two elements.")

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id


@dataclass
class PropertyType(CogniteObject, ABC):
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
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> PropertyType:
        type_ = resource["type"]
        if type_ == "text":
            return Text(is_list=resource["list"], collation=resource.get("collation", "ucs_basic"))
        elif type_ == "boolean":
            return Boolean(is_list=resource["list"])
        elif type_ == "float32":
            return Float32(is_list=resource["list"], unit=cls.__load_unit_ref(resource))
        elif type_ == "float64":
            return Float64(is_list=resource["list"], unit=cls.__load_unit_ref(resource))
        elif type_ == "int32":
            return Int32(is_list=resource["list"], unit=cls.__load_unit_ref(resource))
        elif type_ == "int64":
            return Int64(is_list=resource["list"], unit=cls.__load_unit_ref(resource))
        elif type_ == "timestamp":
            return Timestamp(is_list=resource["list"])
        elif type_ == "date":
            return Date(is_list=resource["list"])
        elif type_ == "json":
            return Json(is_list=resource["list"])
        elif type_ == "timeseries":
            return TimeSeriesReference(is_list=resource["list"])
        elif type_ == "file":
            return FileReference(is_list=resource["list"])
        elif type_ == "sequence":
            return SequenceReference(is_list=resource["list"])
        elif type_ == "direct":
            return DirectRelation(
                container=ContainerId.load(container) if (container := resource.get("container")) else None,
                # The PropertyTypes are used as both read and write objects. The `list` was added later
                # in the API for DirectRelations. Thus, we need to set the default value to False
                # to avoid breaking changes. When used as a read object, the `list` will always be present.
                is_list=resource.get("list", False),
            )
        elif type_ == "enum":
            values = {key: EnumValue._load(value) for key, value in resource["values"].items()}
            return Enum(values=values, unknown_value=resource.get("unknownValue"))
        logger.warning(f"Unknown property type: {type_}")
        return cast(Self, UnknownCogniteObject(resource))


# Kept around for backwards compatibility
UnknownPropertyType: TypeAlias = UnknownCogniteObject


@dataclass
class ListablePropertyType(PropertyType, ABC):
    is_list: bool = False


@dataclass
class Text(ListablePropertyType):
    _type = "text"
    collation: str = "ucs_basic"


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
class EnumValue(CogniteObject):
    name: str | None = None
    description: str | None = None


@dataclass
class Enum(PropertyType):
    _type = "enum"
    values: dict[str, EnumValue]
    unknown_value: str | None = None
