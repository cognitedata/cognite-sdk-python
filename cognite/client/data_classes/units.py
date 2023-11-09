from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._text import convert_dict_to_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class UnitConversion:
    """
    The conversion between a unit and its base unit.

    Args:
        multiplier (float): The multiplier to convert from the unit to the base unit.
        offset (float): The offset to convert from the unit to the base unit.
    """

    multiplier: float
    offset: float

    @classmethod
    def load(cls, data: dict[str, Any]) -> Self:
        return cls(
            multiplier=data["multiplier"],
            offset=data["offset"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "multiplier": self.multiplier,
            "offset": self.offset,
        }


class UnitID(CogniteResource):
    """
    Unit Identifier

    Args:
        unit_external_id (str): External ID of the unit.
        name (str): Name of the unit.
    """

    def __init__(self, unit_external_id: str, name: str) -> None:
        self.unit_external_id = unit_external_id
        self.name = name

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            unit_external_id=resource["unitExternalId"],
            name=resource["name"],
        )


class Unit(CogniteResource):
    """
    This class represents a Unit in CDF.

    Args:
        external_id (str): A unique identifier of the unit.
        name (str): The name of the unit, e.g. DEG_C for Celsius.
        long_name (str): A more descriptive name of the unit, e.g., degrees Celsius.
        symbol (str): The symbol of the unit, e.g., °C.
        alias_names (list[str]): List of alias names for the unit, e.g., Degree C, degC, °C, and so on.
        quantity (str): The quantity of the unit, e.g., temperature.
        conversion (UnitConversion): The conversion between the unit and its base unit. For example, the base unit for
            temperature is Kelvin, and the conversion from Celsius to Kelvin is multiplier = 1, offset = 273.15.
        source (str | None): The source of the unit, e.g., qudt.org
        source_reference (str | None): The reference to the source of the unit, e.g., http://qudt.org/vocab/unit/DEG_C
    """

    def __init__(
        self,
        external_id: str,
        name: str,
        long_name: str,
        symbol: str,
        alias_names: list[str],
        quantity: str,
        conversion: UnitConversion,
        source: str | None = None,
        source_reference: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.long_name = long_name
        self.symbol = symbol
        self.alias_names = alias_names
        self.quantity = quantity
        self.conversion = conversion
        self.source = source
        self.source_reference = source_reference

    def as_unit_id(self) -> UnitID:
        """Returns the UnitID of this unit."""
        return UnitID(unit_external_id=self.external_id, name=self.name)

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            long_name=resource["longName"],
            symbol=resource["symbol"],
            alias_names=resource["aliasNames"],
            quantity=resource["quantity"],
            conversion=UnitConversion.load(resource["conversion"]),
            source=resource.get("source"),
            source_reference=resource.get("sourceReference"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case)
        dumped["conversion"] = self.conversion.dump(camel_case)
        return convert_dict_to_case(dumped, camel_case)


class UnitList(CogniteResourceList[Unit]):
    """List of Units"""

    _RESOURCE = Unit

    def as_external_ids(self) -> list[str]:
        return [unit.external_id for unit in self]


class UnitSystem(CogniteResource):
    """
    This class represents a Unit System in CDF.

    Args:
        name (str): The name of the unit system, e.g., SI and Imperial.
        quantities (list[UnitID]): The quantities of the unit system, e.g., length, mass, and so on.

    """

    def __init__(self, name: str, quantities: list[UnitID]) -> None:
        self.name = name
        self.quantities = quantities

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            quantities=[UnitID._load(quantity) for quantity in resource["quantities"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"name": self.name, "quantities": [quantity.dump(camel_case) for quantity in self.quantities]}


class UnitSystemList(CogniteResourceList[UnitSystem]):
    """List of Unit Systems"""

    _RESOURCE = UnitSystem

    def as_names(self) -> list[str]:
        return [unit_system.name for unit_system in self]
