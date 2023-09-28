from __future__ import annotations

from dataclasses import dataclass

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList


@dataclass
class UnitConversion:
    multiplier: float
    offset: float


class UnitID(CogniteResource):
    def __init__(self, unit_external_id: str, name: str):
        self.unit_external_id = unit_external_id
        self.name = name


class Unit(CogniteResource):
    def __init__(
        self,
        external_id: str,
        name: str,
        long_name: str,
        alias_names: list[str],
        quantity: str,
        conversion: UnitConversion,
        source: str | None = None,
        source_reference: str | None = None,
    ):
        self.external_id = external_id
        self.name = name
        self.long_name = long_name
        self.alias_names = alias_names
        self.quantity = quantity
        self.conversion = conversion
        self.source = source
        self.source_reference = source_reference

    def as_id(self) -> UnitID:
        return UnitID(unit_external_id=self.external_id, name=self.name)


class UnitList(CogniteResourceList[Unit]):
    _RESOURCE = Unit


class UnitSystem(CogniteResource):
    def __init__(
        self,
        name: str,
        quantities: list[UnitID],
    ):
        self.name = name
        self.quantities = quantities


class UnitSystemList(CogniteResourceList[UnitSystem]):
    _RESOURCE = UnitSystem
