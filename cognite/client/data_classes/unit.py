from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class UnitConversion:
    multiplier: float
    offset: float

    @classmethod
    def load(cls, data: dict[str, Any]) -> Self:
        return cls(
            multiplier=data["multiplier"],
            offset=data["offset"],
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            "multiplier": self.multiplier,
            "offset": self.offset,
        }


class UnitID(CogniteResource):
    def __init__(self, unit_external_id: str, name: str):
        self.unit_external_id = unit_external_id
        self.name = name

    @classmethod
    def _load(cls, resource: dict[str, Any] | str, cognite_client: CogniteClient | None = None) -> Self:
        loaded = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            unit_external_id=loaded["unitExternalId"],
            name=loaded["name"],
        )


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

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Self:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            external_id=resource["externalId"],
            name=resource["name"],
            long_name=resource["longName"],
            alias_names=resource["aliasNames"],
            quantity=resource["quantity"],
            conversion=UnitConversion.load(resource["conversion"]),
            source=resource.get("source"),
            source_reference=resource.get("sourceReference"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            ("externalId" if camel_case else "external_id"): self.external_id,
            "name": self.name,
            ("longName" if camel_case else "long_name"): self.long_name,
            ("aliasNames" if camel_case else "alias_names"): self.alias_names,
            "quantity": self.quantity,
            "conversion": self.conversion.dump(camel_case),
            "source": self.source,
            ("sourceReference" if camel_case else "source_reference"): self.source_reference,
        }


class UnitList(CogniteResourceList[Unit]):
    _RESOURCE = Unit

    def as_external_ids(self) -> list[str]:
        return [unit.external_id for unit in self]


class UnitSystem(CogniteResource):
    def __init__(
        self,
        name: str,
        quantities: list[UnitID],
    ):
        self.name = name
        self.quantities = quantities

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Self:
        resource = json.loads(resource) if isinstance(resource, str) else resource

        return cls(
            name=resource["name"],
            quantities=[UnitID._load(quantity) for quantity in resource["quantities"]],
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {"name": self.name, "quantities": [quantity.dump(camel_case) for quantity in self.quantities]}


class UnitSystemList(CogniteResourceList[UnitSystem]):
    _RESOURCE = UnitSystem

    def as_names(self) -> list[str]:
        return [unit_system.name for unit_system in self]
