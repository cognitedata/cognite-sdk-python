from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
)
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import CogniteClient

_WARNING = FeaturePreviewWarning(api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators")


@dataclass
class SimulationValueUnitName(CogniteObject):
    name: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
        )

    def __post_init__(self) -> None:
        _WARNING.warn()

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


@dataclass
class SimulationInputOverride(CogniteObject):
    reference_id: str
    value: str | int | float | list[str] | list[int] | list[float]
    unit: SimulationValueUnitName | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            reference_id=resource["referenceId"],
            value=resource["value"],
            unit=SimulationValueUnitName._load(resource["unit"], cognite_client) if "unit" in resource else None,
        )

    def __post_init__(self) -> None:
        _WARNING.warn()

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if self.unit is not None:
            output["unit"] = self.unit.dump(camel_case=camel_case)

        return output
