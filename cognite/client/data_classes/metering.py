from __future__ import annotations

from typing import Any

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList


class MeteringDataPoint:
    """A single timestamped data point with average value.

    Args:
        timestamp (int): The timestamp for this data point, in milliseconds since epoch.
        average (float): The average metric value for this time bucket.
    """

    def __init__(self, timestamp: int, average: float) -> None:
        self.timestamp = timestamp
        self.average = average

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> MeteringDataPoint:
        return cls(timestamp=resource["timestamp"], average=resource["average"])

    def dump(self) -> dict[str, Any]:
        return {"timestamp": self.timestamp, "average": self.average}

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, MeteringDataPoint) and self.dump() == other.dump()

    def __repr__(self) -> str:
        return f"MeteringDataPoint(timestamp={self.timestamp}, average={self.average})"


class MeteringData(CogniteResource):
    """A singular representation of the current consumption.

    Metering is identified by an id containing the service name and a service-scoped metering name.
    For instance ``atlas.monthly_ai_tokens`` is the id of the ``atlas`` service metering ``monthly_ai_tokens``.
    Service and metering names are always in ``lower_snake_case``.

    Args:
        meter_id (str): The metering ID, e.g. ``atlas.monthly_ai_tokens``.
        datapoints (list[MeteringDataPoint] | None): Timestamped data points with average values. Empty unless both ``start`` and ``number_of_datapoints`` are provided in the request.
    """

    def __init__(self, meter_id: str, datapoints: list[MeteringDataPoint] | None = None) -> None:
        self.meter_id = meter_id
        self.datapoints: list[MeteringDataPoint] = datapoints or []

    def as_id(self) -> str:
        """Returns the meter ID."""
        return self.meter_id

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> MeteringData:
        return cls(
            meter_id=resource["meterId"],
            datapoints=[MeteringDataPoint._load(dp) for dp in resource.get("datapoints", [])],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        key = "meterId" if camel_case else "meter_id"
        return {
            key: self.meter_id,
            "datapoints": [dp.dump() for dp in self.datapoints],
        }


class MeteringDataList(CogniteResourceList[MeteringData]):
    _RESOURCE = MeteringData

    def as_ids(self) -> list[str]:
        """Returns a list of meter IDs."""
        return [item.as_id() for item in self]
