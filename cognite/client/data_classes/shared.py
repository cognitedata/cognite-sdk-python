from __future__ import annotations

from typing import Any, Literal

from cognite.client.data_classes._base import CognitePropertyClassUtil, Geometry
from cognite.client.utils._text import convert_all_keys_to_camel_case


class TimestampRange(dict):
    """Range between two timestamps.

    Args:
        max (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        **kwargs (Any): No description.
    """

    def __init__(self, max: int | None = None, min: int | None = None, **kwargs: Any) -> None:
        self.max = max
        self.min = min
        self.update(kwargs)

    max = CognitePropertyClassUtil.declare_property("max")
    min = CognitePropertyClassUtil.declare_property("min")


class AggregateResult(dict):
    """Aggregation group

    Args:
        count (int | None): Size of the aggregation group
        **kwargs (Any): No description.
    """

    def __init__(self, count: int | None = None, **kwargs: Any) -> None:
        super().__init__(count=count, **kwargs)
        self.count = count


class AggregateUniqueValuesResult(AggregateResult):
    """Aggregation group

    Args:
        count (int | None): Size of the aggregation group
        value (int | str | None): A unique value from the requested field
        **kwargs (Any): No description.
    """

    def __init__(self, count: int | None = None, value: int | str | None = None, **kwargs: Any) -> None:
        super().__init__(count=count, value=value, **kwargs)
        self.value = value


class AggregateBucketResult(AggregateResult):
    """Aggregation group

    Args:
        count (int | None): Size of the bucket
        value (int | str | None): A unique value for the bucket
        **kwargs (Any): No description.
    """

    def __init__(self, count: int | None = None, value: int | str | None = None, **kwargs: Any) -> None:
        super().__init__(count=count, value=value, **kwargs)
        self.value = value


class GeometryFilter(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args:
        type (Literal["Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]): The geometry type.
        coordinates (list): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.
    """

    _VALID_TYPES = frozenset({"Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"})

    def __init__(
        self,
        type: Literal["Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"],
        coordinates: list,
    ) -> None:
        if type not in self._VALID_TYPES:
            raise ValueError(f"type must be one of {self._VALID_TYPES}")
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")


class GeoLocation(dict):
    """A GeoLocation object conforming to the GeoJSON spec.

    Args:
        type (Literal['Feature']): The GeoJSON type. Currently only 'Feature' is supported.
        geometry (Geometry): The geometry. One of 'Point', 'MultiPoint, 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
        properties (dict | None): Optional additional properties in a String key -> Object value format.
    """

    _VALID_TYPES = frozenset({"Feature"})

    def __init__(self, type: Literal["Feature"], geometry: Geometry, properties: dict | None = None) -> None:
        if type not in self._VALID_TYPES:
            raise ValueError("Only the 'Feature' type is supported.")
        self.type = type
        self.geometry = geometry
        self.properties = properties

    type = CognitePropertyClassUtil.declare_property("type")
    geometry = CognitePropertyClassUtil.declare_property("geometry")
    properties = CognitePropertyClassUtil.declare_property("properties")

    @classmethod
    def _load(cls, raw_geo_location: dict[str, Any]) -> GeoLocation:
        return cls(
            type=raw_geo_location.get("type", "Feature"),
            geometry=raw_geo_location["geometry"],
            properties=raw_geo_location.get("properties"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)


class GeoLocationFilter(dict):
    """Return only the resource matching the specified geographic relation.

    Args:
        relation (str): One of the following supported queries: INTERSECTS, DISJOINT, WITHIN.
        shape (GeometryFilter): Represents the points, curves and surfaces in the coordinate space.
    """

    def __init__(self, relation: str, shape: GeometryFilter) -> None:
        self.relation = relation
        self.shape = shape

    relation = CognitePropertyClassUtil.declare_property("relation")
    shape = CognitePropertyClassUtil.declare_property("shape")

    @classmethod
    def _load(cls, raw_geo_location_filter: dict[str, Any]) -> GeoLocationFilter:
        return cls(relation=raw_geo_location_filter["relation"], shape=raw_geo_location_filter["shape"])

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)
