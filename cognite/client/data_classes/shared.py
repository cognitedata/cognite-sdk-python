from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from cognite.client.data_classes._base import CognitePropertyClassUtil, Geometry
from cognite.client.utils._text import convert_all_keys_to_camel_case


class TimestampRange(dict):
    """Range between two timestamps.

    Args:
        max (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(self, max: Optional[int] = None, min: Optional[int] = None, **kwargs: Any):
        self.max = max
        self.min = min
        self.update(kwargs)

    max = CognitePropertyClassUtil.declare_property("max")
    min = CognitePropertyClassUtil.declare_property("min")


class AggregateResult(dict):
    """Aggregation group

    Args:
        count (int): Size of the aggregation group
    """

    def __init__(self, count: Optional[int] = None, **kwargs: Any):
        super().__init__(count=count, **kwargs)
        self.count = count


class AggregateUniqueValuesResult(AggregateResult):
    """Aggregation group

    Args:
        count (int): Size of the aggregation group
        value (Union(int, str)): A unique value from the requested field
    """

    def __init__(self, count: Optional[int] = None, value: Optional[Union[int, str]] = None, **kwargs: Any):
        super().__init__(count=count, value=value, **kwargs)
        self.value = value


class AggregateBucketResult(AggregateResult):
    """Aggregation group

    Args:
        count (int): Size of the bucket
        value (Union(int, str)): A unique value for the bucket
    """

    def __init__(self, count: Optional[int] = None, value: Optional[Union[int, str]] = None, **kwargs: Any):
        super().__init__(count=count, value=value, **kwargs)
        self.value = value


class GeometryFilter(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args: type (str): The geometry type. One of 'Point', 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
          coordinates (List): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.
    """

    _VALID_TYPES = frozenset({"Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"})

    def __init__(
        self,
        type: Literal["Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"],
        coordinates: List,
    ):
        if type not in self._VALID_TYPES:
            raise ValueError(f"type must be one of {self._VALID_TYPES}")
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")


class GeoLocation(dict):
    """A GeoLocation object conforming to the GeoJSON spec.

    Args: type (str): The GeoJSON type. Currently only 'Feature' is supported.
          geometry (object): The geometry type. One of 'Point', 'MultiPoint, 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
          properties (object): Optional additional properties in a String key -> Object value format.
    """

    _VALID_TYPES = frozenset({"Feature"})

    def __init__(self, type: Literal["Feature"], geometry: Geometry, properties: Optional[dict] = None):
        if type not in self._VALID_TYPES:
            raise ValueError("Only the 'Feature' type is supported.")
        self.type = type
        self.geometry = geometry
        self.properties = properties

    type = CognitePropertyClassUtil.declare_property("type")
    geometry = CognitePropertyClassUtil.declare_property("geometry")
    properties = CognitePropertyClassUtil.declare_property("properties")

    @classmethod
    def _load(cls, raw_geo_location: Dict[str, Any]) -> GeoLocation:
        return cls(
            type=raw_geo_location.get("type", "Feature"),
            geometry=raw_geo_location["geometry"],
            properties=raw_geo_location.get("properties"),
        )

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)


class GeoLocationFilter(dict):
    """Return only the resource matching the specified geographic relation.

    Args: relation (str): One of the following supported queries: INTERSECTS, DISJOINT, WITHIN.
          shape (GeometryFilter): Represents the points, curves and surfaces in the coordinate space.
    """

    def __init__(self, relation: str, shape: GeometryFilter):
        self.relation = relation
        self.shape = shape

    relation = CognitePropertyClassUtil.declare_property("relation")
    shape = CognitePropertyClassUtil.declare_property("shape")

    @classmethod
    def _load(cls, raw_geo_location_filter: Dict[str, Any]) -> GeoLocationFilter:
        return cls(relation=raw_geo_location_filter["relation"], shape=raw_geo_location_filter["shape"])

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)
