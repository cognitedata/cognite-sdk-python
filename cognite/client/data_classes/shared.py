from __future__ import annotations

from typing import Any, Dict, List, Literal, Union, cast

from cognite.client.data_classes._base import CognitePropertyClassUtil
from cognite.client.utils._auxiliary import convert_all_keys_to_camel_case, handle_deprecated_camel_case_argument


class TimestampRange(dict):
    """Range between two timestamps.

    Args:
        max (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(self, max: int = None, min: int = None, **kwargs: Any):
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

    def __init__(self, count: int = None, **kwargs: Any):
        super().__init__(count=count, **kwargs)
        self.count = count


class AggregateUniqueValuesResult(AggregateResult):
    """Aggregation group

    Args:
        count (int): Size of the aggregation group
        value (Union(int, str)): A unique value from the requested field
    """

    def __init__(self, count: int = None, value: Union[int, str] = None, **kwargs: Any):
        super().__init__(count=count, value=value, **kwargs)
        self.value = value


class AggregateBucketResult(AggregateResult):
    """Aggregation group

    Args:
        count (int): Size of the bucket
        value (Union(int, str)): A unique value for the bucket
    """

    def __init__(self, count: int = None, value: Union[int, str] = None, **kwargs: Any):
        super().__init__(count=count, value=value, **kwargs)
        self.value = value


class Geometry(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args:
        type (str): The geometry type. One of 'Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
        coordinates (List): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.

            Point:
                Coordinates of a point in 2D space, described as an array of 2 numbers.

                Example: `[4.306640625, 60.205710352530346]`


            LineString:
                Coordinates of a line described by a list of two or more points.
                Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.

                Example: `[[30, 10], [10, 30], [40, 40]]`


            Polygon:
                List of one or more linear rings representing a shape.
                A linear ring is the boundary of a surface or the boundary of a hole in a surface. It is defined as a list consisting of 4 or more Points, where the first and last Point is equivalent.
                Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

                Example: `[[[35, 10], [45, 45], [15, 40], [10, 20], [35, 10]], [[20, 30], [35, 35], [30, 20], [20, 30]]]`
                type: array

            MultiPoint:
                List of Points. Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

                Example: `[[35, 10], [45, 45]]`

            MultiLineString:
                    List of lines where each line (LineString) is defined as a list of two or more points.
                    Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.

                    Example: `[[[30, 10], [10, 30]], [[35, 10], [10, 30], [40, 40]]]`

            MultiPolygon:
                List of multiple polygons.

                Each polygon is defined as a list of one or more linear rings representing a shape.

                A linear ring is the boundary of a surface or the boundary of a hole in a surface. It is defined as a list consisting of 4 or more Points, where the first and last Point is equivalent.

                Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

                Example: `[[[[30, 20], [45, 40], [10, 40], [30, 20]]], [[[15, 5], [40, 10], [10, 20], [5, 10], [15, 5]]]]`
    """

    _VALID_TYPES = frozenset({"Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"})

    def __init__(
        self,
        type: Literal["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"],
        coordinates: List,
    ):
        if type not in self._VALID_TYPES:
            raise ValueError(f"type must be one of {self._VALID_TYPES}")
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")

    @classmethod
    def _load(cls, raw_geometry: Dict[str, Any]) -> Geometry:
        return cls(type=raw_geometry["type"], coordinates=raw_geometry["coordinates"])

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)


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

    def __init__(self, type: Literal["Feature"], geometry: Geometry, properties: dict = None):
        if type not in self._VALID_TYPES:
            raise ValueError("Only the 'Feature' type is supported.")
        self.type = type
        self.geometry = geometry
        self.properties = properties

    type = CognitePropertyClassUtil.declare_property("type")
    geometry = CognitePropertyClassUtil.declare_property("geometry")
    properties = CognitePropertyClassUtil.declare_property("properties")

    @classmethod
    def _load(cls, raw_geo_location: Dict[str, Any] = None, **kwargs: Dict[str, Any]) -> GeoLocation:
        # TODO: Remove support for old argument name in major version 6
        raw_geo_location = cast(
            Dict[str, Any],
            handle_deprecated_camel_case_argument(raw_geo_location, "raw_geoLocation", "_load", kwargs),
        )
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
    def _load(cls, raw_geo_location_filter: Dict[str, Any] = None, **kwargs: Dict[str, Any]) -> GeoLocationFilter:
        # TODO: Remove support for old argument name in major version 6
        raw_geo_location_filter = cast(
            Dict[str, Any],
            handle_deprecated_camel_case_argument(raw_geo_location_filter, "raw_geoLocation_filter", "_load", kwargs),
        )
        return cls(relation=raw_geo_location_filter["relation"], shape=raw_geo_location_filter["shape"])

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)
