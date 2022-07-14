from typing import Any, Dict, List, Union

from cognite.client import utils
from cognite.client.data_classes._base import CognitePropertyClassUtil


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
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class AggregateUniqueValuesResult(AggregateResult):
    """Aggregation group

    Args:
        count (int): Size of the aggregation group
        value (Union(int, str)): A unique value from the requested field
    """

    def __init__(self, count: int = None, value: Union[int, str] = None, **kwargs: Any):
        super().__init__(count, **kwargs)
        self.value = value

    value = CognitePropertyClassUtil.declare_property("value")


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

    def __init__(self, type: str, coordinates: List):
        valid_types = ["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]
        if type not in valid_types:
            raise ValueError("type must be one of " + str(valid_types))
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")

    @classmethod
    def _load(self, raw_geometry: Dict[str, Any]) -> "Geometry":
        return Geometry(type=raw_geometry["type"], coordinates=raw_geometry["coordinates"])

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


class GeometryFilter(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args: type (str): The geometry type. One of 'Point', 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
          coordinates (List): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.
    """

    def __init__(self, type: str, coordinates: List):
        valid_types = ["Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]
        if type not in valid_types:
            raise ValueError("type must be one of " + str(valid_types))
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

    def __init__(self, type: str, geometry: Geometry, properties: dict = None):
        if type != "Feature":
            raise ValueError("Only the 'Feature' type is supported.")
        self.type = type
        self.geometry = geometry
        self.properties = properties

    type = CognitePropertyClassUtil.declare_property("type")
    geometry = CognitePropertyClassUtil.declare_property("geometry")
    properties = CognitePropertyClassUtil.declare_property("properties")

    @classmethod
    def _load(self, raw_geoLocation: Dict[str, Any]) -> "GeoLocation":
        return GeoLocation(
            type=raw_geoLocation.get("type", "Feature"),
            geometry=raw_geoLocation["geometry"],
            properties=raw_geoLocation.get("properties"),
        )

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


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
    def _load(self, raw_geoLocation_filter: Dict[str, Any]) -> "GeoLocationFilter":
        return GeoLocationFilter(relation=raw_geoLocation_filter["relation"], shape=raw_geoLocation_filter["shape"])

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}
