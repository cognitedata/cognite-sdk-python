from typing import *

from cognite.client.data_classes._base import *


class TimestampRange(dict):
    """Range between two timestamps.

    Args:
        max (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(self, max: int = None, min: int = None, **kwargs):
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

    def __init__(self, count: int = None, **kwargs):
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class AggregateUniqueValuesResult(AggregateResult):
    """Aggregation group

    Args:
        count (int): Size of the aggregation group
        value (Union(int, str)): A unique value from the requested field
    """

    def __init__(self, count: int = None, value: Union[int, str] = None, **kwargs):
        super().__init__(count, **kwargs)
        self.value = value

    value = CognitePropertyClassUtil.declare_property("value")


class Geometry(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args: type (str): The geometry type. One of 'Point', 'MultiPoint, 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
          coordinates (List): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.
    """

    def __init__(self, type: str, coordinates: List):
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")

    @classmethod
    def _load(self, raw_geometry: Dict[str, Any]):
        return Geometry(type=raw_geometry["type"], coordinates=raw_geometry["coordinates"])

    def dump(self, camel_case: bool = False):
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


class GeometryFilter(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args: type (str): The geometry type. One of 'Point', 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
          coordinates (List): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.
    """

    def __init__(self, type: str, coordinates: List):
        self.type = type
        self.coordinates = coordinates

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")


class GeoLocation(dict):
    """A GeoLocation object conforming to the GeoJSON spec.

    Args: type (str): The GeoJSON type. Currently only 'Feature' is supported.
          geometry (object): The geometry type. One of 'Point', 'MultiPoint, 'LineString', 'MultiLineString', 'Polygon', or 'MultiPolygon'.
          properties (object): Additional properties in a String key -> Object value format.
    """

    def __init__(self, type: str, geometry: Geometry, properties: dict = None):
        self.type = type
        self.geometry = geometry
        self.properties = properties

    type = CognitePropertyClassUtil.declare_property("type")
    geometry = CognitePropertyClassUtil.declare_property("geometry")
    properties = CognitePropertyClassUtil.declare_property("properties")

    @classmethod
    def _load(self, raw_geoLocation: Dict[str, Any]):
        return GeoLocation(
            type=raw_geoLocation["type"],
            geometry=raw_geoLocation["geometry"],
            properties=raw_geoLocation.get("properties"),
        )

    def dump(self, camel_case: bool = False):
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}


class GeoLocationFilter(dict):
    """Return only the file matching the specified geographic relation.

    Args: relation (str): One of the following supported queries: INTERSECTS, DISJOINT, WITHIN.
          shape (GeometryFilter): Represents the points, curves and surfaces in the coordinate space.
    """

    def __init__(self, relation: str, shape: GeometryFilter):
        self.relation = relation
        self.shape = shape

    relation = CognitePropertyClassUtil.declare_property("relation")
    shape = CognitePropertyClassUtil.declare_property("shape")

    @classmethod
    def _load(self, raw_geoLocation_filter: Dict[str, Any]):
        return GeoLocationFilter(relation=raw_geoLocation_filter["relation"], shape=raw_geoLocation_filter["shape"])

    def dump(self, camel_case: bool = False):
        dump_key = lambda key: key if not camel_case else utils._auxiliary.to_camel_case(key)
        return {dump_key(key): value for key, value in self.items()}
