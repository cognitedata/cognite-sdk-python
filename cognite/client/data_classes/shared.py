from __future__ import annotations

from typing import Any, Literal, Sequence, cast

from typing_extensions import Self

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


class GeometryFilter(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args:
        type (Literal["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]): The geometry type.
        coordinates (Sequence[float] | Sequence[Sequence[float]] | Sequence[Sequence[Sequence[float]]] | Sequence[Sequence[Sequence[Sequence[float]]]]): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.

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
        coordinates: Sequence[float]
        | Sequence[Sequence[float]]
        | Sequence[Sequence[Sequence[float]]]
        | Sequence[Sequence[Sequence[Sequence[float]]]],
    ) -> None:
        if type not in self._VALID_TYPES:
            raise ValueError(f"type must be one of {self._VALID_TYPES}")
        self.type = type
        self.coordinates = cast(list, coordinates)

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")

    @classmethod
    def load(cls, raw_geometry: dict[str, Any]) -> Self:
        return cls(type=raw_geometry["type"], coordinates=raw_geometry["coordinates"])

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)


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
    def load(cls, raw_geo_location: dict[str, Any]) -> GeoLocation:
        return cls(
            type=raw_geo_location.get("type", "Feature"),
            geometry=raw_geo_location["geometry"],
            properties=raw_geo_location.get("properties"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        result = convert_all_keys_to_camel_case(self) if camel_case else dict(self)
        if self.geometry:
            result["geometry"] = dict(self.geometry)
        return result


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
    def load(cls, raw_geo_location_filter: dict[str, Any]) -> GeoLocationFilter:
        return cls(relation=raw_geo_location_filter["relation"], shape=raw_geo_location_filter["shape"])

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return convert_all_keys_to_camel_case(self) if camel_case else dict(self)
