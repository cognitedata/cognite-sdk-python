from __future__ import annotations

from collections.abc import Collection, Sequence
from datetime import datetime
from typing import Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteFilter, CogniteResource, UnknownCogniteResource
from cognite.client.utils._time import timestamp_to_ms


class TimestampRange(CogniteResource):
    """Range between two timestamps.

    Args:
        max (int | float | str | datetime | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds, a string in time-shift format or a datetime object.
        min (int | float | str | datetime | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds, a string in time-shift format or a datetime object.
        **_ (Any): No description.
    """

    def __init__(
        self, max: int | float | str | datetime | None = None, min: int | float | str | datetime | None = None, **_: Any
    ) -> None:
        self.max = timestamp_to_ms(max) if max is not None else None
        self.min = timestamp_to_ms(min) if min is not None else None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(max=resource.get("max"), min=resource.get("min"))


class AggregateResult(CogniteResource):
    """Aggregation group

    Args:
        count (int): Size of the aggregation group
    """

    def __init__(self, count: int) -> None:
        self.count = count

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(count=resource["count"])


class AggregateUniqueValuesResult(AggregateResult):
    """Aggregation group

    Args:
        count (int): Size of the aggregation group
        value (int | str | None): A unique value from the requested field
    """

    def __init__(self, count: int, value: int | str | None) -> None:
        super().__init__(count=count)
        self.value = value

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(count=resource["count"], value=resource.get("value"))


class Geometry(CogniteResource):
    """Represents the points, curves and surfaces in the coordinate space.

    Args:
        type (Literal['Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']): The geometry type.
        coordinates (list): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.
        geometries (Collection[Geometry] | None): No description.

    Examples:
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
        coordinates: list,
        geometries: Collection[Geometry] | None = None,
    ) -> None:
        if type not in self._VALID_TYPES:
            raise ValueError(f"type must be one of {self._VALID_TYPES}")
        self.type = type
        self.coordinates = coordinates
        self.geometries = geometries and list(geometries)

    @classmethod
    def _load(cls, raw_geometry: dict[str, Any]) -> Geometry:
        return cls(
            type=raw_geometry["type"],
            coordinates=raw_geometry["coordinates"],
            geometries=raw_geometry.get("geometries"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case)
        if self.geometries:
            dumped["geometries"] = [g.dump(camel_case) for g in self.geometries]
        return dumped


class GeometryFilter(CogniteFilter):
    """Represents the points, curves and surfaces in the coordinate space.

    Args:
        type (Literal['Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon']): The geometry type.
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
        self.coordinates = list(coordinates)

    @classmethod
    def _load(cls, raw_geometry: dict[str, Any]) -> Self:
        return cls(type=raw_geometry["type"], coordinates=raw_geometry["coordinates"])

    @classmethod
    def load(cls, raw_geometry: dict[str, Any]) -> Self:
        return cls(type=raw_geometry["type"], coordinates=raw_geometry["coordinates"])


class GeoLocation(CogniteResource):
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

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> GeoLocation:
        if "type" in resource:
            return cls(
                type=resource["type"],
                geometry=Geometry._load(resource["geometry"]),
                properties=resource.get("properties"),
            )
        # Years ago, the API didn't enforce the current restriction on the type field:
        return UnknownCogniteResource(resource)  # type: ignore[return-value]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        if self.geometry:
            result["geometry"] = self.geometry.dump(camel_case)
        return result


class GeoLocationFilter(CogniteResource):
    """Return only the resource matching the specified geographic relation.

    Args:
        relation (str): One of the following supported queries: INTERSECTS, DISJOINT, WITHIN.
        shape (GeometryFilter): Represents the points, curves and surfaces in the coordinate space.
    """

    def __init__(self, relation: str, shape: GeometryFilter) -> None:
        self.relation = relation
        self.shape = shape

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> GeoLocationFilter:
        return cls(relation=resource["relation"], shape=resource["shape"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case)
        if isinstance(self.shape, GeometryFilter):
            dumped["shape"] = self.shape.dump(camel_case)
        return dumped
