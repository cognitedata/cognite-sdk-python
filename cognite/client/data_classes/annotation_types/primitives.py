from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._auxiliary import local_import
from cognite.client.utils._text import convert_dict_to_case

if TYPE_CHECKING:
    import pandas


class VisionResource(CogniteResource):
    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        dumped = {}
        for k, v in vars(self).items():
            if v is None or k.startswith("_"):
                continue
            if isinstance(v, list) and all(isinstance(item, VisionResource) for item in v):
                v = [item.dump(camel_case) for item in v]
            elif isinstance(v, dict) and all(isinstance(item, VisionResource) for item in v.values()):
                v = {item_key: item_value.dump(camel_case) for item_key, item_value in v.items()}
            elif isinstance(v, VisionResource):
                v = v.dump(camel_case)
            dumped[k] = v

        return convert_dict_to_case(dumped, camel_case)

    def to_pandas(self, camel_case: bool = False) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the instance into a pandas DataFrame.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = cast(Any, local_import("pandas"))
        return pd.Series(self.dump(camel_case), name="value").to_frame()


@dataclass
class Point(VisionResource):
    x: float
    y: float


def _process_vertices(vertices: list[dict[str, float]] | list[Point]) -> list[Point]:
    processed_vertices: list[Point] = []
    for v in vertices:
        if isinstance(v, Point):
            processed_vertices.append(v)
        elif isinstance(v, dict) and set(v) == set("xy"):
            processed_vertices.append(Point(**v))
        else:
            raise ValueError(f"{v} is an invalid point.")
    return processed_vertices


@dataclass
class BoundingBox(VisionResource):
    x_min: float
    x_max: float
    y_min: float
    y_max: float


@dataclass
class CdfResourceRef(VisionResource):
    # A valid reference instance contains exactly one of these
    id: int | None = None
    external_id: str | None = None


@dataclass
class Polygon(VisionResource):
    # A valid polygon contains *at least* three vertices
    vertices: list[Point]

    def __post_init__(self) -> None:
        self.vertices = _process_vertices(self.vertices)


@dataclass
class Polyline(VisionResource):
    # A valid polyline contains *at least* two vertices
    vertices: list[Point]

    def __post_init__(self) -> None:
        self.vertices = _process_vertices(self.vertices)


@dataclass
class Keypoint(VisionResource):
    point: Point
    confidence: float | None = None

    def __post_init__(self) -> None:
        if isinstance(self.point, dict):
            self.point = Point(**self.point)


@dataclass
class Attribute(VisionResource):
    type: Literal["boolean", "numerical"]
    value: bool | float
    description: str | None = None
