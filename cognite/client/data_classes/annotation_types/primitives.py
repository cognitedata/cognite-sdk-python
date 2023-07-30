from __future__ import annotations

import json
from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, cast

from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._text import convert_dict_to_case

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


class VisionResource(ABC, CogniteResource):
    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        dumped = {}
        for k, v in vars(self).items():
            if v is None or k.startswith("_"):
                continue
            if isinstance(v, list) and all(isinstance(item, VisionResource) for item in v):
                v = [item.dump(camel_case) for item in v]
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
        pd = cast(Any, utils._auxiliary.local_import("pandas"))
        return pd.Series(self.dump(camel_case), name="value").to_frame()


@dataclass
class Point(VisionResource):
    x: float
    y: float

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> Point:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(x=resource["x"], y=resource["y"])


PointDict = Dict[str, float]


def _process_vertices(vertices: Union[List[PointDict], List[Point]]) -> List[Point]:
    processed_vertices: List[Point] = []
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

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> BoundingBox:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(x_min=resource["xMin"], x_max=resource["xMax"], y_min=resource["yMin"], y_max=resource["yMax"])


@dataclass
class CdfResourceRef(VisionResource):
    # A valid reference instance contains exactly one of these
    id: Optional[int] = None
    external_id: Optional[str] = None

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> CdfResourceRef:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(id=resource.get("id"), external_id=resource.get("externalId"))


@dataclass
class Polygon(VisionResource):
    # A valid polygon contains *at least* three vertices
    vertices: List[Point]

    def __post_init__(self) -> None:
        self.vertices = _process_vertices(self.vertices)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> Polygon:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(vertices=[Point._load(vertex) for vertex in resource["vertices"]])


@dataclass
class PolyLine(VisionResource):
    # A valid polyline contains *at least* two vertices
    vertices: List[Point]

    def __post_init__(self) -> None:
        self.vertices = _process_vertices(self.vertices)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> PolyLine:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(vertices=[Point._load(vertex) for vertex in resource["vertices"]])
