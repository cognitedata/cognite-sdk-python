from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from cognite.client import utils
from cognite.client.data_classes._base import EXCLUDE_VALUE


class VisionResource:
    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        return {
            utils._auxiliary.to_camel_case(key)
            if camel_case
            else key: value.dump(camel_case=camel_case)
            if hasattr(value, "dump")
            else value
            for key, value in self.__dict__.items()
            if value not in EXCLUDE_VALUE and not key.startswith("_")
        }

    def to_pandas(self, camel_case: bool = False) -> Dict[str, Any]:
        pd = utils._auxiliary.local_import("pandas")
        df = pd.DataFrame(columns=["value"])

        for key, value in self.__dict__.items():
            if isinstance(value, list) and all(hasattr(v, "dump") for v in value):
                df.loc[utils._auxiliary.to_camel_case(key) if camel_case else key] = [
                    [v.dump(camel_case=camel_case) for v in value]
                ]
            if hasattr(value, "dump"):
                df.loc[utils._auxiliary.to_camel_case(key) if camel_case else key] = [value.dump(camel_case=camel_case)]
            else:
                if value not in EXCLUDE_VALUE:
                    df.loc[utils._auxiliary.to_camel_case(key) if camel_case else key] = [value]

        return df

    def _repr_html_(self):
        return self.to_pandas(camel_case=False)._repr_html_()


@dataclass
class Point(VisionResource):
    x: float
    y: float


PointDict = Dict[str, float]


def _process_vertices(vertices: List[Union[PointDict, Point]]) -> List[Point]:
    processed_vertices = []
    for v in vertices:
        if isinstance(v, Point):
            processed_vertices.push(v)
        elif isinstance(v, Dict) and v.keys() == ["x", "y"]:
            processed_vertices.push(Point(**v))
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
    id: Optional[int] = None
    external_id: Optional[str] = None


@dataclass
class Polygon(VisionResource):
    # A valid polygon contains *at least* three vertices
    vertices: List[Point]

    def __post_init__(self) -> None:
        self.vertices = _process_vertices(self.vertices)


@dataclass
class PolyLine(VisionResource):
    # A valid polyline contains *at least* two vertices
    vertices: List[Point]

    def __post_init__(self) -> None:
        self.vertices = _process_vertices(self.vertices)
