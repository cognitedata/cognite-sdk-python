
from dataclasses import dataclass
from cognite.client import utils
from cognite.client.data_classes._base import EXCLUDE_VALUE, CogniteResource
if TYPE_CHECKING:
    import pandas

class VisionResource(CogniteResource):

    def dump(self, camel_case=False):
        return {(utils._auxiliary.to_camel_case(key) if camel_case else key): (value.dump(camel_case=camel_case) if hasattr(value, 'dump') else value) for (key, value) in self.__dict__.items() if ((value not in EXCLUDE_VALUE) and (not key.startswith('_')))}

    def to_pandas(self, camel_case=False):
        pd = cast(Any, utils._auxiliary.local_import('pandas'))
        df = pd.DataFrame(columns=['value'])
        for (key, value) in self.__dict__.items():
            if (isinstance(value, list) and all((hasattr(v, 'dump') for v in value))):
                df.loc[(utils._auxiliary.to_camel_case(key) if camel_case else key)] = [[v.dump(camel_case=camel_case) for v in value]]
            if hasattr(value, 'dump'):
                df.loc[(utils._auxiliary.to_camel_case(key) if camel_case else key)] = [value.dump(camel_case=camel_case)]
            elif (value not in EXCLUDE_VALUE):
                df.loc[(utils._auxiliary.to_camel_case(key) if camel_case else key)] = [value]
        return df

@dataclass
class Point(VisionResource):
    x: float
    y: float
PointDict = Dict[(str, float)]

def _process_vertices(vertices):
    processed_vertices: List[Point] = []
    for v in vertices:
        if isinstance(v, Point):
            processed_vertices.append(v)
        elif (isinstance(v, dict) and (set(v) == set('xy'))):
            processed_vertices.append(Point(**v))
        else:
            raise ValueError(f'{v} is an invalid point.')
    return processed_vertices

@dataclass
class BoundingBox(VisionResource):
    x_min: float
    x_max: float
    y_min: float
    y_max: float

@dataclass
class CdfResourceRef(VisionResource):
    id: Optional[int] = None
    external_id: Optional[str] = None

@dataclass
class Polygon(VisionResource):
    vertices: List[Point]

    def __post_init__(self):
        self.vertices = _process_vertices(self.vertices)

@dataclass
class PolyLine(VisionResource):
    vertices: List[Point]

    def __post_init__(self):
        self.vertices = _process_vertices(self.vertices)
