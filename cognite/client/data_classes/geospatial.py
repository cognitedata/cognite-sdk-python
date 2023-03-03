import dataclasses
import json
from abc import ABC, abstractmethod

from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._auxiliary import to_snake_case

if TYPE_CHECKING:
    pass

RESERVED_PROPERTIES = {"externalId", "dataSetId", "assetIds", "createdTime", "lastUpdatedTime"}


class FeatureType(CogniteResource):
    def __init__(
        self,
        external_id=None,
        data_set_id=None,
        created_time=None,
        last_updated_time=None,
        properties=None,
        search_spec=None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.properties = properties
        self.search_spec = search_spec
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        instance = cls(cognite_client=cognite_client)
        for (key, value) in resource.items():
            snake_case_key = to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class FeatureTypeList(CogniteResourceList):
    _RESOURCE = FeatureType


class PropertyAndSearchSpec:
    def __init__(self, properties=None, search_spec=None):
        self.properties = properties
        self.search_spec = search_spec


class FeatureTypeUpdate:
    def __init__(self, external_id=None, add=None, remove=None, cognite_client=None):
        self.external_id = external_id
        self.add = add if (add is not None) else PropertyAndSearchSpec()
        self.remove = remove if (remove is not None) else PropertyAndSearchSpec()
        self._cognite_client = cast("CogniteClient", cognite_client)


@dataclasses.dataclass
class Patches:
    add: Optional[Dict[(str, Any)]] = None
    remove: Optional[List[str]] = None


@dataclasses.dataclass
class FeatureTypePatch:
    external_id: Optional[str] = None
    property_patches: Optional[Patches] = None
    search_spec_patches: Optional[Patches] = None


class FeatureTypeUpdateList:
    _RESOURCE = FeatureTypeUpdate


class Feature(CogniteResource):
    PRE_DEFINED_SNAKE_CASE_NAMES = {to_snake_case(key) for key in RESERVED_PROPERTIES}

    def __init__(self, external_id=None, cognite_client=None, **properties: Any):
        self.external_id = external_id
        for key in properties:
            setattr(self, key, properties[key])
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        instance = cls(cognite_client=cognite_client)
        for (key, value) in resource.items():
            normalized_key = _to_feature_property_name(key)
            setattr(instance, normalized_key, value)
        return instance

    def dump(self, camel_case=False):
        def to_camel_case(key: str) -> str:
            if camel_case and (key in self.PRE_DEFINED_SNAKE_CASE_NAMES):
                return utils._auxiliary.to_camel_case(key)
            return key

        return {
            to_camel_case(key): value
            for (key, value) in self.__dict__.items()
            if ((value is not None) and (not key.startswith("_")))
        }


def _is_geometry_type(property_type):
    return property_type in {
        "GEOMETRY",
        "POINT",
        "LINESTRING",
        "POLYGON",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
        "GEOMETRYZ",
        "POINTZ",
        "LINESTRINGZ",
        "POLYGONZ",
        "MULTIPOINTZ",
        "MULTILINESTRINGZ",
        "MULTIPOLYGONZ",
        "GEOMETRYCOLLECTIONZ",
        "GEOMETRYM",
        "POINTM",
        "LINESTRINGM",
        "POLYGONM",
        "MULTIPOINTM",
        "MULTILINESTRINGM",
        "MULTIPOLYGONM",
        "GEOMETRYCOLLECTIONM",
        "GEOMETRYZM",
        "POINTZM",
        "LINESTRINGZM",
        "POLYGONZM",
        "MULTIPOINTZM",
        "MULTILINESTRINGZM",
        "MULTIPOLYGONZM",
        "GEOMETRYCOLLECTIONZM",
    }


def _to_feature_property_name(property_name):
    return to_snake_case(property_name) if (property_name in RESERVED_PROPERTIES) else property_name


class FeatureList(CogniteResourceList):
    _RESOURCE = Feature

    def to_geopandas(self, geometry, camel_case=False):
        df = self.to_pandas(camel_case)
        wkt = cast(Any, utils._auxiliary.local_import("shapely.wkt"))
        df[geometry] = df[geometry].apply(lambda g: wkt.loads(g["wkt"]))
        geopandas = cast(Any, utils._auxiliary.local_import("geopandas"))
        return geopandas.GeoDataFrame(df, geometry=geometry)

    @staticmethod
    def from_geopandas(
        feature_type,
        geodataframe,
        external_id_column="externalId",
        property_column_mapping=None,
        data_set_id_column="dataSetId",
    ):
        features = []
        assert feature_type.properties
        if property_column_mapping is None:
            property_column_mapping = {prop_name: prop_name for (prop_name, _) in feature_type.properties.items()}
        for (_, row) in geodataframe.iterrows():
            feature = Feature(external_id=row[external_id_column], data_set_id=row.get(data_set_id_column, None))
            for prop in feature_type.properties.items():
                prop_name = prop[0]
                if prop_name.startswith("_") or (
                    prop_name in ["createdTime", "lastUpdatedTime", "externalId", "dataSetId"]
                ):
                    continue
                prop_type = prop[1]["type"]
                prop_optional = prop[1].get("optional", False)
                column_name = property_column_mapping.get(prop_name, None)
                column_value = row.get(column_name, None)
                column_value = nan_to_none(column_value)
                if (column_name is None) or (column_value is None):
                    if prop_optional:
                        continue
                    else:
                        raise ValueError(f"Missing value for property {prop_name}")
                prop_name = _to_feature_property_name(prop_name)
                if _is_geometry_type(prop_type):
                    setattr(feature, prop_name, {"wkt": column_value.wkt})
                else:
                    setattr(feature, prop_name, column_value)
            features.append(feature)
        return FeatureList(features)


def nan_to_none(column_value):
    from pandas import isna
    from pandas.api.types import is_scalar

    return None if (is_scalar(column_value) and isna(column_value)) else column_value


class FeatureAggregate(CogniteResource):
    def __init__(self, cognite_client=None):
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        instance = cls(cognite_client=cognite_client)
        for (key, value) in resource.items():
            snake_case_key = to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class FeatureAggregateList(CogniteResourceList):
    _RESOURCE = FeatureAggregate


class CoordinateReferenceSystem(CogniteResource):
    def __init__(self, srid=None, wkt=None, proj_string=None, cognite_client=None):
        self.srid = srid
        self.wkt = wkt
        self.proj_string = proj_string
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        instance = cls(cognite_client=cognite_client)
        for (key, value) in resource.items():
            snake_case_key = to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class CoordinateReferenceSystemList(CogniteResourceList):
    _RESOURCE = CoordinateReferenceSystem


class OrderSpec:
    def __init__(self, property, direction):
        self.property = property
        self.direction = direction


class RasterMetadata:
    def __init__(self, **properties: Any):
        for key in properties:
            setattr(self, key, properties[key])

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = cls(cognite_client=cognite_client)
        for (key, value) in resource.items():
            snake_case_key = to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class GeospatialComputeFunction(ABC):
    @abstractmethod
    def to_json_payload(self):
        ...


class GeospatialGeometryTransformComputeFunction(GeospatialComputeFunction):
    def __init__(self, geospatial_geometry_compute_function, srid):
        self.geospatial_geometry_compute_function = geospatial_geometry_compute_function
        self.srid = srid

    def to_json_payload(self):
        return {
            "stTransform": {"geometry": self.geospatial_geometry_compute_function.to_json_payload(), "srid": self.srid}
        }


class GeospatialGeometryComputeFunction(GeospatialComputeFunction, ABC):
    ...


class GeospatialGeometryValueComputeFunction(GeospatialGeometryComputeFunction):
    def __init__(self, ewkt):
        self.ewkt = ewkt

    def to_json_payload(self):
        return {"ewkt": self.ewkt}


class GeospatialComputedItem(CogniteResource):
    def __init__(self, resource, cognite_client=None):
        self.resource = resource
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        instance = cls(resource=resource, cognite_client=cognite_client)
        for (key, value) in resource.items():
            snake_case_key = to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class GeospatialComputedItemList(CogniteResourceList):
    _RESOURCE = GeospatialComputedItem


class GeospatialComputedResponse(CogniteResource):
    def __init__(self, computed_item_list, cognite_client=None):
        self.items = computed_item_list
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        item_list = GeospatialComputedItemList._load(
            cast("List[Any]", resource.get("items")), cognite_client=cognite_client
        )
        instance = cls(item_list, cognite_client=cognite_client)
        for (key, value) in resource.items():
            snake_case_key = to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance
