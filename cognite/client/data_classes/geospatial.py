import dataclasses
import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, cast

from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList

if TYPE_CHECKING:
    import geopandas

    from cognite.client import CogniteClient

RESERVED_PROPERTIES = {"externalId", "dataSetId", "assetIds", "createdTime", "lastUpdatedTime"}


class FeatureType(CogniteResource):
    """A representation of a feature type in the geospatial api."""

    def __init__(
        self,
        external_id: str = None,
        data_set_id: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        properties: Dict[str, Any] = None,
        search_spec: Dict[str, Any] = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.external_id = external_id
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.properties = properties
        self.search_spec = search_spec
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[str, Dict[str, Any]], cognite_client: "CogniteClient" = None) -> "FeatureType":
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class FeatureTypeList(CogniteResourceList):
    _RESOURCE = FeatureType


class PropertyAndSearchSpec:
    """A representation of a feature type property and search spec."""

    def __init__(
        self, properties: Union[Dict[str, Any], List[str]] = None, search_spec: Union[Dict[str, Any], List[str]] = None
    ):
        self.properties = properties
        self.search_spec = search_spec


class FeatureTypeUpdate:
    """A representation of a feature type update in the geospatial api."""

    def __init__(
        self,
        external_id: str = None,
        add: PropertyAndSearchSpec = None,
        remove: PropertyAndSearchSpec = None,
        cognite_client: "CogniteClient" = None,
    ):
        self.external_id = external_id
        self.add = add if add is not None else PropertyAndSearchSpec()
        self.remove = remove if remove is not None else PropertyAndSearchSpec()
        self._cognite_client = cast("CogniteClient", cognite_client)


@dataclasses.dataclass
class Patches:
    add: Optional[Dict[str, Any]] = None
    remove: Optional[List[str]] = None


@dataclasses.dataclass
class FeatureTypePatch:
    external_id: Optional[str] = None
    property_patches: Optional[Patches] = None
    search_spec_patches: Optional[Patches] = None


class FeatureTypeUpdateList:
    _RESOURCE = FeatureTypeUpdate


class Feature(CogniteResource):
    """A representation of a feature in the geospatial api."""

    PRE_DEFINED_SNAKE_CASE_NAMES = {utils._auxiliary.to_snake_case(key) for key in RESERVED_PROPERTIES}

    def __init__(self, external_id: str = None, cognite_client: "CogniteClient" = None, **properties: Any):
        self.external_id = external_id
        for key in properties:
            setattr(self, key, properties[key])
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[str, Dict[str, Any]], cognite_client: "CogniteClient" = None) -> "Feature":
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            # Keep properties defined in Feature Type as is
            normalized_key = _to_feature_property_name(key)
            setattr(instance, normalized_key, value)
        return instance

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        def to_camel_case(key: str) -> str:
            # Keep properties defined in Feature Type as is
            if camel_case and key in self.PRE_DEFINED_SNAKE_CASE_NAMES:
                return utils._auxiliary.to_camel_case(key)
            return key

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if value is not None and not key.startswith("_")
        }


def _is_geometry_type(property_type: str) -> bool:
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


def _is_reserved_property(property_name: str) -> bool:
    return property_name.startswith("_") or property_name in RESERVED_PROPERTIES


def _to_feature_property_name(property_name: str) -> str:
    return utils._auxiliary.to_snake_case(property_name) if property_name in RESERVED_PROPERTIES else property_name


class FeatureList(CogniteResourceList):
    _RESOURCE = Feature

    def to_geopandas(self, geometry: str, camel_case: bool = True) -> "geopandas.GeoDataFrame":  # noqa: F821
        """Convert the instance into a GeoPandas GeoDataFrame.

        Args:
            geometry (str): The name of the feature type geometry property to use in the GeoDataFrame
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            geopandas.GeoDataFrame: The GeoPandas GeoDataFrame.

        Examples:

            Convert a FeatureList into a GeoPandas GeoDataFrame:

                >>> from cognite.client.data_classes.geospatial import PropertyAndSearchSpec
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> features = c.geospatial.search_features(...)
                >>> gdf = features.to_geopandas(
                ...     geometry="position",
                ...     camel_case=False
                ... )
                >>> gdf.head()
        """
        df = self.to_pandas(camel_case)
        wkt = cast(Any, utils._auxiliary.local_import("shapely.wkt"))
        df[geometry] = df[geometry].apply(lambda g: wkt.loads(g["wkt"]))
        geopandas = cast(Any, utils._auxiliary.local_import("geopandas"))
        gdf = geopandas.GeoDataFrame(df, geometry=geometry)
        return gdf

    @staticmethod
    def from_geopandas(
        feature_type: FeatureType,
        geodataframe: "geopandas.GeoDataFrame",  # noqa: F821
        external_id_column: str = "externalId",
        property_column_mapping: Dict[str, str] = None,
        data_set_id_column: str = "dataSetId",
    ) -> "FeatureList":
        """Convert a GeoDataFrame instance into a FeatureList.

        Args:
            feature_type (FeatureType): The feature type the features will conform to
            geodataframe (GeoDataFrame): the geodataframe instance to convert into features
            external_id_column: the geodataframe column to use for the feature external id
            data_set_id_column: the geodataframe column to use for the feature dataSet id
            property_column_mapping: provides a mapping from featuretype property names to geodataframe columns

        Returns:
            FeatureList: The list of features converted from the geodataframe rows.

        Examples:

            Create features from a geopandas dataframe:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature_type = ... # some feature type with 'position' and 'temperature' properties
                >>> my_geodataframe = ...  # some geodataframe with 'center_xy', 'temp' and 'id' columns
                >>> feature_list = FeatureList.from_geopandas(feature_type=my_feature_type, geodataframe=my_geodataframe,
                >>>     external_id_column="id", data_set_id_column="dataSetId",
                >>>     property_column_mapping={'position': 'center_xy', 'temperature': 'temp'})
                >>> created_features = c.geospatial.create_features(my_feature_type.external_id, feature_list)

        """
        features = []
        assert feature_type.properties
        if property_column_mapping is None:
            property_column_mapping = {prop_name: prop_name for (prop_name, _) in feature_type.properties.items()}
        for _, row in geodataframe.iterrows():
            feature = Feature(external_id=row[external_id_column], data_set_id=row.get(data_set_id_column, None))
            for prop in feature_type.properties.items():
                prop_name = prop[0]
                # skip generated columns and externalId, dataSetId columns
                if prop_name.startswith("_") or prop_name in [
                    "createdTime",
                    "lastUpdatedTime",
                    "externalId",
                    "dataSetId",
                ]:
                    continue
                prop_type = prop[1]["type"]
                prop_optional = prop[1].get("optional", False)
                column_name = property_column_mapping.get(prop_name, None)
                column_value = row.get(column_name, None)
                if column_name is None or column_value is None:
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


class FeatureAggregate(CogniteResource):
    """A result of aggregating features in geospatial api."""

    def __init__(self, cognite_client: "CogniteClient" = None):
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[str, Dict[str, Any]], cognite_client: "CogniteClient" = None) -> "FeatureAggregate":
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class FeatureAggregateList(CogniteResourceList):
    _RESOURCE = FeatureAggregate


class CoordinateReferenceSystem(CogniteResource):
    """A representation of a feature in the geospatial api."""

    def __init__(
        self, srid: int = None, wkt: str = None, proj_string: str = None, cognite_client: "CogniteClient" = None
    ):
        self.srid = srid
        self.wkt = wkt
        self.proj_string = proj_string
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(
        cls, resource: Union[str, Dict[str, Any]], cognite_client: "CogniteClient" = None
    ) -> "CoordinateReferenceSystem":
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class CoordinateReferenceSystemList(CogniteResourceList):
    _RESOURCE = CoordinateReferenceSystem


class OrderSpec:
    """An order specification with respect to an property."""

    def __init__(self, property: str, direction: str):
        self.property = property
        self.direction = direction
