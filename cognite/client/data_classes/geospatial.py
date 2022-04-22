from typing import Any, Dict, List, Union

from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList


class FeatureType(CogniteResource):
    """A representation of a feature type in the geospatial api."""

    def __init__(
        self,
        external_id: str = None,
        created_time: int = None,
        last_updated_time: int = None,
        properties: Dict[str, Any] = None,
        search_spec: Dict[str, Any] = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.properties = properties
        self.search_spec = search_spec
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Dict, cognite_client=None):
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class FeatureTypeList(CogniteResourceList):
    _RESOURCE = FeatureType
    _ASSERT_CLASSES = False


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
        cognite_client=None,
    ):
        self.external_id = external_id
        self.add = add
        self.remove = remove
        self._cognite_client = cognite_client


class FeatureTypeUpdateList:
    _RESOURCE = FeatureTypeUpdate
    _ASSERT_CLASSES = False


class Feature(CogniteResource):
    """A representation of a feature in the geospatial api."""

    def __init__(self, external_id: str = None, cognite_client=None, **properties):
        self.external_id = external_id
        for key in properties:
            setattr(self, key, properties[key])
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Dict, cognite_client=None):
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


def _is_geometry_type(property_type: str):
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


def _is_reserved_property(property_name: str):
    return property_name.startswith("_") or property_name in {"externalId", "createdTime", "lastUpdatedTime"}


class FeatureList(CogniteResourceList):
    _RESOURCE = Feature
    _ASSERT_CLASSES = False

    def to_geopandas(self, geometry: str, camel_case: bool = True) -> "geopandas.GeoDataFrame":
        """Convert the instance into a geopandas GeoDataFrame.

        Args:
            geometry (str): The name of the geometry property
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            geopandas.GeoDataFrame: The geodataframe.
        """
        df = self.to_pandas(camel_case)
        wkt = utils._auxiliary.local_import("shapely.wkt")
        df[geometry] = df[geometry].apply(lambda g: wkt.loads(g["wkt"]))
        geopandas = utils._auxiliary.local_import("geopandas")
        gdf = geopandas.GeoDataFrame(df, geometry=geometry)
        return gdf

    @staticmethod
    def from_geopandas(
        feature_type: FeatureType,
        geodataframe: "geopandas.GeoDataFrame",
        external_id_column: str = "externalId",
        property_column_mapping: Dict[str, str] = None,
    ) -> "FeatureList":
        """Convert a GeoDataFrame instance into a FeatureList.

        Args:
            feature_type (FeatureType): The feature type the features will conform to
            geodataframe (GeoDataFrame): the geodataframe instance to convert into features
            external_id_column: the geodataframe column to use for the feature external id
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
                >>>     external_id_column="id", property_column_mapping={'position': 'center_xy', 'temperature': 'temp'})
                >>> created_features = c.geospatial.create_features(my_feature_type.external_id, feature_list)

        """
        features = []
        if property_column_mapping is None:
            property_column_mapping = {prop_name: prop_name for (prop_name, _) in feature_type.properties.items()}
        for _, row in geodataframe.iterrows():
            feature = Feature(external_id=row[external_id_column])
            for prop in feature_type.properties.items():
                prop_name = prop[0]
                prop_type = prop[1]["type"]
                prop_optional = prop[1].get("optional", False)
                if _is_reserved_property(prop_name):
                    continue
                column_name = property_column_mapping[prop[0]]
                column_value = row[column_name]
                if column_value is None and prop_optional:
                    continue
                if _is_geometry_type(prop_type):
                    setattr(feature, prop_name, {"wkt": column_value.wkt})
                else:
                    setattr(feature, prop_name, column_value)
            features.append(feature)
        return FeatureList(features)


class FeatureAggregate(CogniteResource):
    """A result of aggregating features in geospatial api."""

    def __init__(self, cognite_client=None):
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Dict, cognite_client=None):
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class FeatureAggregateList(CogniteResourceList):
    _RESOURCE = FeatureAggregate
    _ASSERT_CLASSES = False


class CoordinateReferenceSystem(CogniteResource):
    """A representation of a feature in the geospatial api."""

    def __init__(self, srid: int = None, wkt: str = None, proj_string: str = None, cognite_client=None):
        self.srid = srid
        self.wkt = wkt
        self.proj_string = proj_string
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Dict, cognite_client=None):
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class CoordinateReferenceSystemList(CogniteResourceList):
    _RESOURCE = CoordinateReferenceSystem
    _ASSERT_CLASSES = False


class OrderSpec:
    """An order specification with respect to an property."""

    def __init__(self, property: str, direction: str):
        self.property = property
        self.direction = direction
