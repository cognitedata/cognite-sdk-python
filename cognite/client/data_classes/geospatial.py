from typing import Any, Dict

import geopandas
from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList


class FeatureType(CogniteResource):
    """A representation of a feature type in the geospatial api.
    """

    def __init__(
        self,
        external_id: str = None,
        created_time: int = None,
        last_updated_time: int = None,
        attributes: Dict[str, Any] = None,
        search_spec: Dict[str, Any] = None,
        cognite_client=None,
        cognite_domain=None,
    ):
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.attributes = attributes
        self.search_spec = search_spec
        self._cognite_client = cognite_client
        self._cognite_domain = cognite_domain

    @classmethod
    def _load(cls, resource: Dict, cognite_client=None):
        cognite_domain = cognite_client.geospatial.get_current_cognite_domain()
        instance = cls(cognite_client=cognite_client, cognite_domain=cognite_domain)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class FeatureTypeList(CogniteResourceList):
    _RESOURCE = FeatureType
    _ASSERT_CLASSES = False


class AttributeAndSearchSpec:
    """A representation of a feature type attribute and search spec.
    """

    def __init__(
        self, attributes: Dict[str, Any] = None, search_spec: Dict[str, Any] = None,
    ):
        self.attributes = attributes
        self.search_spec = search_spec


class FeatureTypeUpdate:
    """A representation of a feature type update in the geospatial api.
    """

    def __init__(
        self, external_id: str = None, add: AttributeAndSearchSpec = None, cognite_client=None, cognite_domain=None,
    ):
        self.external_id = external_id
        self.add = add
        self._cognite_client = cognite_client
        self._cognite_domain = cognite_domain


class FeatureTypeUpdateList:
    _RESOURCE = FeatureTypeUpdate
    _ASSERT_CLASSES = False


class Feature(CogniteResource):
    """A representation of a feature in the geospatial api.
    """

    def __init__(
        self, external_id: str = None, cognite_client=None, **attributes,
    ):
        self.external_id = external_id
        for key in attributes:
            setattr(self, key, attributes[key])
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Dict, cognite_client=None):
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = utils._auxiliary.to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


def _is_geometry_type(attribute_type: str):
    return attribute_type in {"POINT"}


class FeatureList(CogniteResourceList):
    _RESOURCE = Feature
    _ASSERT_CLASSES = False

    def to_geopandas(self, geometry: str, camel_case: bool = True) -> "geopandas.GeoDataFrame":
        """Convert the instance into a geopandas GeoDataFrame.

        Args:
            geometry (str): The name of the geometry attribute
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            geopandas.GeoDataFrame: The geodataframe.
        """
        df = self.to_pandas(camel_case)
        wkt = utils._auxiliary.local_import("shapely.wkt")
        df[geometry] = df[geometry].apply(lambda g: wkt.loads(g["wkt"]))
        gpd = utils._auxiliary.local_import("geopandas")
        gdf = gpd.GeoDataFrame(df, geometry=geometry)
        return gdf

    @staticmethod
    def from_geopandas(feature_type: FeatureType, gdf: geopandas.GeoDataFrame) -> "FeatureList":
        """Convert a GeoDataFrame instance into a FeatureList.

        Args:
            feature_type (FeatureType): The feature type the features will conform to
            gdf (GeoDataFrame): the geodataframe instance to convert into features

        Returns:
            FeatureList: The list of features converted from the geodataframe rows.
        """
        features = []
        for _, row in gdf.iterrows():
            feature = Feature(external_id=row["externalId"])
            for attr in feature_type.attributes.items():
                attr_name = attr[0]
                attr_type = attr[1]["type"]
                if attr_name.startswith("_"):
                    continue
                if _is_geometry_type(attr_type):
                    setattr(feature, attr_name, {"wkt": row[attr_name].wkt})
                else:
                    setattr(feature, attr_name, row[attr_name])
            features.append(feature)
        return FeatureList(features)


class CoordinateReferenceSystem(CogniteResource):
    """A representation of a feature in the geospatial api.
    """

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
