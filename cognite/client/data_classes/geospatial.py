from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import convert_all_keys_to_camel_case, to_camel_case, to_snake_case

if TYPE_CHECKING:
    import geopandas

    from cognite.client import CogniteClient

RESERVED_PROPERTIES = {"externalId", "dataSetId", "assetIds", "createdTime", "lastUpdatedTime"}


class FeatureTypeCore(WriteableCogniteResource["FeatureTypeWrite"], ABC):
    """A representation of a feature type in the geospatial API.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        data_set_id (int | None): The ID of the dataset this feature type belongs to.
        properties (dict[str, Any] | None): The properties of the feature type.
        search_spec (dict[str, Any] | None): The search spec of the feature type.
    """

    def __init__(
        self,
        external_id: str | None = None,
        data_set_id: int | None = None,
        properties: dict[str, Any] | None = None,
        search_spec: dict[str, Any] | None = None,
    ) -> None:
        self.external_id = external_id
        self.data_set_id = data_set_id
        self.properties = properties
        self.search_spec = search_spec


class FeatureType(FeatureTypeCore):
    """A representation of a feature type in the geospatial API.
    This is the reading version of the FeatureType class, it is used when retrieving feature types from the api.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        data_set_id (int | None): The ID of the dataset this feature type belongs to.
        created_time (int | None): The created time of the feature type.
        last_updated_time (int | None): The last updated time of the feature type.
        properties (dict[str, Any] | None): The properties of the feature type.
        search_spec (dict[str, Any] | None): The search spec of the feature type.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str | None = None,
        data_set_id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        properties: dict[str, Any] | None = None,
        search_spec: dict[str, Any] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            data_set_id=data_set_id,
            properties=properties,
            search_spec=search_spec,
        )
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> FeatureType:
        return cls(
            external_id=resource.get("externalId"),
            data_set_id=resource.get("dataSetId"),
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
            properties=resource.get("properties"),
            search_spec=resource.get("searchSpec"),
            cognite_client=cognite_client,
        )

    def as_write(self) -> FeatureTypeWrite:
        """Returns a write version of this feature type."""
        if self.external_id is None or self.properties is None:
            raise ValueError("External ID and properties must be set to create a feature type")

        return FeatureTypeWrite(
            external_id=self.external_id,
            properties=self.properties,
            data_set_id=self.data_set_id,
            search_spec=self.search_spec,
        )


class FeatureTypeWrite(FeatureTypeCore):
    """A representation of a feature type in the geospatial API.
    This is the writing version of the FeatureType class, it is used when creating feature types in the api.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        properties (dict[str, Any]): The properties of the feature type.
        data_set_id (int | None): The ID of the dataset this feature type belongs to.
        search_spec (dict[str, Any] | None): The search spec of the feature type.
    """

    def __init__(
        self,
        external_id: str,
        properties: dict[str, Any],
        data_set_id: int | None = None,
        search_spec: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            data_set_id=data_set_id,
            properties=properties,
            search_spec=search_spec,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> FeatureTypeWrite:
        return cls(
            external_id=resource["externalId"],
            properties=resource["properties"],
            data_set_id=resource.get("dataSetId"),
            search_spec=resource.get("searchSpec"),
        )

    def as_write(self) -> FeatureTypeWrite:
        """Returns this FeatureTypeWrite instance."""
        return self


class FeatureTypeWriteList(CogniteResourceList[FeatureTypeWrite], ExternalIDTransformerMixin):
    _RESOURCE = FeatureTypeWrite


class FeatureTypeList(WriteableCogniteResourceList[FeatureTypeWrite, FeatureType]):
    _RESOURCE = FeatureType

    def as_write(self) -> FeatureTypeWriteList:
        return FeatureTypeWriteList(
            [feature_type.as_write() for feature_type in self], cognite_client=self._get_cognite_client()
        )


@dataclass
class PropertyAndSearchSpec(CogniteObject):
    """A representation of a feature type property and search spec."""

    properties: dict[str, Any] | list[str] | None = None
    search_spec: dict[str, Any] | list[str] | None = None


@dataclass
class Patches(CogniteObject):
    add: dict[str, Any] | None = None
    remove: list[str] | None = None


@dataclass
class FeatureTypePatch(CogniteObject):
    external_id: str | None = None
    property_patches: Patches | None = None
    search_spec_patches: Patches | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        item = {
            "external_id": self.external_id,
            "property_patches": self.property_patches.dump(camel_case=camel_case) if self.property_patches else None,
            "search_spec_patches": self.search_spec_patches.dump(camel_case=camel_case)
            if self.search_spec_patches
            else None,
        }
        if camel_case:
            return convert_all_keys_to_camel_case(item)
        return item

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        property_patches = (
            Patches.load(resource["propertyPatches"], cognite_client=cognite_client)
            if resource.get("propertyPatches") is not None
            else None
        )
        search_spec_patches = (
            Patches.load(resource["searchSpecPatches"], cognite_client=cognite_client)
            if resource.get("searchSpecPatches") is not None
            else None
        )
        return cls(resource.get("externalId"), property_patches, search_spec_patches)


class FeatureCore(WriteableCogniteResource["FeatureWrite"], ABC):
    """A representation of a feature in the geospatial API.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        **properties (Any): The properties of the feature.
    """

    PRE_DEFINED_SNAKE_CASE_NAMES = frozenset({to_snake_case(key) for key in RESERVED_PROPERTIES})

    def __init__(self, external_id: str | None = None, **properties: Any) -> None:
        self.external_id = external_id
        for key in properties:
            setattr(self, key, properties[key])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        def handle_case(key: str) -> str:
            # Keep properties defined in Feature Type as is
            if camel_case and key in self.PRE_DEFINED_SNAKE_CASE_NAMES:
                return to_camel_case(key)
            return key

        return {
            handle_case(key): value
            for key, value in self.__dict__.items()
            if value is not None and not key.startswith("_")
        }


T_Feature = TypeVar("T_Feature", bound=FeatureCore)


class Feature(FeatureCore):
    """A representation of a feature in the geospatial API.
    This is the reading version of the Feature class, it is used when retrieving features from the api.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        cognite_client (CogniteClient | None): The client to associate with this object.
        **properties (Any): The properties of the feature.
    """

    def __init__(
        self, external_id: str | None = None, cognite_client: CogniteClient | None = None, **properties: Any
    ) -> None:
        super().__init__(external_id=external_id, **properties)
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Feature:
        return cls(
            external_id=resource.get("externalId"),
            cognite_client=cognite_client,
            **{_to_feature_property_name(key): value for key, value in resource.items() if key != "externalId"},
        )

    def as_write(self) -> FeatureWrite:
        """Returns a write version of this feature."""
        if self.external_id is None:
            raise ValueError("External ID must be set to create a feature")

        return FeatureWrite(
            external_id=self.external_id,
            **{
                key: value
                for key, value in self.__dict__.items()
                if value is not None and not key.startswith("_") and key != "external_id"
            },
        )


class FeatureWrite(FeatureCore):
    """A representation of a feature in the geospatial API.
    This is the writing version of the Feature class, it is used when creating features in the api.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        **properties (Any): The properties of the feature.
    """

    def __init__(self, external_id: str, **properties: Any) -> None:
        super().__init__(external_id=external_id, **properties)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> FeatureWrite:
        return cls(
            external_id=resource["externalId"],
            **{_to_feature_property_name(key): value for key, value in resource.items() if key != "externalId"},
        )

    def as_write(self) -> FeatureWrite:
        """Returns this FeatureWrite instance."""
        return self


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


def _to_feature_property_name(property_name: str) -> str:
    return to_snake_case(property_name) if property_name in RESERVED_PROPERTIES else property_name


class FeatureListCore(WriteableCogniteResourceList[FeatureWrite, T_Feature], ExternalIDTransformerMixin):
    def to_geopandas(self, geometry: str, camel_case: bool = False) -> geopandas.GeoDataFrame:
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
                >>> client = CogniteClient()
                >>> features = client.geospatial.search_features(...)
                >>> gdf = features.to_geopandas(
                ...     geometry="position",
                ...     camel_case=False
                ... )
                >>> gdf.head()
        """
        df = self.to_pandas(camel_case)
        wkt = local_import("shapely.wkt")
        df[geometry] = df[geometry].apply(lambda g: wkt.loads(g["wkt"]))
        geopandas = local_import("geopandas")
        return geopandas.GeoDataFrame(df, geometry=geometry)

    @staticmethod
    def from_geopandas(
        feature_type: FeatureType,
        geodataframe: geopandas.GeoDataFrame,
        external_id_column: str = "externalId",
        property_column_mapping: dict[str, str] | None = None,
        data_set_id_column: str = "dataSetId",
    ) -> FeatureList:
        """Convert a GeoDataFrame instance into a FeatureList.

        Args:
            feature_type (FeatureType): The feature type the features will conform to
            geodataframe (geopandas.GeoDataFrame): the geodataframe instance to convert into features
            external_id_column (str): the geodataframe column to use for the feature external id
            property_column_mapping (dict[str, str] | None): provides a mapping from featuretype property names to geodataframe columns
            data_set_id_column (str): the geodataframe column to use for the feature dataSet id

        Returns:
            FeatureList: The list of features converted from the geodataframe rows.

        Examples:

            Create features from a geopandas dataframe:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> my_feature_type = ... # some feature type with 'position' and 'temperature' properties
                >>> my_geodataframe = ...  # some geodataframe with 'center_xy', 'temp' and 'id' columns
                >>> feature_list = FeatureList.from_geopandas(feature_type=my_feature_type, geodataframe=my_geodataframe,
                >>>     external_id_column="id", data_set_id_column="dataSetId",
                >>>     property_column_mapping={'position': 'center_xy', 'temperature': 'temp'})
                >>> created_features = client.geospatial.create_features(my_feature_type.external_id, feature_list)

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
                column_value = nan_to_none(column_value)
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


class FeatureWriteList(FeatureListCore[FeatureWrite]):
    _RESOURCE = FeatureWrite

    def as_write(self) -> FeatureWriteList:
        return self


class FeatureList(FeatureListCore[Feature]):
    _RESOURCE = Feature

    def as_write(self) -> FeatureWriteList:
        return FeatureWriteList([feature.as_write() for feature in self], cognite_client=self._get_cognite_client())


def nan_to_none(column_value: Any) -> Any:
    """Convert NaN value to None."""
    from pandas import isna
    from pandas.api.types import is_scalar

    return None if is_scalar(column_value) and isna(column_value) else column_value


class FeatureAggregate(CogniteResource):
    """A result of aggregating features in geospatial API."""

    def __init__(self, cognite_client: CogniteClient | None = None) -> None:
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> FeatureAggregate:
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class FeatureAggregateList(CogniteResourceList[FeatureAggregate]):
    _RESOURCE = FeatureAggregate


class CoordinateReferenceSystemCore(WriteableCogniteResource["CoordinateReferenceSystemWrite"], ABC):
    """A representation of a feature in the geospatial API.

    Args:
        srid (int | None): EPSG code, e.g., 4326. Only valid for geometry types. See https://en.wikipedia.org/wiki/Spatial_reference_system
        wkt (str | None): Well-known text of the geometry, see https://docs.geotools.org/stable/javadocs/org/opengis/referencing/doc-files/WKT.html
        proj_string (str | None): The projection specification string as described in https://proj.org/usage/quickstart.html
    """

    def __init__(
        self,
        srid: int | None = None,
        wkt: str | None = None,
        proj_string: str | None = None,
    ) -> None:
        self.srid = srid
        self.wkt = wkt
        self.proj_string = proj_string


class CoordinateReferenceSystem(CoordinateReferenceSystemCore):
    """A representation of a feature in the geospatial API.
    This is the reading version of the CoordinateReferenceSystem class, it is used when retrieving from the CDF.

    Args:
        srid (int | None): EPSG code, e.g., 4326. Only valid for geometry types. See https://en.wikipedia.org/wiki/Spatial_reference_system
        wkt (str | None): Well-known text of the geometry, see https://docs.geotools.org/stable/javadocs/org/opengis/referencing/doc-files/WKT.html
        proj_string (str | None): The projection specification string as described in https://proj.org/usage/quickstart.html
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        srid: int | None = None,
        wkt: str | None = None,
        proj_string: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            srid=srid,
            wkt=wkt,
            proj_string=proj_string,
        )
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> CoordinateReferenceSystem:
        return cls(
            srid=resource.get("srid"),
            wkt=resource.get("wkt"),
            proj_string=resource.get("projString"),
            cognite_client=cognite_client,
        )

    def as_write(self) -> CoordinateReferenceSystemWrite:
        """Returns a write version of this coordinate reference system."""
        if self.srid is None or self.wkt is None or self.proj_string is None:
            raise ValueError("SRID, WKT, and projString must be set to create a coordinate reference system")

        return CoordinateReferenceSystemWrite(
            srid=self.srid,
            wkt=self.wkt,
            proj_string=self.proj_string,
        )


class CoordinateReferenceSystemWrite(CoordinateReferenceSystemCore):
    """A representation of a feature in the geospatial API.

    Args:
        srid (int): EPSG code, e.g., 4326. Only valid for geometry types. See https://en.wikipedia.org/wiki/Spatial_reference_system
        wkt (str): Well-known text of the geometry, see https://docs.geotools.org/stable/javadocs/org/opengis/referencing/doc-files/WKT.html
        proj_string (str): The projection specification string as described in https://proj.org/usage/quickstart.html
    """

    def __init__(
        self,
        srid: int,
        wkt: str,
        proj_string: str,
    ) -> None:
        super().__init__(
            srid=srid,
            wkt=wkt,
            proj_string=proj_string,
        )

    @classmethod
    def _load(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> CoordinateReferenceSystemWrite:
        return cls(
            srid=resource["srid"],
            wkt=resource["wkt"],
            proj_string=resource["projString"],
        )

    def as_write(self) -> CoordinateReferenceSystemWrite:
        """Returns this CoordinateReferenceSystemWrite instance."""
        return self


class CoordinateReferenceSystemWriteList(CogniteResourceList[CoordinateReferenceSystemWrite]):
    _RESOURCE = CoordinateReferenceSystemWrite


class CoordinateReferenceSystemList(
    WriteableCogniteResourceList[CoordinateReferenceSystemWrite, CoordinateReferenceSystem]
):
    _RESOURCE = CoordinateReferenceSystem

    def as_write(self) -> CoordinateReferenceSystemWriteList:
        return CoordinateReferenceSystemWriteList(
            [coordinate_reference_system.as_write() for coordinate_reference_system in self],
            cognite_client=self._get_cognite_client(),
        )


class OrderSpec:
    """An order specification with respect to an property."""

    def __init__(self, property: str, direction: str) -> None:
        self.property = property
        self.direction = direction


class RasterMetadata:
    """Raster metadata"""

    def __init__(self, **properties: Any) -> None:
        for key in properties:
            setattr(self, key, properties[key])

    @classmethod
    def load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> RasterMetadata:
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class GeospatialComputeFunction(ABC):
    """A geospatial compute function"""

    @abstractmethod
    def to_json_payload(self) -> dict:
        """Convert function to json for request payload"""


class GeospatialGeometryTransformComputeFunction(GeospatialComputeFunction):
    "A stTransform geospatial compute function"

    def __init__(self, geospatial_geometry_compute_function: GeospatialComputeFunction, srid: int) -> None:
        self.geospatial_geometry_compute_function = geospatial_geometry_compute_function
        self.srid = srid

    def to_json_payload(self) -> dict:
        return {
            "stTransform": {"geometry": self.geospatial_geometry_compute_function.to_json_payload(), "srid": self.srid}
        }


class GeospatialGeometryComputeFunction(GeospatialComputeFunction, ABC):
    "A geospatial geometry compute function"


class GeospatialGeometryValueComputeFunction(GeospatialGeometryComputeFunction):
    """A geospatial geometry value compute function.
    Accepts a well-known text of the geometry prefixed with a spatial reference identifier,
    see https://docs.geotools.org/stable/javadocs/org/opengis/referencing/doc-files/WKT.html
    Args:
        ewkt (str): No description."""

    def __init__(self, ewkt: str) -> None:
        self.ewkt = ewkt

    def to_json_payload(self) -> dict:
        return {
            "ewkt": self.ewkt,
        }


class GeospatialComputedItem(CogniteResource):
    """A representation of an item computed from geospatial."""

    def __init__(self, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> None:
        self.resource = resource
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> GeospatialComputedItem:
        instance = cls(resource=resource, cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = to_snake_case(key)
            setattr(instance, snake_case_key, value)
        return instance


class GeospatialComputedItemList(CogniteResourceList[GeospatialComputedItem]):
    "A list of items computed from geospatial."

    _RESOURCE = GeospatialComputedItem


class GeospatialComputedResponse(CogniteResource):
    "The geospatial compute response."

    def __init__(
        self, computed_item_list: GeospatialComputedItemList, cognite_client: CogniteClient | None = None
    ) -> None:
        self.items = computed_item_list
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> GeospatialComputedResponse:
        item_list = GeospatialComputedItemList._load(
            cast(list[Any], resource.get("items")), cognite_client=cognite_client
        )
        return cls(item_list, cognite_client=cognite_client)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"items": self.items.dump(camel_case=camel_case)}
