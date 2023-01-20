import json as complexjson
import numbers
import urllib.parse
import warnings
from typing import Any, Dict, Generator, List, Optional, Sequence, Union, cast, overload

from requests.exceptions import ChunkedEncodingError

from cognite.client._api_client import APIClient
from cognite.client.data_classes.geospatial import (
    CoordinateReferenceSystem,
    CoordinateReferenceSystemList,
    Feature,
    FeatureAggregateList,
    FeatureList,
    FeatureType,
    FeatureTypeList,
    FeatureTypePatch,
    FeatureTypeUpdate,
    GeospatialComputedResponse,
    GeospatialComputeFunction,
    OrderSpec,
    RasterMetadata,
)
from cognite.client.exceptions import CogniteConnectionError
from cognite.client.utils._identifier import IdentifierSequence


class GeospatialAPI(APIClient):
    _RESOURCE_PATH = "/geospatial"

    @staticmethod
    def _feature_resource_path(feature_type_external_id: str) -> str:
        return f"{GeospatialAPI._RESOURCE_PATH}/featuretypes/{feature_type_external_id}/features"

    @staticmethod
    def _raster_resource_path(
        feature_type_external_id: str, feature_external_id: str, raster_property_name: str
    ) -> str:
        encoded_feature_external_id = urllib.parse.quote(feature_external_id, safe="")
        encoded_raster_property_name = urllib.parse.quote(raster_property_name, safe="")
        return (
            GeospatialAPI._feature_resource_path(feature_type_external_id)
            + f"/{encoded_feature_external_id}/rasters/{encoded_raster_property_name}"
        )

    @staticmethod
    def _compute_path() -> str:
        return f"{GeospatialAPI._RESOURCE_PATH}/compute"

    @overload
    def create_feature_types(self, feature_type: FeatureType) -> FeatureType:
        ...

    @overload
    def create_feature_types(self, feature_type: Sequence[FeatureType]) -> FeatureTypeList:
        ...

    def create_feature_types(
        self, feature_type: Union[FeatureType, Sequence[FeatureType]]
    ) -> Union[FeatureType, FeatureTypeList]:
        """`Creates feature types`
        <https://docs.cognite.com/api/v1/#operation/createFeatureTypes>

        Args:
            feature_type (Union[FeatureType, Sequence[FeatureType]]): feature type definition or list of feature type definitions to create.

        Returns:
            Union[FeatureType, FeatureTypeList]: Created feature type definition(s)

        Examples:

            Create new type definitions:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.geospatial import FeatureType
                >>> c = CogniteClient()
                >>> feature_types = [
                ...     FeatureType(external_id="wells", properties={"location": {"type": "POINT", "srid": 4326}})
                ...     FeatureType(
                ...       external_id="cities",
                ...       properties={"name": {"type": "STRING", "size": 10}},
                ...       search_spec={"name_index": {"properties": ["name"]}}
                ...     )
                ... ]
                >>> res = c.geospatial.create_feature_types(feature_types)
        """
        return self._create_multiple(
            list_cls=FeatureTypeList,
            resource_cls=FeatureType,
            items=feature_type,
            resource_path=f"{self._RESOURCE_PATH}/featuretypes",
        )

    def delete_feature_types(self, external_id: Union[str, Sequence[str]], recursive: bool = False) -> None:
        """`Delete one or more feature type`
        <https://docs.cognite.com/api/v1/#operation/GeospatialDeleteFeatureTypes>

        Args:
            external_id (Union[str, Sequence[str]]): External ID or list of external ids
            recursive (bool): if `true` the features will also be dropped

        Returns:
            None

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.geospatial.delete_feature_types(external_id=["wells", "cities"])
        """
        extra_body_fields = {"recursive": True} if recursive else {}
        return self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
            resource_path=f"{self._RESOURCE_PATH}/featuretypes",
            extra_body_fields=extra_body_fields,
        )

    def list_feature_types(self) -> FeatureTypeList:
        """`List feature types`
        <https://docs.cognite.com/api/v1/#operation/listFeatureTypes>

        Returns:
            FeatureTypeList: List of feature types

        Examples:

            Iterate over feature type definitions:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for feature_type in c.geospatial.list_feature_types():
                ...     feature_type # do something with the feature type definition
        """
        return self._list(
            list_cls=FeatureTypeList,
            resource_cls=FeatureType,
            method="POST",
            resource_path=f"{self._RESOURCE_PATH}/featuretypes",
        )

    @overload
    def retrieve_feature_types(self, external_id: str) -> FeatureType:
        ...

    @overload
    def retrieve_feature_types(self, external_id: List[str]) -> FeatureTypeList:
        ...

    def retrieve_feature_types(self, external_id: Union[str, List[str]]) -> Union[FeatureType, FeatureTypeList]:
        """`Retrieve feature types`
        <https://docs.cognite.com/api/v1/#operation/getFeatureTypesByIds>

        Args:
            external_id (Union[str, List[str]]): External ID

        Returns:
            FeatureTypeList: Requested Type or None if it does not exist.

        Examples:

            Get Type by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.geospatial.retrieve_feature_types(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_id)
        return self._retrieve_multiple(
            list_cls=FeatureTypeList,
            resource_cls=FeatureType,
            identifiers=identifiers.as_singleton() if identifiers.is_singleton() else identifiers,
            resource_path=f"{self._RESOURCE_PATH}/featuretypes",
        )

    def update_feature_types(self, update: Union[FeatureTypeUpdate, Sequence[FeatureTypeUpdate]]) -> FeatureTypeList:
        """`Update feature types (Deprecated)`
        <https://docs.cognite.com/api/v1/#operation/updateFeatureTypes>

        Args:
            update (Union[FeatureTypeUpdate, Sequence[FeatureTypeUpdate]]): the update to apply

        Returns:
            FeatureTypeList: The updated feature types.

        Examples:

            Add one property and one index to a feature type:

                >>> from cognite.client.data_classes.geospatial import PropertyAndSearchSpec
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.geospatial.update_feature_types(
                ...     update=FeatureTypeUpdate(external_id="wells",
                ...         add=PropertyAndSearchSpec(
                ...             properties={"altitude": {"type": "DOUBLE"}},
                ...             search_spec={"altitude_idx": {"properties": ["altitude"]}}
                ...         )
                ...     )
                ... )

            Remove one property and one index from a feature type:
                >>> from cognite.client.data_classes.geospatial import PropertyAndSearchSpec
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.geospatial.update_feature_types(
                ...     update=FeatureTypeUpdate(external_id="wells",
                ...         remove=PropertyAndSearchSpec(
                ...             properties=["volume"],
                ...             search_spec=["vol_press_idx"]
                ...         )
                ...     )
                ... )
        """
        warnings.warn("update_feature_types is deprecated, use patch_feature_types instead.", DeprecationWarning)
        if isinstance(update, FeatureTypeUpdate):
            update = [update]

        def mapper(it: FeatureTypeUpdate) -> Dict[str, Any]:
            add_properties = it.add.properties if hasattr(it, "add") else None
            remove_properties = it.remove.properties if hasattr(it, "remove") else None
            add_search_spec = it.add.search_spec if hasattr(it, "add") else None
            remove_search_spec = it.remove.search_spec if hasattr(it, "remove") else None
            properties_update = {"add": add_properties, "remove": remove_properties}
            search_spec_update = {"add": add_search_spec, "remove": remove_search_spec}
            return {"properties": properties_update, "searchSpec": search_spec_update}

        json = {"items": [{"externalId": it.external_id, "update": mapper(it)} for it in update]}
        res = self._post(url_path=f"{self._RESOURCE_PATH}/featuretypes/update", json=json)
        return FeatureTypeList._load(res.json()["items"], cognite_client=self._cognite_client)

    def patch_feature_types(self, patch: Union[FeatureTypePatch, Sequence[FeatureTypePatch]]) -> FeatureTypeList:
        """`Patch feature types`
        <https://docs.cognite.com/api/v1/#operation/updateFeatureTypes>

        Args:
            patch (Union[FeatureTypePatch, Sequence[FeatureTypePatch]]): the patch to apply

        Returns:
            FeatureTypeList: The patched feature types.

        Examples:

            Add one property to a feature type and add indexes

                >>> from cognite.client.data_classes.geospatial import Patches
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.geospatial.patch_feature_types(
                ...    patch=FeatureTypePatch(
                ...       external_id="wells",
                ...       property_patches=Patches(add={"altitude": {"type": "DOUBLE"}}),
                ...       search_spec_patches=Patches(
                ...         add={
                ...           "altitude_idx": {"properties": ["altitude"]},
                ...           "composite_idx": {"properties": ["location", "altitude"]}
                ...         }
                ...       )
                ...    )
                ... )

            Add an additional index to an existing property

                >>> from cognite.client.data_classes.geospatial import Patches
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.geospatial.patch_feature_types(
                ...    patch=FeatureTypePatch(
                ...         external_id="wells",
                ...         search_spec_patches=Patches(add={"location_idx": {"properties": ["location"]}})
                ... ))

        """
        if isinstance(patch, FeatureTypePatch):
            patch = [patch]
        json = {
            "items": [
                {
                    "externalId": it.external_id,
                    "update": {"properties": it.property_patches, "searchSpec": it.search_spec_patches},
                }
                for it in patch
            ]
        }
        res = self._post(url_path=f"{self._RESOURCE_PATH}/featuretypes/update", json=json)
        return FeatureTypeList._load(res.json()["items"], cognite_client=self._cognite_client)

    @overload
    def create_features(
        self,
        feature_type_external_id: str,
        feature: Feature,
        allow_crs_transformation: bool = False,
        chunk_size: int = None,
    ) -> Feature:
        ...

    @overload
    def create_features(
        self,
        feature_type_external_id: str,
        feature: Union[Sequence[Feature], FeatureList],
        allow_crs_transformation: bool = False,
        chunk_size: int = None,
    ) -> FeatureList:
        ...

    def create_features(
        self,
        feature_type_external_id: str,
        feature: Union[Feature, Sequence[Feature], FeatureList],
        allow_crs_transformation: bool = False,
        chunk_size: int = None,
    ) -> Union[Feature, FeatureList]:
        """`Creates features`
        <https://docs.cognite.com/api/v1/#operation/createFeatures>

        Args:
            feature_type_external_id: Feature type definition for the features to create.
            feature: one feature or a list of features to create or a FeatureList object
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference
                System defined in the feature type specification. When it is false, then requests with geometries in
                Coordinate Reference System different from the ones defined in the feature type will result in
                CogniteAPIError exception.
            chunk_size: maximum number of items in a single request to the api

        Returns:
            Union[Feature, FeatureList]: Created features

        Examples:

            Create a new feature type and corresponding feature:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> feature_types = [
                ...     FeatureType(
                ...         external_id="my_feature_type",
                ...         properties={
                ...             "location": {"type": "POINT", "srid": 4326},
                ...             "temperature": {"type": "DOUBLE"}
                ...         }
                ...     )
                ... ]
                >>> res = c.geospatial.create_feature_types(feature_types)
                >>> res = c.geospatial.create_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(
                ...         external_id="my_feature",
                ...         location={"wkt": "POINT(1 1)"},
                ...         temperature=12.4
                ...     )
                ... )
        """
        if chunk_size is not None and (chunk_size < 1 or chunk_size > self._CREATE_LIMIT):
            raise ValueError(f"The chunk_size must be strictly positive and not exceed {self._CREATE_LIMIT}")
        if isinstance(feature, FeatureList):
            feature = list(feature)
        resource_path = self._feature_resource_path(feature_type_external_id)
        extra_body_fields = {"allowCrsTransformation": "true"} if allow_crs_transformation else {}
        return self._create_multiple(
            list_cls=FeatureList,
            resource_cls=Feature,
            items=feature,
            resource_path=resource_path,
            extra_body_fields=extra_body_fields,
            limit=chunk_size,
        )

    def delete_features(self, feature_type_external_id: str, external_id: Union[str, Sequence[str]] = None) -> None:
        """`Delete one or more feature`
        <https://docs.cognite.com/api/v1/#operation/deleteFeatures>

        Args:
            feature_type_external_id : Feature type external id for the features to delete.
            external_id (Union[str, Sequence[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.geospatial.delete_features(
                ...     feature_type_external_id="my_feature_type",
                ...     external_id=my_feature
                ... )
        """
        resource_path = self._feature_resource_path(feature_type_external_id)
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id), resource_path=resource_path, wrap_ids=True
        )

    @overload
    def retrieve_features(
        self,
        feature_type_external_id: str,
        external_id: str,
        properties: Dict[str, Any] = None,
    ) -> Feature:
        ...

    @overload
    def retrieve_features(
        self,
        feature_type_external_id: str,
        external_id: List[str],
        properties: Dict[str, Any] = None,
    ) -> FeatureList:
        ...

    def retrieve_features(
        self,
        feature_type_external_id: str,
        external_id: Union[str, List[str]],
        properties: Dict[str, Any] = None,
    ) -> Union[FeatureList, Feature]:
        """`Retrieve features`
        <https://docs.cognite.com/api/v1/#operation/getFeaturesByIds>

        Args:
            feature_type_external_id : Feature type external id for the features to retrieve.
            external_id (Union[str, List[str]]): External ID or list of external ids
            properties (Dict[str, Any]): the output property selection

        Returns:
            FeatureList: Requested features or None if it does not exist.

        Examples:

            Retrieve one feature by its external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.geospatial.retrieve_features(
                ...     feature_type_external_id="my_feature_type",
                ...     external_id="my_feature"
                ... )
        """
        resource_path = self._feature_resource_path(feature_type_external_id)
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_id)
        return self._retrieve_multiple(
            list_cls=FeatureList,
            resource_cls=Feature,
            identifiers=identifiers.as_singleton() if identifiers.is_singleton() else identifiers,
            resource_path=resource_path,
            other_params={"output": {"properties": properties}},
        )

    def update_features(
        self,
        feature_type_external_id: str,
        feature: Union[Feature, Sequence[Feature]],
        allow_crs_transformation: bool = False,
        chunk_size: int = None,
    ) -> FeatureList:
        """`Update features`
        <https://docs.cognite.com/api/v1/#operation/updateFeatures>

        Args:
            feature_type_external_id : Feature type definition for the features to update.
            feature (Union[Feature, Sequence[Feature]]): feature or list of features.
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference
                System defined in the feature type specification. When it is false, then requests with geometries in
                Coordinate Reference System different from the ones defined in the feature type will result in
                CogniteAPIError exception.
            chunk_size: maximum number of items in a single request to the api

        Returns:
            FeatureList: Updated features

        Examples:

            Update one feature:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature = c.geospatial.create_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(external_id="my_feature", temperature=12.4)
                ... )
                >>> my_updated_feature = c.geospatial.update_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(external_id="my_feature", temperature=6.237)
                ... )
        """
        if chunk_size is not None and (chunk_size < 1 or chunk_size > self._UPDATE_LIMIT):
            raise ValueError(f"The chunk_size must be strictly positive and not exceed {self._UPDATE_LIMIT}")
        if isinstance(feature, FeatureList):
            feature = list(feature)
        # updates for feature are not following the patch structure from other resources
        # they are more like a replace so an update looks like a feature creation
        resource_path = self._feature_resource_path(feature_type_external_id) + "/update"
        extra_body_fields = {"allowCrsTransformation": "true"} if allow_crs_transformation else {}
        return cast(
            FeatureList,
            self._create_multiple(
                list_cls=FeatureList,
                resource_cls=Feature,
                items=feature,
                resource_path=resource_path,
                extra_body_fields=extra_body_fields,
                limit=chunk_size,
            ),
        )

    def list_features(
        self,
        feature_type_external_id: str,
        filter: Optional[Dict[str, Any]] = None,
        properties: Dict[str, Any] = None,
        limit: int = 100,
        allow_crs_transformation: bool = False,
    ) -> FeatureList:
        """`List features`
        <https://docs.cognite.com/api/v1/#operation/listFeatures>

        This method allows to filter all features.

        Args:
            feature_type_external_id: the feature type to list features for
            filter (Dict[str, Any]): the list filter
            limit (int, optional): Maximum number of features to return. Defaults to 25. Set to -1, float("inf") or None
                to return all features.
            properties (Dict[str, Any]): the output property selection
            allow_crs_transformation: If true, then input geometries if existing in the filter will be transformed into
                the Coordinate Reference System defined in the feature type specification. When it is false, then
                requests with geometries in Coordinate Reference System different from the ones defined in the feature
                type will result in CogniteAPIError exception.

        Returns:
            FeatureList: The filtered features

        Examples:

            List features:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature_type = c.geospatial.retrieve_feature_types(
                ...     external_id="my_feature_type"
                ... )
                >>> my_feature = c.geospatial.create_features(
                ...     feature_type_external_id=my_feature_type,
                ...     feature=Feature(
                ...         external_id="my_feature",
                ...         temperature=12.4,
                ...         location={"wkt": "POINT(0 1)"}
                ...     )
                ... )
                >>> res = c.geospatial.list_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={"range": {"property": "temperature", "gt": 12.0}}
                ... )
                >>> for f in res:
                ...     # do something with the features

            Search for features and select output properties:

                >>> res = c.geospatial.list_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={},
                ...     properties={"temperature": {}, "pressure": {}}
                ... )

            Search for features with spatial filters:

                >>> res = c.geospatial.list_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={"stWithin": {
                ...         "property": "location",
                ...         "value": {"wkt": "POLYGON((0 0, 0 1, 1 1, 0 0))"}
                ...     }}
                ... )
        """
        return self._list(
            list_cls=FeatureList,
            resource_cls=Feature,
            resource_path=self._feature_resource_path(feature_type_external_id),
            method="POST",
            limit=limit,
            filter=filter,
            other_params={
                "allowCrsTransformation": (True if allow_crs_transformation else None),
                "output": {"properties": properties},
            },
        )

    def search_features(
        self,
        feature_type_external_id: str,
        filter: Optional[Dict[str, Any]] = None,
        properties: Dict[str, Any] = None,
        limit: int = 100,
        order_by: Sequence[OrderSpec] = None,
        allow_crs_transformation: bool = False,
    ) -> FeatureList:
        """`Search for features`
        <https://docs.cognite.com/api/v1/#operation/searchFeatures>

        This method allows to order the result by one or more of the properties of the feature type.
        However, the number of items returned is  limited to 1000 and there is no support for cursors yet.
        If you need to return more than 1000 items, use the `stream_features(...)` method instead.

        Args:
            feature_type_external_id: the feature type to search for
            filter (Dict[str, Any]): the search filter
            limit (int): maximum number of results
            properties (Dict[str, Any]): the output property selection
            order_by (Sequence[OrderSpec]): the order specification
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference
                System defined in the feature type specification. When it is false, then requests with geometries in
                Coordinate Reference System different from the ones defined in the feature type will result in
                CogniteAPIError exception.

        Returns:
            FeatureList: the filtered features

        Examples:

            Search for features:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature_type = c.geospatial.retrieve_feature_types(
                ...     external_id="my_feature_type"
                ... )
                >>> my_feature = c.geospatial.create_features(
                ...     feature_type_external_id=my_feature_type,
                ...     feature=Feature(
                ...         external_id="my_feature",
                ...         temperature=12.4,
                ...         location={"wkt": "POINT(0 1)"}
                ...     )
                ... )
                >>> res = c.geospatial.search_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={"range": {"property": "temperature", "gt": 12.0}}
                ... )
                >>> for f in res:
                ...     # do something with the features

            Search for features and select output properties:

                >>> res = c.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={},
                ...     properties={"temperature": {}, "pressure": {}}
                ... )

            Search for features and order results:

                >>> res = c.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={},
                ...     order_by=[
                ...         OrderSpec("temperature", "ASC"),
                ...         OrderSpec("pressure", "DESC")]
                ... )

            Search for features with spatial filters:

                >>> res = c.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={"stWithin": {
                ...         "property": "location",
                ...         "value": {"wkt": "POLYGON((0 0, 0 1, 1 1, 0 0))"}
                ...     }}
                ... )

            Combining multiple filters:

                >>> res = c.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={"and": [
                ...         {"range": {"property": "temperature", "gt": 12.0}},
                ...         {"stWithin": {
                ...             "property": "location",
                ...             "value": {"wkt": "POLYGON((0 0, 0 1, 1 1, 0 0))"}
                ...         }}
                ...     ]}
                ... )

                >>> res = c.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={"or": [
                ...         {"range": {"property": "temperature", "gt": 12.0}},
                ...         {"stWithin": {
                ...             "property": "location",
                ...             "value": {"wkt": "POLYGON((0 0, 0 1, 1 1, 0 0))"}
                ...         }}
                ...     ]}
                ... )
        """
        resource_path = self._feature_resource_path(feature_type_external_id) + "/search"
        cls = FeatureList
        order = None if order_by is None else [f"{item.property}:{item.direction}" for item in order_by]
        res = self._post(
            url_path=resource_path,
            json={
                "filter": filter or {},
                "limit": limit,
                "output": {"properties": properties},
                "sort": order,
                "allowCrsTransformation": (True if allow_crs_transformation else None),
            },
        )
        return cls._load(res.json()["items"], cognite_client=self._cognite_client)

    def stream_features(
        self,
        feature_type_external_id: str,
        filter: Optional[Dict[str, Any]] = None,
        properties: Dict[str, Any] = None,
        allow_crs_transformation: bool = False,
    ) -> Generator[Feature, None, None]:
        """`Stream features`
        <https://docs.cognite.com/api/v1/#operation/searchFeaturesStreaming>

        This method allows to return any number of items until the underlying
        api calls times out. The order of the result items is not deterministic.
        If you need to order the results, use the `search_features(...)` method instead.

        Args:
            feature_type_external_id: the feature type to search for
            filter (Dict[str, Any]): the search filter
            properties (Dict[str, Any]): the output property selection
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference
                System defined in the feature type specification. When it is false, then requests with geometries in
                Coordinate Reference System different from the ones defined in the feature type will result in
                CogniteAPIError exception.

        Returns:
            Generator[Feature]: a generator for the filtered features

        Examples:

            Stream features:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature = c.geospatial.create_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(external_id="my_feature", temperature=12.4)
                ... )
                >>> features = c.geospatial.stream_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={"range": {"property": "temperature", "gt": 12.0}}
                ... )
                >>> for f in features:
                ...     # do something with the features

            Stream features and select output properties:

                >>> features = c.geospatial.stream_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={},
                ...     properties={"temperature": {}, "pressure": {}}
                ... )
                >>> for f in features:
                ...     # do something with the features

        """
        resource_path = self._feature_resource_path(feature_type_external_id) + "/search-streaming"
        json = {"filter": filter or {}, "output": {"properties": properties, "jsonStreamFormat": "NEW_LINE_DELIMITED"}}
        params = {"allowCrsTransformation": "true"} if allow_crs_transformation else None
        res = self._do_request(
            "POST", url_path=resource_path, json=json, timeout=self._config.timeout, stream=True, params=params
        )

        try:
            for line in res.iter_lines():
                yield Feature._load(complexjson.loads(line))
        except (ChunkedEncodingError, ConnectionError) as e:
            raise CogniteConnectionError(e)

    def aggregate_features(
        self,
        feature_type_external_id: str,
        property: str = None,
        aggregates: Sequence[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        group_by: Sequence[str] = None,
        order_by: Sequence[OrderSpec] = None,
        output: Dict[str, Any] = None,
    ) -> FeatureAggregateList:
        """`Aggregate filtered features`
        <https://docs.cognite.com/api/v1/#operation/aggregateFeatures>

        Args:
            feature_type_external_id: the feature type to filter features from
            filter (Dict[str, Any]): the search filter
            property (str): the property for which aggregates should be calculated
            aggregates (Sequence[str]): list of aggregates to be calculated
            group_by (Sequence[str]): list of properties to group by with
            order_by (Sequence[OrderSpec]): the order specification
            output (Dict[str, Any]): the aggregate output

        Returns:
            FeatureAggregateList: the filtered features

        Examples:

            Aggregate property of features:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature = c.geospatial.create_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(external_id="my_feature", temperature=12.4)
                ... )
                >>> res_deprecated = c.geospatial.aggregate_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={"range": {"property": "temperature", "gt": 12.0}},
                ...     property="temperature",
                ...     aggregates=["max", "min"],
                ...     group_by=["category"],
                ...     order_by=[OrderSpec("category", "ASC")]
                ... ) # deprecated
                >>> res = c.geospatial.aggregate_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={"range": {"property": "temperature", "gt": 12.0}},
                ...     group_by=["category"],
                ...     order_by=[OrderSpec("category", "ASC")],
                ...     output={"min_temperature": {"min": {"property": "temperature"}},
                ...         "max_volume": {"max": {"property": "volume"}}
                ...     }
                ... )
                >>> for a in res:
                ...     # loop over aggregates in different groups
        """
        if property or aggregates:
            warnings.warn("property and aggregates are deprecated, use output instead.", DeprecationWarning)
        resource_path = self._feature_resource_path(feature_type_external_id) + "/aggregate"
        cls = FeatureAggregateList
        order = None if order_by is None else [f"{item.property}:{item.direction}" for item in order_by]
        res = self._post(
            url_path=resource_path,
            json={
                "filter": filter or {},
                "property": property,
                "aggregates": aggregates,
                "groupBy": group_by,
                "sort": order,
                "output": output,
            },
        )
        return cls._load(res.json()["items"], cognite_client=self._cognite_client)

    def get_coordinate_reference_systems(self, srids: Union[int, Sequence[int]]) -> CoordinateReferenceSystemList:
        """`Get Coordinate Reference Systems`
        <https://docs.cognite.com/api/v1/#operation/getCoordinateReferenceSystem>

        Args:
            srids: (Union[int, Sequence[int]]): SRID or list of SRIDs

        Returns:
            CoordinateReferenceSystemList: Requested CRSs.

        Examples:

            Get two CRS definitions:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> crs = c.geospatial.get_coordinate_reference_systems(srids=[4326, 4327])
        """
        if isinstance(srids, (int, numbers.Integral)):
            srids_processed: Sequence[Union[numbers.Integral, int]] = [srids]
        else:
            srids_processed = srids

        res = self._post(
            url_path=f"{self._RESOURCE_PATH}/crs/byids", json={"items": [{"srid": srid} for srid in srids_processed]}
        )
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def list_coordinate_reference_systems(self, only_custom: bool = False) -> CoordinateReferenceSystemList:
        """`List Coordinate Reference Systems`
        <https://docs.cognite.com/api/v1/#operation/listGeospatialCoordinateReferenceSystems>

        Args:
            only_custom (bool): list only custom CRSs or not

        Returns:
            CoordinateReferenceSystemList: list of CRSs.

        Examples:

            Fetch all custom CRSs:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> crs = c.geospatial.list_coordinate_reference_systems(only_custom=True)
        """
        res = self._get(url_path=f"{self._RESOURCE_PATH}/crs", params={"filterCustom": only_custom})
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def create_coordinate_reference_systems(
        self, crs: Union[CoordinateReferenceSystem, Sequence[CoordinateReferenceSystem]]
    ) -> CoordinateReferenceSystemList:
        """`Create Coordinate Reference System`
        <https://docs.cognite.com/api/v1/#operation/createGeospatialCoordinateReferenceSystems>

        Args:
            crs: a CoordinateReferenceSystem or a list of CoordinateReferenceSystem

        Returns:
            CoordinateReferenceSystemList: list of CRSs.

        Examples:

            Create a custom CRS:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> custom_crs = CoordinateReferenceSystem(
                ...     srid = 121111,
                ...     wkt=(
                ...          'PROJCS["NTF (Paris) / Lambert zone II",'
                ...          ' GEOGCS["NTF (Paris)",'
                ...          '  DATUM["Nouvelle_Triangulation_Francaise_Paris",'
                ...          '   SPHEROID["Clarke 1880 (IGN)",6378249.2,293.4660212936265,'
                ...          '    AUTHORITY["EPSG","7011"]],'
                ...          '   TOWGS84[-168,-60,320,0,0,0,0],'
                ...          '   AUTHORITY["EPSG","6807"]],'
                ...          '  PRIMEM["Paris",2.33722917,'
                ...          '   AUTHORITY["EPSG","8903"]],'
                ...          '  UNIT["grad",0.01570796326794897,'
                ...          '   AUTHORITY["EPSG","9105"]], '
                ...          '  AUTHORITY["EPSG","4807"]],'
                ...          ' PROJECTION["Lambert_Conformal_Conic_1SP"],'
                ...          ' PARAMETER["latitude_of_origin",52],'
                ...          ' PARAMETER["central_meridian",0],'
                ...          ' PARAMETER["scale_factor",0.99987742],'
                ...          ' PARAMETER["false_easting",600000],'
                ...          ' PARAMETER["false_northing",2200000],'
                ...          ' UNIT["metre",1,'
                ...          '  AUTHORITY["EPSG","9001"]],'
                ...          ' AXIS["X",EAST],'
                ...          ' AXIS["Y",NORTH],'
                ...          ' AUTHORITY["EPSG","27572"]]'
                ...     ),
                ...     proj_string=(
                ...          '+proj=lcc +lat_1=46.8 +lat_0=46.8 +lon_0=0 +k_0=0.99987742 '
                ...          '+x_0=600000 +y_0=2200000 +a=6378249.2 +b=6356515 '
                ...          '+towgs84=-168,-60,320,0,0,0,0 +pm=paris +units=m +no_defs'
                ...     )
                ... )
                >>> crs = c.geospatial.create_coordinate_reference_systems(custom_crs)
        """
        if isinstance(crs, CoordinateReferenceSystem):
            crs = [crs]

        res = self._post(
            url_path=f"{self._RESOURCE_PATH}/crs", json={"items": [it.dump(camel_case=True) for it in crs]}
        )
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def delete_coordinate_reference_systems(self, srids: Union[int, Sequence[int]]) -> None:
        """`Delete Coordinate Reference System`
        <https://docs.cognite.com/api/v1/#operation/deleteGeospatialCoordinateReferenceSystems>

        Args:
            srids: (Union[int, Sequence[int]]): SRID or list of SRIDs

        Returns:
            None

        Examples:

            Delete a custom CRS:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> crs = c.geospatial.delete_coordinate_reference_systems(srids=[121111])
        """
        if isinstance(srids, (int, numbers.Integral)):
            srids_processed: Sequence[Union[numbers.Integral, int]] = [srids]
        else:
            srids_processed = srids

        self._post(
            url_path=f"{self._RESOURCE_PATH}/crs/delete", json={"items": [{"srid": srid} for srid in srids_processed]}
        )

    def put_raster(
        self,
        feature_type_external_id: str,
        feature_external_id: str,
        raster_property_name: str,
        raster_format: str,
        raster_srid: int,
        file: str,
        allow_crs_transformation: bool = False,
        raster_scale_x: Optional[float] = None,
        raster_scale_y: Optional[float] = None,
    ) -> RasterMetadata:
        """`Put raster`
        <https://docs.cognite.com/api/v1/#tag/Geospatial/operation/putRaster>

        Args:
            feature_type_external_id : Feature type definition for the features to create.
            feature_external_id: one feature or a list of features to create
            raster_property_name: the raster property name
            raster_format: the raster input format
            raster_srid: the associated SRID for the raster
            file: the path to the file of the raster
            allow_crs_transformation: When the parameter is false, requests with rasters in Coordinate Reference
                System different from the one defined in the feature type will result in bad request response code.
            raster_scale_x: the X component of the pixel width in units of coordinate reference system
            raster_scale_y: the Y component of the pixel height in units of coordinate reference system

        Returns:
            RasterMetadata: the raster metadata if it was ingested succesfully

        Examples:

            Put a raster in a feature raster property:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> feature_type = ...
                >>> feature = ...
                >>> raster_property_name = ...
                >>> metadata = c.geospatial.put_raster(feature_type.external_id, feature.external_id,
                ...         raster_property_name, "XYZ", 3857, file)
        """
        query_params = f"format={raster_format}&srid={raster_srid}"
        if allow_crs_transformation:
            query_params += "&allowCrsTransformation=true"
        if raster_scale_x:
            query_params += f"&scaleX={raster_scale_x}"
        if raster_scale_y:
            query_params += f"&scaleY={raster_scale_y}"
        url_path = (
            self._raster_resource_path(feature_type_external_id, feature_external_id, raster_property_name)
            + f"?{query_params}"
        )
        res = self._do_request(
            "PUT",
            url_path,
            data=open(file, "rb").read(),
            headers={"Content-Type": "application/binary"},
            timeout=self._config.timeout,
        )
        return RasterMetadata._load(res.json(), cognite_client=self._cognite_client)

    def delete_raster(
        self,
        feature_type_external_id: str,
        feature_external_id: str,
        raster_property_name: str,
    ) -> None:
        """`Delete raster`
        <https://docs.cognite.com/api/v1/#tag/Geospatial/operation/deleteRaster>

        Args:
            feature_type_external_id : Feature type definition for the features to create.
            feature_external_id: one feature or a list of features to create
            raster_property_name: the raster property name

        Returns:
            None

        Examples:

            Delete a raster in a feature raster property:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> feature_type = ...
                >>> feature = ...
                >>> raster_property_name = ...
                >>> c.geospatial.delete_raster(feature_type.external_id, feature.external_id, raster_property_name)
        """
        url_path = (
            self._raster_resource_path(feature_type_external_id, feature_external_id, raster_property_name) + "/delete"
        )
        self._do_request(
            "POST",
            url_path,
            timeout=self._config.timeout,
        )

    def get_raster(
        self,
        feature_type_external_id: str,
        feature_external_id: str,
        raster_property_name: str,
        raster_format: str,
        raster_options: Dict[str, Any] = None,
        raster_srid: Optional[int] = None,
        raster_scale_x: Optional[float] = None,
        raster_scale_y: Optional[float] = None,
        allow_crs_transformation: bool = False,
    ) -> bytes:
        """`Get raster`
        <https://docs.cognite.com/api/v1/#tag/Geospatial/operation/getRaster>

        Args:
            feature_type_external_id : Feature type definition for the features to create.
            feature_external_id: one feature or a list of features to create
            raster_property_name: the raster property name
            raster_format: the raster output format
            raster_options: GDAL raster creation key-value options
            raster_srid: the SRID for the output raster
            raster_scale_x: the X component of the output pixel width in units of coordinate reference system
            raster_scale_y: the Y component of the output pixel height in units of coordinate reference system
            allow_crs_transformation: When the parameter is false, requests with output rasters in Coordinate Reference
                System different from the one defined in the feature type will result in bad request response code.

        Returns:
            bytes: the raster data

        Examples:

            Get a raster from a feature raster property:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> feature_type = ...
                >>> feature = ...
                >>> raster_property_name = ...
                >>> raster_data = c.geospatial.get_raster(feature_type.external_id, feature.external_id,
                ...    raster_property_name, "XYZ", {"SIGNIFICANT_DIGITS": "4"})
        """
        url_path = self._raster_resource_path(feature_type_external_id, feature_external_id, raster_property_name)
        res = self._do_request(
            "POST",
            url_path,
            timeout=self._config.timeout,
            json={
                "format": raster_format,
                "options": raster_options,
                "allowCrsTransformation": (True if allow_crs_transformation else None),
                "srid": raster_srid,
                "scaleX": raster_scale_x,
                "scaleY": raster_scale_y,
            },
        )
        return res.content

    def compute(
        self,
        output: Dict[str, GeospatialComputeFunction],
    ) -> GeospatialComputedResponse:
        """`Compute`
        <https://docs.cognite.com/api/v1/#tag/Geospatial/operation/compute>

        Args:
            output : Mapping of keys to compute functions.

        Returns:
            dict: Mapping of keys to computed items.

        Examples:

            Compute the transformation of an ewkt geometry from one SRID to another:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.geospatial import GeospatialGeometryTransformComputeFunction, GeospatialGeometryValueComputeFunction
                >>> c = CogniteClient()
                >>> compute_function = GeospatialGeometryTransformComputeFunction(GeospatialGeometryValueComputeFunction("SRID=4326;POLYGON((0 0,10 0,10 10,0 10,0 0))"), srid=23031)
                >>> compute_result = c.geospatial.compute(output = {"output": compute_function})
        """
        url_path = self._compute_path()

        res = self._do_request(
            "POST",
            url_path,
            timeout=self._config.timeout,
            json={"output": {k: v.to_json_payload() for k, v in output.items()}},
        )
        json = res.json()
        return GeospatialComputedResponse._load(json, cognite_client=self._cognite_client)
