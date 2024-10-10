from __future__ import annotations

import numbers
import urllib.parse
from collections.abc import Iterator, Sequence
from typing import Any, cast, overload

from requests.exceptions import ChunkedEncodingError

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.geospatial import (
    CoordinateReferenceSystem,
    CoordinateReferenceSystemList,
    CoordinateReferenceSystemWrite,
    Feature,
    FeatureAggregateList,
    FeatureList,
    FeatureType,
    FeatureTypeList,
    FeatureTypePatch,
    FeatureTypeWrite,
    FeatureWrite,
    FeatureWriteList,
    GeospatialComputedResponse,
    GeospatialComputeFunction,
    OrderSpec,
    RasterMetadata,
)
from cognite.client.exceptions import CogniteConnectionError
from cognite.client.utils import _json
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


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

    @overload
    def create_feature_types(self, feature_type: FeatureType | FeatureTypeWrite) -> FeatureType: ...

    @overload
    def create_feature_types(
        self, feature_type: Sequence[FeatureType] | Sequence[FeatureTypeWrite]
    ) -> FeatureTypeList: ...

    def create_feature_types(
        self, feature_type: FeatureType | FeatureTypeWrite | Sequence[FeatureType] | Sequence[FeatureTypeWrite]
    ) -> FeatureType | FeatureTypeList:
        """`Creates feature types`
        <https://developer.cognite.com/api#tag/Geospatial/operation/createFeatureTypes>

        Args:
            feature_type (FeatureType | FeatureTypeWrite | Sequence[FeatureType] | Sequence[FeatureTypeWrite]): feature type definition or list of feature type definitions to create.

        Returns:
            FeatureType | FeatureTypeList: Created feature type definition(s)

        Examples:

            Create new type definitions:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.geospatial import FeatureTypeWrite
                >>> client = CogniteClient()
                >>> feature_types = [
                ...     FeatureTypeWrite(external_id="wells", properties={"location": {"type": "POINT", "srid": 4326}})
                ...     FeatureTypeWrite(
                ...       external_id="cities",
                ...       properties={"name": {"type": "STRING", "size": 10}},
                ...       search_spec={"name_index": {"properties": ["name"]}}
                ...     )
                ... ]
                >>> res = client.geospatial.create_feature_types(feature_types)
        """
        return self._create_multiple(
            list_cls=FeatureTypeList,
            resource_cls=FeatureType,
            items=feature_type,
            resource_path=f"{self._RESOURCE_PATH}/featuretypes",
            input_resource_cls=FeatureTypeWrite,
        )

    def delete_feature_types(self, external_id: str | SequenceNotStr[str], recursive: bool = False) -> None:
        """`Delete one or more feature type`
        <https://developer.cognite.com/api#tag/Geospatial/operation/GeospatialDeleteFeatureTypes>

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external ids
            recursive (bool): if `true` the features will also be dropped

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.geospatial.delete_feature_types(external_id=["wells", "cities"])
        """
        extra_body_fields = {"recursive": True} if recursive else {}
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
            resource_path=f"{self._RESOURCE_PATH}/featuretypes",
            extra_body_fields=extra_body_fields,
        )

    def list_feature_types(self) -> FeatureTypeList:
        """`List feature types`
        <https://developer.cognite.com/api#tag/Geospatial/operation/listFeatureTypes>

        Returns:
            FeatureTypeList: List of feature types

        Examples:

            Iterate over feature type definitions:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for feature_type in client.geospatial.list_feature_types():
                ...     feature_type # do something with the feature type definition
        """
        return self._list(
            list_cls=FeatureTypeList,
            resource_cls=FeatureType,
            method="POST",
            resource_path=f"{self._RESOURCE_PATH}/featuretypes",
        )

    @overload
    def retrieve_feature_types(self, external_id: str) -> FeatureType: ...

    @overload
    def retrieve_feature_types(self, external_id: list[str]) -> FeatureTypeList: ...

    def retrieve_feature_types(self, external_id: str | list[str]) -> FeatureType | FeatureTypeList:
        """`Retrieve feature types`
        <https://developer.cognite.com/api#tag/Geospatial/operation/getFeatureTypesByIds>

        Args:
            external_id (str | list[str]): External ID

        Returns:
            FeatureType | FeatureTypeList: Requested Type or None if it does not exist.

        Examples:

            Get Type by external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.geospatial.retrieve_feature_types(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_id)
        return self._retrieve_multiple(
            list_cls=FeatureTypeList,
            resource_cls=FeatureType,
            identifiers=identifiers.as_singleton() if identifiers.is_singleton() else identifiers,
            resource_path=f"{self._RESOURCE_PATH}/featuretypes",
        )

    def patch_feature_types(self, patch: FeatureTypePatch | Sequence[FeatureTypePatch]) -> FeatureTypeList:
        """`Patch feature types`
        <https://developer.cognite.com/api#tag/Geospatial/operation/updateFeatureTypes>

        Args:
            patch (FeatureTypePatch | Sequence[FeatureTypePatch]): the patch to apply

        Returns:
            FeatureTypeList: The patched feature types.

        Examples:

            Add one property to a feature type and add indexes

                >>> from cognite.client.data_classes.geospatial import Patches
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.geospatial.patch_feature_types(
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
                >>> client = CogniteClient()
                >>> res = client.geospatial.patch_feature_types(
                ...    patch=FeatureTypePatch(
                ...         external_id="wells",
                ...         search_spec_patches=Patches(add={"location_idx": {"properties": ["location"]}})
                ... ))

        """
        if isinstance(patch, FeatureTypePatch):
            patch = [patch]
        payload = {
            "items": [
                {
                    "externalId": it.external_id,
                    "update": {"properties": it.property_patches, "searchSpec": it.search_spec_patches},
                }
                for it in patch
            ]
        }
        res = self._post(url_path=f"{self._RESOURCE_PATH}/featuretypes/update", json=payload)
        return FeatureTypeList._load(res.json()["items"], cognite_client=self._cognite_client)

    @overload
    def create_features(
        self,
        feature_type_external_id: str,
        feature: Feature | FeatureWrite,
        allow_crs_transformation: bool = False,
        chunk_size: int | None = None,
    ) -> Feature: ...

    @overload
    def create_features(
        self,
        feature_type_external_id: str,
        feature: Sequence[Feature] | Sequence[FeatureWrite] | FeatureList | FeatureWriteList,
        allow_crs_transformation: bool = False,
        chunk_size: int | None = None,
    ) -> FeatureList: ...

    def create_features(
        self,
        feature_type_external_id: str,
        feature: Feature | FeatureWrite | Sequence[Feature] | Sequence[FeatureWrite] | FeatureList | FeatureWriteList,
        allow_crs_transformation: bool = False,
        chunk_size: int | None = None,
    ) -> Feature | FeatureList:
        """`Creates features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/createFeatures>

        Args:
            feature_type_external_id (str): Feature type definition for the features to create.
            feature (Feature | FeatureWrite | Sequence[Feature] | Sequence[FeatureWrite] | FeatureList | FeatureWriteList): one feature or a list of features to create or a FeatureList object
            allow_crs_transformation (bool): If true, then input geometries will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.
            chunk_size (int | None): maximum number of items in a single request to the api

        Returns:
            Feature | FeatureList: Created features

        Examples:

            Create a new feature type and corresponding feature:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.geospatial import FeatureTypeWrite, FeatureWrite
                >>> client = CogniteClient()
                >>> feature_types = [
                ...     FeatureTypeWrite(
                ...         external_id="my_feature_type",
                ...         properties={
                ...             "location": {"type": "POINT", "srid": 4326},
                ...             "temperature": {"type": "DOUBLE"}
                ...         }
                ...     )
                ... ]
                >>> res = client.geospatial.create_feature_types(feature_types)
                >>> res = client.geospatial.create_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=FeatureWrite(
                ...         external_id="my_feature",
                ...         location={"wkt": "POINT(1 1)"},
                ...         temperature=12.4
                ...     )
                ... )
        """
        if chunk_size is not None and (chunk_size < 1 or chunk_size > self._CREATE_LIMIT):
            raise ValueError(f"The chunk_size must be strictly positive and not exceed {self._CREATE_LIMIT}")
        if isinstance(feature, (FeatureList, FeatureWriteList)):
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
            input_resource_cls=FeatureWrite,
        )

    def delete_features(
        self, feature_type_external_id: str, external_id: str | SequenceNotStr[str] | None = None
    ) -> None:
        """`Delete one or more feature`
        <https://developer.cognite.com/api#tag/Geospatial/operation/deleteFeatures>

        Args:
            feature_type_external_id (str): No description.
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.geospatial.delete_features(
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
        properties: dict[str, Any] | None = None,
    ) -> Feature: ...

    @overload
    def retrieve_features(
        self,
        feature_type_external_id: str,
        external_id: list[str],
        properties: dict[str, Any] | None = None,
    ) -> FeatureList: ...

    def retrieve_features(
        self,
        feature_type_external_id: str,
        external_id: str | list[str],
        properties: dict[str, Any] | None = None,
    ) -> FeatureList | Feature:
        """`Retrieve features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/getFeaturesByIds>

        Args:
            feature_type_external_id (str): No description.
            external_id (str | list[str]): External ID or list of external ids
            properties (dict[str, Any] | None): the output property selection

        Returns:
            FeatureList | Feature: Requested features or None if it does not exist.

        Examples:

            Retrieve one feature by its external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.geospatial.retrieve_features(
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
        feature: Feature | Sequence[Feature],
        allow_crs_transformation: bool = False,
        chunk_size: int | None = None,
    ) -> FeatureList:
        """`Update features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/updateFeatures>

        Args:
            feature_type_external_id (str): No description.
            feature (Feature | Sequence[Feature]): feature or list of features.
            allow_crs_transformation (bool): If true, then input geometries will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.
            chunk_size (int | None): maximum number of items in a single request to the api

        Returns:
            FeatureList: Updated features

        Examples:

            Update one feature:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> my_feature = client.geospatial.create_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(external_id="my_feature", temperature=12.4)
                ... )
                >>> my_updated_feature = client.geospatial.update_features(
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
        filter: dict[str, Any] | None = None,
        properties: dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        allow_crs_transformation: bool = False,
    ) -> FeatureList:
        """`List features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/listFeatures>

        This method allows to filter all features.

        Args:
            feature_type_external_id (str): the feature type to list features for
            filter (dict[str, Any] | None): the list filter
            properties (dict[str, Any] | None): the output property selection
            limit (int | None): Maximum number of features to return. Defaults to 25. Set to -1, float("inf") or None to return all features.
            allow_crs_transformation (bool): If true, then input geometries if existing in the filter will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.

        Returns:
            FeatureList: The filtered features

        Examples:

            List features:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> my_feature_type = client.geospatial.retrieve_feature_types(
                ...     external_id="my_feature_type"
                ... )
                >>> my_feature = client.geospatial.create_features(
                ...     feature_type_external_id=my_feature_type,
                ...     feature=Feature(
                ...         external_id="my_feature",
                ...         temperature=12.4,
                ...         location={"wkt": "POINT(0 1)"}
                ...     )
                ... )
                >>> res = client.geospatial.list_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={"range": {"property": "temperature", "gt": 12.0}}
                ... )
                >>> for f in res:
                ...     # do something with the features

            Search for features and select output properties:

                >>> res = client.geospatial.list_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={},
                ...     properties={"temperature": {}, "pressure": {}}
                ... )

            Search for features with spatial filters:

                >>> res = client.geospatial.list_features(
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
        filter: dict[str, Any] | None = None,
        properties: dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        order_by: Sequence[OrderSpec] | None = None,
        allow_crs_transformation: bool = False,
        allow_dimensionality_mismatch: bool = False,
    ) -> FeatureList:
        """`Search for features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/searchFeatures>

        This method allows to order the result by one or more of the properties of the feature type.
        However, the number of items returned is limited to 1000 and there is no support for cursors yet.
        If you need to return more than 1000 items, use the `stream_features(...)` method instead.

        Args:
            feature_type_external_id (str): The feature type to search for
            filter (dict[str, Any] | None): The search filter
            properties (dict[str, Any] | None): The output property selection
            limit (int): Maximum number of results
            order_by (Sequence[OrderSpec] | None): The order specification
            allow_crs_transformation (bool): If true, then input geometries will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.
            allow_dimensionality_mismatch (bool): Indicating if the spatial filter operators allow input geometries with a different dimensionality than the properties they are applied to. Defaults to False.

        Returns:
            FeatureList: the filtered features

        Examples:

            Search for features:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> my_feature_type = client.geospatial.retrieve_feature_types(
                ...     external_id="my_feature_type"
                ... )
                >>> my_feature = client.geospatial.create_features(
                ...     feature_type_external_id=my_feature_type,
                ...     feature=Feature(
                ...         external_id="my_feature",
                ...         temperature=12.4,
                ...         location={"wkt": "POINT(0 1)"}
                ...     )
                ... )
                >>> res = client.geospatial.search_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={"range": {"property": "temperature", "gt": 12.0}}
                ... )
                >>> for f in res:
                ...     # do something with the features

            Search for features and select output properties:

                >>> res = client.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={},
                ...     properties={"temperature": {}, "pressure": {}}
                ... )

            Search for features and do CRS conversion on an output property:

                >>> res = client.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={},
                ...     properties={"location": {"srid": 3995}}
                ... )

            Search for features and order results:

                >>> res = client.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={},
                ...     order_by=[
                ...         OrderSpec("temperature", "ASC"),
                ...         OrderSpec("pressure", "DESC")]
                ... )

            Search for features with spatial filters:

                >>> res = client.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={"stWithin": {
                ...         "property": "location",
                ...         "value": {"wkt": "POLYGON((0 0, 0 1, 1 1, 0 0))"}
                ...     }}
                ... )

            Combining multiple filters:

                >>> res = client.geospatial.search_features(
                ...     feature_type_external_id=my_feature_type,
                ...     filter={"and": [
                ...         {"range": {"property": "temperature", "gt": 12.0}},
                ...         {"stWithin": {
                ...             "property": "location",
                ...             "value": {"wkt": "POLYGON((0 0, 0 1, 1 1, 0 0))"}
                ...         }}
                ...     ]}
                ... )

                >>> res = client.geospatial.search_features(
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
        order = None if order_by is None else [f"{item.property}:{item.direction}" for item in order_by]
        res = self._post(
            url_path=resource_path,
            json={
                "filter": filter or {},
                "limit": limit,
                "output": {"properties": properties},
                "sort": order,
                "allowCrsTransformation": allow_crs_transformation,
                "allowDimensionalityMismatch": allow_dimensionality_mismatch,
            },
        )
        return FeatureList._load(res.json()["items"], cognite_client=self._cognite_client)

    def stream_features(
        self,
        feature_type_external_id: str,
        filter: dict[str, Any] | None = None,
        properties: dict[str, Any] | None = None,
        allow_crs_transformation: bool = False,
        allow_dimensionality_mismatch: bool = False,
    ) -> Iterator[Feature]:
        """`Stream features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/searchFeaturesStreaming>

        This method allows to return any number of items until the underlying
        api calls times out. The order of the result items is not deterministic.
        If you need to order the results, use the `search_features(...)` method instead.

        Args:
            feature_type_external_id (str): the feature type to search for
            filter (dict[str, Any] | None): the search filter
            properties (dict[str, Any] | None): the output property selection
            allow_crs_transformation (bool): If true, then input geometries will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.
            allow_dimensionality_mismatch (bool): Indicating if the spatial filter operators allow input geometries with a different dimensionality than the properties they are applied to. Defaults to False.

        Yields:
            Feature: a generator for the filtered features

        Examples:

            Stream features:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> my_feature = client.geospatial.create_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(external_id="my_feature", temperature=12.4)
                ... )
                >>> features = client.geospatial.stream_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={"range": {"property": "temperature", "gt": 12.0}}
                ... )
                >>> for f in features:
                ...     # do something with the features

            Stream features and select output properties:

                >>> features = client.geospatial.stream_features(
                ...     feature_type_external_id="my_feature_type",
                ...     filter={},
                ...     properties={"temperature": {}, "pressure": {}}
                ... )
                >>> for f in features:
                ...     # do something with the features
        """
        resource_path = self._feature_resource_path(feature_type_external_id) + "/search-streaming"
        payload = {
            "filter": filter or {},
            "output": {"properties": properties, "jsonStreamFormat": "NEW_LINE_DELIMITED"},
            "allowCrsTransformation": allow_crs_transformation,
            "allowDimensionalityMismatch": allow_dimensionality_mismatch,
        }
        res = self._do_request("POST", url_path=resource_path, json=payload, timeout=self._config.timeout, stream=True)

        try:
            for line in res.iter_lines():
                yield Feature._load(_json.loads(line))
        except (ChunkedEncodingError, ConnectionError) as e:
            raise CogniteConnectionError(e)

    def aggregate_features(
        self,
        feature_type_external_id: str,
        filter: dict[str, Any] | None = None,
        group_by: SequenceNotStr[str] | None = None,
        order_by: Sequence[OrderSpec] | None = None,
        output: dict[str, Any] | None = None,
    ) -> FeatureAggregateList:
        """`Aggregate filtered features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/aggregateFeatures>

        Args:
            feature_type_external_id (str): the feature type to filter features from
            filter (dict[str, Any] | None): the search filter
            group_by (SequenceNotStr[str] | None): list of properties to group by with
            order_by (Sequence[OrderSpec] | None): the order specification
            output (dict[str, Any] | None): the aggregate output

        Returns:
            FeatureAggregateList: the filtered features

        Examples:

            Aggregate property of features:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> my_feature = client.geospatial.create_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(external_id="my_feature", temperature=12.4)
                ... )
                >>> res = client.geospatial.aggregate_features(
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
        resource_path = self._feature_resource_path(feature_type_external_id) + "/aggregate"
        order = None if order_by is None else [f"{item.property}:{item.direction}" for item in order_by]
        res = self._post(
            url_path=resource_path,
            json={
                "filter": filter or {},
                "groupBy": group_by,
                "sort": order,
                "output": output,
            },
        )
        return FeatureAggregateList._load(res.json()["items"], cognite_client=self._cognite_client)

    def get_coordinate_reference_systems(self, srids: int | Sequence[int]) -> CoordinateReferenceSystemList:
        """`Get Coordinate Reference Systems`
        <https://developer.cognite.com/api#tag/Geospatial/operation/getCoordinateReferenceSystem>

        Args:
            srids (int | Sequence[int]): (Union[int, Sequence[int]]): SRID or list of SRIDs

        Returns:
            CoordinateReferenceSystemList: Requested CRSs.

        Examples:

            Get two CRS definitions:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> crs = client.geospatial.get_coordinate_reference_systems(srids=[4326, 4327])
        """
        if isinstance(srids, (int, numbers.Integral)):
            srids_processed: Sequence[numbers.Integral | int] = [srids]
        else:
            srids_processed = srids

        res = self._post(
            url_path=f"{self._RESOURCE_PATH}/crs/byids", json={"items": [{"srid": srid} for srid in srids_processed]}
        )
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def list_coordinate_reference_systems(self, only_custom: bool = False) -> CoordinateReferenceSystemList:
        """`List Coordinate Reference Systems`
        <https://developer.cognite.com/api#tag/Geospatial/operation/listGeospatialCoordinateReferenceSystems>

        Args:
            only_custom (bool): list only custom CRSs or not

        Returns:
            CoordinateReferenceSystemList: list of CRSs.

        Examples:

            Fetch all custom CRSs:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> crs = client.geospatial.list_coordinate_reference_systems(only_custom=True)
        """
        res = self._get(url_path=f"{self._RESOURCE_PATH}/crs", params={"filterCustom": only_custom})
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def create_coordinate_reference_systems(
        self,
        crs: CoordinateReferenceSystem
        | CoordinateReferenceSystemWrite
        | Sequence[CoordinateReferenceSystem]
        | Sequence[CoordinateReferenceSystemWrite],
    ) -> CoordinateReferenceSystemList:
        """`Create Coordinate Reference System`
        <https://developer.cognite.com/api#tag/Geospatial/operation/createGeospatialCoordinateReferenceSystems>

        Args:
            crs (CoordinateReferenceSystem | CoordinateReferenceSystemWrite | Sequence[CoordinateReferenceSystem] | Sequence[CoordinateReferenceSystemWrite]): a CoordinateReferenceSystem or a list of CoordinateReferenceSystem

        Returns:
            CoordinateReferenceSystemList: list of CRSs.

        Examples:

            Create a custom CRS:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import CoordinateReferenceSystemWrite
                >>> client = CogniteClient()
                >>> custom_crs = CoordinateReferenceSystemWrite(
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
                >>> crs = client.geospatial.create_coordinate_reference_systems(custom_crs)
        """
        if isinstance(crs, CoordinateReferenceSystem):
            crs = [crs.as_write()]
        elif isinstance(crs, CoordinateReferenceSystemWrite):
            crs = [crs]
        elif isinstance(crs, Sequence):
            crs = [it.as_write() if isinstance(it, CoordinateReferenceSystem) else it for it in crs]

        res = self._post(
            url_path=f"{self._RESOURCE_PATH}/crs", json={"items": [it.dump(camel_case=True) for it in crs]}
        )
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def delete_coordinate_reference_systems(self, srids: int | Sequence[int]) -> None:
        """`Delete Coordinate Reference System`
        <https://developer.cognite.com/api#tag/Geospatial/operation/deleteGeospatialCoordinateReferenceSystems>

        Args:
            srids (int | Sequence[int]): (Union[int, Sequence[int]]): SRID or list of SRIDs

        Examples:

            Delete a custom CRS:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> crs = client.geospatial.delete_coordinate_reference_systems(srids=[121111])
        """
        if isinstance(srids, (int, numbers.Integral)):
            srids_processed: Sequence[numbers.Integral | int] = [srids]
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
        raster_scale_x: float | None = None,
        raster_scale_y: float | None = None,
    ) -> RasterMetadata:
        """`Put raster <https://developer.cognite.com/api#tag/Geospatial/operation/putRaster>`

        Args:
            feature_type_external_id (str): No description.
            feature_external_id (str): one feature or a list of features to create
            raster_property_name (str): the raster property name
            raster_format (str): the raster input format
            raster_srid (int): the associated SRID for the raster
            file (str): the path to the file of the raster
            allow_crs_transformation (bool): When the parameter is false, requests with rasters in Coordinate Reference System different from the one defined in the feature type will result in bad request response code.
            raster_scale_x (float | None): the X component of the pixel width in units of coordinate reference system
            raster_scale_y (float | None): the Y component of the pixel height in units of coordinate reference system

        Returns:
            RasterMetadata: the raster metadata if it was ingested successfully

        Examples:

            Put a raster in a feature raster property:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> feature_type = ...
                >>> feature = ...
                >>> raster_property_name = ...
                >>> metadata = client.geospatial.put_raster(feature_type.external_id, feature.external_id,
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
        with open(file, "rb") as fh:
            data = fh.read()
        res = self._do_request(
            "PUT",
            url_path,
            data=data,
            headers={"Content-Type": "application/binary"},
            timeout=self._config.timeout,
        )
        return RasterMetadata.load(res.json(), cognite_client=self._cognite_client)

    def delete_raster(
        self,
        feature_type_external_id: str,
        feature_external_id: str,
        raster_property_name: str,
    ) -> None:
        """`Delete raster <https://developer.cognite.com/api#tag/Geospatial/operation/deleteRaster>`

        Args:
            feature_type_external_id (str): No description.
            feature_external_id (str): one feature or a list of features to create
            raster_property_name (str): the raster property name

        Examples:

            Delete a raster in a feature raster property:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> feature_type = ...
                >>> feature = ...
                >>> raster_property_name = ...
                >>> client.geospatial.delete_raster(feature_type.external_id, feature.external_id, raster_property_name)
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
        raster_options: dict[str, Any] | None = None,
        raster_srid: int | None = None,
        raster_scale_x: float | None = None,
        raster_scale_y: float | None = None,
        allow_crs_transformation: bool = False,
    ) -> bytes:
        """`Get raster <https://developer.cognite.com/api#tag/Geospatial/operation/getRaster>`

        Args:
            feature_type_external_id (str): Feature type definition for the features to create.
            feature_external_id (str): one feature or a list of features to create
            raster_property_name (str): the raster property name
            raster_format (str): the raster output format
            raster_options (dict[str, Any] | None): GDAL raster creation key-value options
            raster_srid (int | None): the SRID for the output raster
            raster_scale_x (float | None): the X component of the output pixel width in units of coordinate reference system
            raster_scale_y (float | None): the Y component of the output pixel height in units of coordinate reference system
            allow_crs_transformation (bool): When the parameter is false, requests with output rasters in Coordinate Reference System different from the one defined in the feature type will result in bad request response code.

        Returns:
            bytes: the raster data

        Examples:

            Get a raster from a feature raster property:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> feature_type = ...
                >>> feature = ...
                >>> raster_property_name = ...
                >>> raster_data = client.geospatial.get_raster(feature_type.external_id, feature.external_id,
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
        output: dict[str, GeospatialComputeFunction],
    ) -> GeospatialComputedResponse:
        """`Compute <https://developer.cognite.com/api#tag/Geospatial/operation/compute>`

        Args:
            output (dict[str, GeospatialComputeFunction]): No description.

        Returns:
            GeospatialComputedResponse: Mapping of keys to computed items.

        Examples:

            Compute the transformation of an ewkt geometry from one SRID to another:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.geospatial import GeospatialGeometryTransformComputeFunction, GeospatialGeometryValueComputeFunction
                >>> client = CogniteClient()
                >>> compute_function = GeospatialGeometryTransformComputeFunction(GeospatialGeometryValueComputeFunction("SRID=4326;POLYGON((0 0,10 0,10 10,0 10,0 0))"), srid=23031)
                >>> compute_result = client.geospatial.compute(output = {"output": compute_function})
        """
        res = self._do_request(
            "POST",
            f"{GeospatialAPI._RESOURCE_PATH}/compute",
            timeout=self._config.timeout,
            json={"output": {k: v.to_json_payload() for k, v in output.items()}},
        )
        return GeospatialComputedResponse._load(res.json(), cognite_client=self._cognite_client)
