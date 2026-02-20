"""
===============================================================================
ddd906b7c138e3c75c04f8648c4438c8
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
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
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class SyncGeospatialAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def create_feature_types(self, feature_type: FeatureType | FeatureTypeWrite) -> FeatureType: ...

    @overload
    def create_feature_types(
        self, feature_type: Sequence[FeatureType] | Sequence[FeatureTypeWrite]
    ) -> FeatureTypeList: ...

    def create_feature_types(
        self, feature_type: FeatureType | FeatureTypeWrite | Sequence[FeatureType] | Sequence[FeatureTypeWrite]
    ) -> FeatureType | FeatureTypeList:
        """
        `Creates feature types`
        <https://developer.cognite.com/api#tag/Geospatial/operation/createFeatureTypes>

        Args:
            feature_type: feature type definition or list of feature type definitions to create.

        Returns:
            Created feature type definition(s)

        Examples:

            Create new type definitions:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.geospatial import FeatureTypeWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(self.__async_client.geospatial.create_feature_types(feature_type=feature_type))

    def delete_feature_types(self, external_id: str | SequenceNotStr[str], recursive: bool = False) -> None:
        """
        `Delete one or more feature type`
        <https://developer.cognite.com/api#tag/Geospatial/operation/GeospatialDeleteFeatureTypes>

        Args:
            external_id: External ID or list of external ids
            recursive: if `true` the features will also be dropped

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.geospatial.delete_feature_types(external_id=["wells", "cities"])
        """
        return run_sync(
            self.__async_client.geospatial.delete_feature_types(external_id=external_id, recursive=recursive)
        )

    def list_feature_types(self) -> FeatureTypeList:
        """
        `List feature types`
        <https://developer.cognite.com/api#tag/Geospatial/operation/listFeatureTypes>

        Returns:
            List of feature types

        Examples:

            Iterate over feature type definitions:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> for feature_type in client.geospatial.list_feature_types():
                ...     feature_type # do something with the feature type definition
        """
        return run_sync(self.__async_client.geospatial.list_feature_types())

    @overload
    def retrieve_feature_types(self, external_id: str) -> FeatureType: ...

    @overload
    def retrieve_feature_types(self, external_id: list[str]) -> FeatureTypeList: ...

    def retrieve_feature_types(self, external_id: str | list[str]) -> FeatureType | FeatureTypeList:
        """
        `Retrieve feature types`
        <https://developer.cognite.com/api#tag/Geospatial/operation/getFeatureTypesByIds>

        Args:
            external_id: External ID

        Returns:
            Requested Type or None if it does not exist.

        Examples:

            Get Type by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.geospatial.retrieve_feature_types(external_id="1")
        """
        return run_sync(self.__async_client.geospatial.retrieve_feature_types(external_id=external_id))

    def patch_feature_types(self, patch: FeatureTypePatch | Sequence[FeatureTypePatch]) -> FeatureTypeList:
        """
        `Patch feature types`
        <https://developer.cognite.com/api#tag/Geospatial/operation/updateFeatureTypes>

        Args:
            patch: the patch to apply

        Returns:
            The patched feature types.

        Examples:

            Add one property to a feature type and add indexes

                >>> from cognite.client.data_classes.geospatial import Patches
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
                >>> res = client.geospatial.patch_feature_types(
                ...    patch=FeatureTypePatch(
                ...         external_id="wells",
                ...         search_spec_patches=Patches(add={"location_idx": {"properties": ["location"]}})
                ... ))
        """
        return run_sync(self.__async_client.geospatial.patch_feature_types(patch=patch))

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
        """
        `Creates features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/createFeatures>

        Args:
            feature_type_external_id: Feature type definition for the features to create.
            feature: one feature or a list of features to create or a FeatureList object
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.
            chunk_size: maximum number of items in a single request to the api

        Returns:
            Created features

        Examples:

            Create a new feature type and corresponding feature:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.geospatial import FeatureTypeWrite, FeatureWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(
            self.__async_client.geospatial.create_features(
                feature_type_external_id=feature_type_external_id,
                feature=feature,
                allow_crs_transformation=allow_crs_transformation,
                chunk_size=chunk_size,
            )
        )

    def delete_features(
        self, feature_type_external_id: str, external_id: str | SequenceNotStr[str] | None = None
    ) -> None:
        """
        `Delete one or more feature`
        <https://developer.cognite.com/api#tag/Geospatial/operation/deleteFeatures>

        Args:
            feature_type_external_id: No description.
            external_id: External ID or list of external ids

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.geospatial.delete_features(
                ...     feature_type_external_id="my_feature_type",
                ...     external_id=my_feature
                ... )
        """
        return run_sync(
            self.__async_client.geospatial.delete_features(
                feature_type_external_id=feature_type_external_id, external_id=external_id
            )
        )

    @overload
    def retrieve_features(
        self, feature_type_external_id: str, external_id: str, properties: dict[str, Any] | None = None
    ) -> Feature: ...

    @overload
    def retrieve_features(
        self, feature_type_external_id: str, external_id: list[str], properties: dict[str, Any] | None = None
    ) -> FeatureList: ...

    def retrieve_features(
        self, feature_type_external_id: str, external_id: str | list[str], properties: dict[str, Any] | None = None
    ) -> FeatureList | Feature:
        """
        `Retrieve features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/getFeaturesByIds>

        Args:
            feature_type_external_id: No description.
            external_id: External ID or list of external ids
            properties: the output property selection

        Returns:
            Requested features or None if it does not exist.

        Examples:

            Retrieve one feature by its external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.geospatial.retrieve_features(
                ...     feature_type_external_id="my_feature_type",
                ...     external_id="my_feature"
                ... )
        """
        return run_sync(
            self.__async_client.geospatial.retrieve_features(
                feature_type_external_id=feature_type_external_id, external_id=external_id, properties=properties
            )
        )

    @overload
    def update_features(
        self,
        feature_type_external_id: str,
        feature: Feature | FeatureWrite,
        allow_crs_transformation: bool = False,
        chunk_size: int | None = None,
    ) -> Feature: ...

    @overload
    def update_features(
        self,
        feature_type_external_id: str,
        feature: Sequence[Feature] | Sequence[FeatureWrite],
        allow_crs_transformation: bool = False,
        chunk_size: int | None = None,
    ) -> FeatureList: ...

    def update_features(
        self,
        feature_type_external_id: str,
        feature: Feature | FeatureWrite | Sequence[Feature] | Sequence[FeatureWrite],
        allow_crs_transformation: bool = False,
        chunk_size: int | None = None,
    ) -> Feature | FeatureList:
        """
        `Update features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/updateFeatures>

        Args:
            feature_type_external_id: No description.
            feature: feature or list of features.
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.
            chunk_size: maximum number of items in a single request to the api

        Returns:
            Updated features

        Examples:

            Update one feature:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> my_feature = client.geospatial.create_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(external_id="my_feature", temperature=12.4)
                ... )
                >>> my_updated_feature = client.geospatial.update_features(
                ...     feature_type_external_id="my_feature_type",
                ...     feature=Feature(external_id="my_feature", temperature=6.237)
                ... )
        """
        return run_sync(
            self.__async_client.geospatial.update_features(
                feature_type_external_id=feature_type_external_id,
                feature=feature,
                allow_crs_transformation=allow_crs_transformation,
                chunk_size=chunk_size,
            )
        )

    def list_features(
        self,
        feature_type_external_id: str,
        filter: dict[str, Any] | None = None,
        properties: dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        allow_crs_transformation: bool = False,
    ) -> FeatureList:
        """
        `List features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/listFeatures>

        This method allows to filter all features.

        Args:
            feature_type_external_id: the feature type to list features for
            filter: the list filter
            properties: the output property selection
            limit: Maximum number of features to return. Defaults to 25. Set to -1, float("inf") or None to return all features.
            allow_crs_transformation: If true, then input geometries if existing in the filter will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.

        Returns:
            The filtered features

        Examples:

            List features:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(
            self.__async_client.geospatial.list_features(
                feature_type_external_id=feature_type_external_id,
                filter=filter,
                properties=properties,
                limit=limit,
                allow_crs_transformation=allow_crs_transformation,
            )
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
        """
        `Search for features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/searchFeatures>

        This method allows to order the result by one or more of the properties of the feature type.
        However, the number of items returned is limited to 1000 and there is no support for cursors yet.
        If you need to return more than 1000 items, use the `stream_features(...)` method instead.

        Args:
            feature_type_external_id: The feature type to search for
            filter: The search filter
            properties: The output property selection
            limit: Maximum number of results
            order_by: The order specification
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.
            allow_dimensionality_mismatch: Indicating if the spatial filter operators allow input geometries with a different dimensionality than the properties they are applied to. Defaults to False.

        Returns:
            the filtered features

        Examples:

            Search for features:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(
            self.__async_client.geospatial.search_features(
                feature_type_external_id=feature_type_external_id,
                filter=filter,
                properties=properties,
                limit=limit,
                order_by=order_by,
                allow_crs_transformation=allow_crs_transformation,
                allow_dimensionality_mismatch=allow_dimensionality_mismatch,
            )
        )

    def stream_features(
        self,
        feature_type_external_id: str,
        filter: dict[str, Any] | None = None,
        properties: dict[str, Any] | None = None,
        allow_crs_transformation: bool = False,
        allow_dimensionality_mismatch: bool = False,
    ) -> Iterator[Feature]:
        """
        `Stream features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/searchFeaturesStreaming>

        This method allows to return any number of items until the underlying
        api calls times out. The order of the result items is not deterministic.
        If you need to order the results, use the `search_features(...)` method instead.

        Args:
            feature_type_external_id: the feature type to search for
            filter: the search filter
            properties: the output property selection
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference System defined in the feature type specification. When it is false, then requests with geometries in Coordinate Reference System different from the ones defined in the feature type will result in CogniteAPIError exception.
            allow_dimensionality_mismatch: Indicating if the spatial filter operators allow input geometries with a different dimensionality than the properties they are applied to. Defaults to False.

        Yields:
            a generator for the filtered features

        Examples:

            Stream features:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
        yield from SyncIterator(
            self.__async_client.geospatial.stream_features(
                feature_type_external_id=feature_type_external_id,
                filter=filter,
                properties=properties,
                allow_crs_transformation=allow_crs_transformation,
                allow_dimensionality_mismatch=allow_dimensionality_mismatch,
            )
        )

    def aggregate_features(
        self,
        feature_type_external_id: str,
        filter: dict[str, Any] | None = None,
        group_by: SequenceNotStr[str] | None = None,
        order_by: Sequence[OrderSpec] | None = None,
        output: dict[str, Any] | None = None,
    ) -> FeatureAggregateList:
        """
        `Aggregate filtered features`
        <https://developer.cognite.com/api#tag/Geospatial/operation/aggregateFeatures>

        Args:
            feature_type_external_id: the feature type to filter features from
            filter: the search filter
            group_by: list of properties to group by with
            order_by: the order specification
            output: the aggregate output

        Returns:
            the filtered features

        Examples:

            Aggregate property of features:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(
            self.__async_client.geospatial.aggregate_features(
                feature_type_external_id=feature_type_external_id,
                filter=filter,
                group_by=group_by,
                order_by=order_by,
                output=output,
            )
        )

    def get_coordinate_reference_systems(self, srids: int | Sequence[int]) -> CoordinateReferenceSystemList:
        """
        `Get Coordinate Reference Systems`
        <https://developer.cognite.com/api#tag/Geospatial/operation/getCoordinateReferenceSystem>

        Args:
            srids: SRID or list of SRIDs

        Returns:
            Requested CRSs.

        Examples:

            Get two CRS definitions:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> crs = client.geospatial.get_coordinate_reference_systems(srids=[4326, 4327])
        """
        return run_sync(self.__async_client.geospatial.get_coordinate_reference_systems(srids=srids))

    def list_coordinate_reference_systems(self, only_custom: bool = False) -> CoordinateReferenceSystemList:
        """
        `List Coordinate Reference Systems`
        <https://developer.cognite.com/api#tag/Geospatial/operation/listGeospatialCoordinateReferenceSystems>

        Args:
            only_custom: list only custom CRSs or not

        Returns:
            list of CRSs.

        Examples:

            Fetch all custom CRSs:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> crs = client.geospatial.list_coordinate_reference_systems(only_custom=True)
        """
        return run_sync(self.__async_client.geospatial.list_coordinate_reference_systems(only_custom=only_custom))

    def create_coordinate_reference_systems(
        self,
        crs: CoordinateReferenceSystem
        | CoordinateReferenceSystemWrite
        | Sequence[CoordinateReferenceSystem]
        | Sequence[CoordinateReferenceSystemWrite],
    ) -> CoordinateReferenceSystemList:
        """
        `Create Coordinate Reference System`
        <https://developer.cognite.com/api#tag/Geospatial/operation/createGeospatialCoordinateReferenceSystems>

        Args:
            crs: a CoordinateReferenceSystem or a list of CoordinateReferenceSystem

        Returns:
            list of CRSs.

        Examples:

            Create a custom CRS:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import CoordinateReferenceSystemWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(self.__async_client.geospatial.create_coordinate_reference_systems(crs=crs))

    def delete_coordinate_reference_systems(self, srids: int | Sequence[int]) -> None:
        """
        `Delete Coordinate Reference System`
        <https://developer.cognite.com/api#tag/Geospatial/operation/deleteGeospatialCoordinateReferenceSystems>

        Args:
            srids: SRID or list of SRIDs

        Examples:

            Delete a custom CRS:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> crs = client.geospatial.delete_coordinate_reference_systems(srids=[121111])
        """
        return run_sync(self.__async_client.geospatial.delete_coordinate_reference_systems(srids=srids))

    def put_raster(
        self,
        feature_type_external_id: str,
        feature_external_id: str,
        raster_property_name: str,
        raster_format: str,
        raster_srid: int,
        file: str | Path,
        allow_crs_transformation: bool = False,
        raster_scale_x: float | None = None,
        raster_scale_y: float | None = None,
    ) -> RasterMetadata:
        """
        `Put raster <https://developer.cognite.com/api#tag/Geospatial/operation/putRaster>`

        Args:
            feature_type_external_id: No description.
            feature_external_id: one feature or a list of features to create
            raster_property_name: the raster property name
            raster_format: the raster input format
            raster_srid: the associated SRID for the raster
            file: the path to the file of the raster
            allow_crs_transformation: When the parameter is false, requests with rasters in Coordinate Reference System different from the one defined in the feature type will result in bad request response code.
            raster_scale_x: the X component of the pixel width in units of coordinate reference system
            raster_scale_y: the Y component of the pixel height in units of coordinate reference system

        Returns:
            the raster metadata if it was ingested successfully

        Examples:

            Put a raster in a feature raster property:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> feature_type = ...
                >>> feature = ...
                >>> raster_property_name = ...
                >>> metadata = client.geospatial.put_raster(feature_type.external_id, feature.external_id,
                ...         raster_property_name, "XYZ", 3857, file)
        """
        return run_sync(
            self.__async_client.geospatial.put_raster(
                feature_type_external_id=feature_type_external_id,
                feature_external_id=feature_external_id,
                raster_property_name=raster_property_name,
                raster_format=raster_format,
                raster_srid=raster_srid,
                file=file,
                allow_crs_transformation=allow_crs_transformation,
                raster_scale_x=raster_scale_x,
                raster_scale_y=raster_scale_y,
            )
        )

    def delete_raster(self, feature_type_external_id: str, feature_external_id: str, raster_property_name: str) -> None:
        """
        `Delete raster <https://developer.cognite.com/api#tag/Geospatial/operation/deleteRaster>`

        Args:
            feature_type_external_id: No description.
            feature_external_id: one feature or a list of features to create
            raster_property_name: the raster property name

        Examples:

            Delete a raster in a feature raster property:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> feature_type = ...
                >>> feature = ...
                >>> raster_property_name = ...
                >>> client.geospatial.delete_raster(feature_type.external_id, feature.external_id, raster_property_name)
        """
        return run_sync(
            self.__async_client.geospatial.delete_raster(
                feature_type_external_id=feature_type_external_id,
                feature_external_id=feature_external_id,
                raster_property_name=raster_property_name,
            )
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
        """
        `Get raster <https://developer.cognite.com/api#tag/Geospatial/operation/getRaster>`

        Args:
            feature_type_external_id: Feature type definition for the features to create.
            feature_external_id: one feature or a list of features to create
            raster_property_name: the raster property name
            raster_format: the raster output format
            raster_options: GDAL raster creation key-value options
            raster_srid: the SRID for the output raster
            raster_scale_x: the X component of the output pixel width in units of coordinate reference system
            raster_scale_y: the Y component of the output pixel height in units of coordinate reference system
            allow_crs_transformation: When the parameter is false, requests with output rasters in Coordinate Reference System different from the one defined in the feature type will result in bad request response code.

        Returns:
            the raster data

        Examples:

            Get a raster from a feature raster property:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> feature_type = ...
                >>> feature = ...
                >>> raster_property_name = ...
                >>> raster_data = client.geospatial.get_raster(feature_type.external_id, feature.external_id,
                ...    raster_property_name, "XYZ", {"SIGNIFICANT_DIGITS": "4"})
        """
        return run_sync(
            self.__async_client.geospatial.get_raster(
                feature_type_external_id=feature_type_external_id,
                feature_external_id=feature_external_id,
                raster_property_name=raster_property_name,
                raster_format=raster_format,
                raster_options=raster_options,
                raster_srid=raster_srid,
                raster_scale_x=raster_scale_x,
                raster_scale_y=raster_scale_y,
                allow_crs_transformation=allow_crs_transformation,
            )
        )

    def compute(self, output: dict[str, GeospatialComputeFunction]) -> GeospatialComputedResponse:
        """
        `Compute <https://developer.cognite.com/api#tag/Geospatial/operation/compute>`

        Args:
            output: No description.

        Returns:
            Mapping of keys to computed items.

        Examples:

            Compute the transformation of an ewkt geometry from one SRID to another:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.geospatial import GeospatialGeometryTransformComputeFunction, GeospatialGeometryValueComputeFunction
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> compute_function = GeospatialGeometryTransformComputeFunction(GeospatialGeometryValueComputeFunction("SRID=4326;POLYGON((0 0,10 0,10 10,0 10,0 0))"), srid=23031)
                >>> compute_result = client.geospatial.compute(output = {"output": compute_function})
        """
        return run_sync(self.__async_client.geospatial.compute(output=output))
