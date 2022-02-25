import json as complexjson
import numbers
from typing import Any, Dict, Generator, List, Union

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
    FeatureTypeUpdate,
    OrderSpec,
)
from cognite.client.exceptions import CogniteConnectionError


class GeospatialAPI(APIClient):
    _RESOURCE_PATH = "/geospatial"

    @staticmethod
    def _feature_resource_path(feature_type_external_id: str):
        return f"{GeospatialAPI._RESOURCE_PATH}/featuretypes/{feature_type_external_id}/features"

    def create_feature_types(
        self, feature_type: Union[FeatureType, List[FeatureType]]
    ) -> Union[FeatureType, FeatureTypeList]:
        """`Creates feature types`
        <https://docs.cognite.com/api/v1/#operation/createFeatureTypes>

        Args:
            feature_type (Union[FeatureType, List[FeatureType]]): feature type definition or list of feature type definitions to create.

        Returns:
            Union[FeatureType, FeatureTypeList]: Created feature type definition(s)

        Examples:

            Create new type definitions:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.geospatial import FeatureType
                >>> c = CogniteClient()
                >>> feature_types = [
                ...     FeatureType(external_id="wells", properties={"location": {"type": "POINT", "srid": 4326}})
                ...     FeatureType(external_id="pipelines", properties={"location": {"type": "LINESTRING", "srid": 2001}})
                ... ]
                >>> res = c.geospatial.create_feature_types(feature_types)
        """
        return self._create_multiple(
            items=feature_type, cls=FeatureTypeList, resource_path=f"{self._RESOURCE_PATH}/featuretypes"
        )

    def delete_feature_types(self, external_id: Union[str, List[str]], recursive: bool = False) -> None:
        """`Delete one or more feature type`
        <https://docs.cognite.com/api/v1/#operation/GeospatialDeleteFeatureTypes>

        Args:
            external_id (Union[str, List[str]]): External ID or list of external ids
            recursive (bool): if `true` the features will also be dropped

        Returns:
            None

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.geospatial.delete_feature_types(external_id=["wells", "pipelines"])
        """
        extra_body_fields = {"recursive": True} if recursive else {}
        return self._delete_multiple(
            external_ids=external_id,
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
        return self._list(method="POST", cls=FeatureTypeList, resource_path=f"{self._RESOURCE_PATH}/featuretypes")

    def retrieve_feature_types(self, external_id: Union[str, List[str]] = None) -> FeatureTypeList:
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
        return self._retrieve_multiple(
            wrap_ids=True,
            external_ids=external_id,
            cls=FeatureTypeList,
            resource_path=f"{self._RESOURCE_PATH}/featuretypes",
        )

    def update_feature_types(self, update: Union[FeatureTypeUpdate, List[FeatureTypeUpdate]] = None) -> FeatureTypeList:
        """`Patch feature types`
        <https://docs.cognite.com/api/v1/#operation/updateFeatureTypes>

        Args:
            update (Union[FeatureTypeUpdate, List[FeatureTypePatch]]): the update to apply

        Returns:
            FeatureTypeList: The updated feature types.

        Examples:

            Add one property to a feature type:

                >>> from cognite.client.data_classes.geospatial import PropertyAndSearchSpec
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.geospatial.update_feature_types(update=FeatureTypeUpdate(external_id="wells", add=PropertyAndSearchSpec(properties={"altitude": {"type": "DOUBLE"}}, search_spec={"altitude_idx": {"properties": ["altitude"]}})))
        """
        if isinstance(update, FeatureTypeUpdate):
            update = [update]

        def mapper(it):
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

    def create_features(
        self,
        feature_type_external_id: str,
        feature: Union[Feature, List[Feature], FeatureList],
        allow_crs_transformation: bool = False,
    ) -> Union[Feature, FeatureList]:
        """`Creates features`
        <https://docs.cognite.com/api/v1/#operation/createFeatures>

        Args:
            feature_type_external_id : Feature type definition for the features to create.
            feature: one feature or a list of features to create or a FeatureList object
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference
                System defined in the feature type specification. When it is false, then requests with geometries in
                Coordinate Reference System different from the ones defined in the feature type will result in
                CogniteAPIError exception.

        Returns:
            Union[Feature, FeatureList]: Created features

        Examples:

            Create a new feature type and corresponding feature:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> feature_types = [
                ...     FeatureType(external_id="my_feature_type", properties={"location": {"type": "POINT", "srid": 4326}, "temperature": {"type": "DOUBLE"}})
                ... ]
                >>> res = c.geospatial.create_feature_types(feature_types)
                >>> res = c.geospatial.create_features("my_feature_type", Feature(external_id="my_feature", location={"wkt": "POINT(1 1)"}, temperature=12.4))
        """
        if isinstance(feature, FeatureList):
            feature = list(feature)
        resource_path = self._feature_resource_path(feature_type_external_id)
        extra_body_fields = {"allowCrsTransformation": "true"} if allow_crs_transformation else {}
        return self._create_multiple(
            items=feature, resource_path=resource_path, cls=FeatureList, extra_body_fields=extra_body_fields
        )

    def delete_features(self, feature_type_external_id: str, external_id: Union[str, List[str]] = None) -> None:
        """`Delete one or more feature`
        <https://docs.cognite.com/api/v1/#operation/deleteFeatures>

        Args:
            feature_type_external_id : Feature type external id for the features to delete.
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.geospatial.delete_feature("my_feature_type", external_id=my_feature)
        """
        resource_path = self._feature_resource_path(feature_type_external_id)
        self._delete_multiple(external_ids=external_id, wrap_ids=True, resource_path=resource_path)

    def retrieve_features(
        self, feature_type_external_id: str, external_id: Union[str, List[str]] = None
    ) -> FeatureList:
        """`Retrieve features`
        <https://docs.cognite.com/api/v1/#operation/getFeaturesByIds>

        Args:
            feature_type_external_id : Feature type external id for the features to retrieve.
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            FeatureList: Requested features or None if it does not exist.

        Examples:

            Retrieve one feature by its external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.geospatial.retrieve_feature("my_feature_type", external_id="my_feature")
        """
        resource_path = self._feature_resource_path(feature_type_external_id)
        return self._retrieve_multiple(
            wrap_ids=True, external_ids=external_id, resource_path=resource_path, cls=FeatureList
        )

    def update_features(
        self,
        feature_type_external_id: str,
        feature: Union[Feature, List[Feature]],
        allow_crs_transformation: bool = False,
    ) -> FeatureList:
        """`Update features`
        <https://docs.cognite.com/api/v1/#operation/updateFeatures>

        Args:
            feature_type_external_id : Feature type definition for the features to update.
            feature (Union[Feature, List[Feature]]): feature or list of features.
            allow_crs_transformation: If true, then input geometries will be transformed into the Coordinate Reference
                System defined in the feature type specification. When it is false, then requests with geometries in
                Coordinate Reference System different from the ones defined in the feature type will result in
                CogniteAPIError exception.

        Returns:
            FeatureList: Updated features

        Examples:

            Update one feature:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature = c.geospatial.create_features("my_feature_type", Feature(external_id="my_feature", temperature=12.4))
                >>> # do some stuff
                >>> my_updated_feature = c.geospatial.update_features("my_feature_type", Feature(external_id="my_feature", temperature=6.237))
        """
        if isinstance(feature, FeatureList):
            feature = list(feature)
        # updates for feature are not following the patch structure from other resources
        # they are more like a replace so an update looks like a feature creation (yeah, borderline ?)
        resource_path = self._feature_resource_path(feature_type_external_id) + "/update"
        extra_body_fields = {"allowCrsTransformation": "true"} if allow_crs_transformation else {}
        return self._create_multiple(
            feature, resource_path=resource_path, cls=FeatureList, extra_body_fields=extra_body_fields
        )

    def search_features(
        self,
        feature_type_external_id: str,
        filter: Dict[str, Any],
        properties: Dict[str, Any] = None,
        limit: int = 100,
        order_by: List[OrderSpec] = None,
        allow_crs_transformation: bool = False,
    ) -> FeatureList:
        """`Search for features`
        <https://docs.cognite.com/api/v1/#operation/searchFeatures>

        Args:
            feature_type_external_id: the feature type to search for
            filter (Dict[str, Any]): the search filter
            limit (int): maximum number of results
            properties (Dict[str, Any]): the output property selection
            order_by (List[OrderSpec]): the order specification
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
                >>> my_feature_type = c.geospatial.retrieve_feature_types(external_id="my_feature_type")
                >>> my_feature = c.geospatial.create_features(my_feature_type, Feature(external_id="my_feature", temperature=12.4, location={"wkt": "POINT(0 1)"}))
                >>> res = c.geospatial.search_features("my_feature_type", filter={"range": {"property": "temperature", "gt": 12.0}})
                >>> for f in res:
                ...     # do something with the features

            Search for features and select output properties:

                >>> res = c.geospatial.search_features(my_feature_type, filter={}, properties={"temperature": {}, "pressure": {}})

            Search for features and order results:

                >>> res = c.geospatial.search_features(my_feature_type, filter={}, order_by=[OrderSpec("temperature", "ASC"), OrderSpec("pressure", "DESC")])

            Search for features with spatial filters:

                >>> res = c.geospatial.search_features(my_feature_type, filter={"stWithin": {"property": "location", "value": {"wkt": "POLYGON((0 0, 0 1, 1 1, 0 0))"}}})

            Combining multiple filters:

                >>> res = c.geospatial.search_features(my_feature_type, filter={"and": [{"range": {"property": "temperature", "gt": 12.0}}, {"stWithin": {"property": "location", "value": {"wkt": "POLYGON((0 0, 0 1, 1 1, 0 0))"}}}]})
                >>> res = c.geospatial.search_features(my_feature_type, filter={"or": [{"range": {"property": "temperature", "gt": 12.0}}, {"stWithin": {"property": "location", "value": {"wkt": "POLYGON((0 0, 0 1, 1 1, 0 0))"}}}]})


        """
        resource_path = self._feature_resource_path(feature_type_external_id) + "/search"
        cls = FeatureList
        order = None if order_by is None else [f"{item.property}:{item.direction}" for item in order_by]
        res = self._post(
            url_path=resource_path,
            json={
                "filter": filter,
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
        filter: Dict[str, Any],
        properties: Dict[str, Any] = None,
        allow_crs_transformation: bool = False,
    ) -> Generator[Feature, None, None]:
        """`Stream features`
        <https://docs.cognite.com/api/v1/#operation/searchFeaturesStreaming>

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
                >>> my_feature = c.geospatial.create_features("my_feature_type", Feature(external_id="my_feature", temperature=12.4))
                >>> features = c.geospatial.stream_features("my_feature_type", filter={"range": {"property": "temperature", "gt": 12.0}})
                >>> for f in features:
                ...     # do something with the features

            Stream features and select output properties:

                >>> features = c.geospatial.stream_features("my_feature_type", filter={}, properties={"temperature": {}, "pressure": {}})
                >>> for f in features:
                ...     # do something with the features

        """
        resource_path = self._feature_resource_path(feature_type_external_id) + "/search-streaming"
        json = {"filter": filter, "output": {"properties": properties, "jsonStreamFormat": "NEW_LINE_DELIMITED"}}
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
        filter: Dict[str, Any],
        property: str,
        aggregates: List[str],
        group_by: List[str] = None,
    ) -> FeatureAggregateList:
        """`Aggregate filtered features`
        <https://docs.cognite.com/api/v1/#operation/aggregateFeatures>

        Args:
            feature_type_external_id: the feature type to filter features from
            filter (Dict[str, Any]): the search filter
            property (str): the property for which aggregates should be calculated
            aggregates (List[str]): list of aggregates to be calculated
            group_by (List[str]): list of properties to group by with

        Returns:
            FeatureAggregateList: the filtered features

        Examples:

            Aggregate property of features:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature = c.geospatial.create_features("my_feature_type", Feature(external_id="my_feature", temperature=12.4))
                >>> res = c.geospatial.aggregate_features("my_feature_type", filter={"range": {"property": "temperature", "gt": 12.0}}, property="temperature", aggregates=["max", "min"], groupBy=["category"])
                >>> for a in res:
                ...     # loop over aggregates in different groups

        """
        resource_path = self._feature_resource_path(feature_type_external_id) + "/aggregate"
        cls = FeatureAggregateList
        res = self._post(
            url_path=resource_path,
            json={"filter": filter, "property": property, "aggregates": aggregates, "groupBy": group_by},
        )
        return cls._load(res.json()["items"], cognite_client=self._cognite_client)

    def get_coordinate_reference_systems(self, srids: Union[int, List[int]] = None) -> CoordinateReferenceSystemList:
        """`Get Coordinate Reference Systems`
        <https://docs.cognite.com/api/v1/#operation/getCoordinateReferenceSystem>

        Args:
            srids: (Union[int, List[int]]): SRID or list of SRIDs

        Returns:
            CoordinateReferenceSystemList: Requested CRSs.

        Examples:

            Get two CRS definitions:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> crs = c.geospatial.get_coordinate_reference_systems(srids=[4326, 4327])
        """
        if isinstance(srids, numbers.Integral):
            srids = [srids]

        res = self._post(
            url_path=f"{self._RESOURCE_PATH}/crs/byids", json={"items": [{"srid": srid} for srid in srids]}
        )
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def list_coordinate_reference_systems(self, only_custom=False) -> CoordinateReferenceSystemList:
        """`List Coordinate Reference Systems`
        <https://docs.cognite.com/api/v1/#operation/listGeospatialCoordinateReferenceSystems>

        Args:
            only_custom: list only custom CRSs or not

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
        self, crs: Union[CoordinateReferenceSystem, List[CoordinateReferenceSystem]]
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
                >>> custom_crs = CoordinateReferenceSystem(srid = 121111, wkt="wkt", proj_string="proj")
                >>> crs = c.geospatial.create_coordinate_reference_systems(custom_crs)
        """
        if isinstance(crs, CoordinateReferenceSystem):
            crs = [crs]

        res = self._post(
            url_path=f"{self._RESOURCE_PATH}/crs", json={"items": [it.dump(camel_case=True) for it in crs]}
        )
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def delete_coordinate_reference_systems(self, srids: Union[int, List[int]] = None) -> None:
        """`Delete Coordinate Reference System`
        <https://docs.cognite.com/api/v1/#operation/deleteGeospatialCoordinateReferenceSystems>

        Args:
            srids: (Union[int, List[int]]): SRID or list of SRIDs

        Returns:
            None

        Examples:

            Delete a custom CRS:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> crs = c.geospatial.delete_coordinate_reference_systems(srids=[121111])
        """
        if isinstance(srids, numbers.Integral):
            srids = [srids]

        self._post(url_path=f"{self._RESOURCE_PATH}/crs/delete", json={"items": [{"srid": srid} for srid in srids]})
