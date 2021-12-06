import functools
import json as complexjson
import numbers
from typing import Any, Dict, Generator, List, Union

from cognite.client._api_client import APIClient

from cognite.client.data_classes.geospatial import (
    CoordinateReferenceSystem,
    CoordinateReferenceSystemList,
    Feature,
    FeatureList,
    FeatureType,
    FeatureTypeList,
    FeatureTypeUpdate,
)


class GeospatialAPI(APIClient):

    X_COGNITE_DOMAIN = "x-cognite-domain"

    _cognite_domain = None

    @staticmethod
    def _feature_resource_path(feature_type: FeatureType):
        return f"/spatial/featuretypes/{feature_type.external_id}/features"

    def _with_cognite_domain(func):
        @functools.wraps(func)
        def wrapper_with_cognite_domain(self, *args, **kwargs):
            self._config.headers.pop(self.X_COGNITE_DOMAIN, None)
            if self._cognite_domain is not None:
                self._config.headers.update({self.X_COGNITE_DOMAIN: self._cognite_domain})
            res = func(self, *args, **kwargs)
            self._config.headers.pop(self.X_COGNITE_DOMAIN, None)
            return res

        return wrapper_with_cognite_domain

    def set_current_cognite_domain(self, cognite_domain: str):
        self._cognite_domain = cognite_domain

    def get_current_cognite_domain(self):
        return self._cognite_domain

    @_with_cognite_domain
    def create_feature_types(
        self, feature_type: Union[FeatureType, List[FeatureType]]
    ) -> Union[FeatureType, FeatureTypeList]:
        """`Creates feature types`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/createFeatureTypes>

        Args:
            feature_type (Union[FeatureType, List[FeatureType]]): feature type definition or list of feature type definitions to create.

        Returns:
            Union[FeatureType, FeatureTypeList]: Created feature type definition(s)

        Examples:

            Create new type definitions:

                >>> from cognite.experimental import CogniteClient
                >>> from cognite.experimental.data_classes.geospatial import FeatureType
                >>> c = CogniteClient()
                >>> feature_types = [
                ...     FeatureType(external_id="wells", attributes={"location": {"type": "POINT", "srid": 4326}})
                ...     FeatureType(external_id="pipelines", attributes={"location": {"type": "LINESTRING", "srid": 2001}})
                ... ]
                >>> res = c.geospatial.create_feature_types(feature_types)
        """
        return self._create_multiple(items=feature_type, cls=FeatureTypeList, resource_path="/spatial/featuretypes")

    @_with_cognite_domain
    def delete_feature_types(self, external_id: Union[str, List[str]], force: bool = False) -> None:
        """`Delete one or more feature type`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/deleteFeatureTypes>

        Args:
            external_id (Union[str, List[str]]): External ID or list of external ids
            force (bool): if `true` the features will also be dropped

        Returns:
            None

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> c.geospatial.delete_feature_types(external_id=["wells", "pipelines"])
        """
        params = {"force": "true"} if force else None
        return self._delete_multiple(
            external_ids=external_id, wrap_ids=True, resource_path="/spatial/featuretypes", params=params
        )

    @_with_cognite_domain
    def list_feature_types(self) -> FeatureTypeList:
        """`List feature types`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/listFeatureTypes>

        Returns:
            FeatureTypeList: List of feature types

        Examples:

            Iterate over feature type definitions:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> for feature_type in c.geospatial.list_feature_types():
                ...     feature_type # do something with the feature type definition
        """
        return self._list(method="POST", cls=FeatureTypeList, resource_path="/spatial/featuretypes")

    @_with_cognite_domain
    def retrieve_feature_types(self, external_id: Union[str, List[str]] = None) -> FeatureTypeList:
        """`Retrieve feature types`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/getFeatureTypesByIds>

        Args:
            external_id (Union[str, List[str]]): External ID

        Returns:
            FeatureTypeList: Requested Type or None if it does not exist.

        Examples:

            Get Type by external id:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.geospatial.retrieve_feature_types(external_id="1")
        """
        return self._retrieve_multiple(
            wrap_ids=True, external_ids=external_id, cls=FeatureTypeList, resource_path="/spatial/featuretypes"
        )

    @_with_cognite_domain
    def update_feature_types(self, update: Union[FeatureTypeUpdate, List[FeatureTypeUpdate]] = None) -> FeatureTypeList:
        """`Patch feature types`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/updateFeatureType>

        Args:
            update (Union[FeatureTypeUpdate, List[FeatureTypePatch]]): the update to apply

        Returns:
            FeatureTypeList: The updated feature types.

        Examples:

            Add one attribute to a feature type:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.geospatial.update_feature_types(update=FeatureTypeUpdate(external_id="wells", add=AttributeAndSearchSpec(attributes={"altitude": {"type": "DOUBLE"}})))
        """
        if isinstance(update, FeatureTypeUpdate):
            update = [update]

        mapper = lambda it: {
            "attributes": None if not hasattr(it, "attributes") else it.attributes,
            "searchSpec": None if not hasattr(it, "search_spec") else it.search_spec,
        }
        json = {"items": [{"externalId": it.external_id, "add": mapper(it.add)} for it in update]}
        res = self._post(url_path=f"/spatial/featuretypes/update", json=json)
        return FeatureTypeList._load(res.json()["items"], cognite_client=self._cognite_client)

    @_with_cognite_domain
    def create_features(
        self, feature_type: FeatureType, feature: Union[Feature, List[Feature]]
    ) -> Union[Feature, FeatureList]:
        """`Creates features`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/createFeatures>

        Args:
            feature_type : Feature type definition for the features to create.
            feature: one feature or a list of features to create

        Returns:
            Union[Feature, FeatureList]: Created features

        Examples:

            Create a new feature:

                >>> from cognite.experimental import CogniteClient
                >>> from cognite.experimental.data_classes.geospatial import FeatureType
                >>> c = CogniteClient()
                >>> my_feature_type = c.geospatial.retrieve_feature_types(external_id="my_feature_type")
                >>> res = c.geospatial.create_features(my_feature_types, Feature(external_id="my_feature", temperature=12.4))
        """
        resource_path = self._feature_resource_path(feature_type)
        return self._create_multiple(items=feature, resource_path=resource_path, cls=FeatureList)

    @_with_cognite_domain
    def delete_features(self, feature_type: FeatureType, external_id: Union[str, List[str]] = None) -> None:
        """`Delete one or more feature`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/deleteFeatures>

        Args:
            feature_type : Feature type definition for the features to delete.
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete feature type definitions external id:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature_type = c.geospatial.retrieve_feature_types(external_id="my_feature_type")
                >>> c.geospatial.delete_feature(my_feature_type, external_id=my_feature)
        """
        resource_path = self._feature_resource_path(feature_type)
        self._delete_multiple(external_ids=external_id, wrap_ids=True, resource_path=resource_path)

    @_with_cognite_domain
    def retrieve_features(self, feature_type: FeatureType, external_id: Union[str, List[str]] = None) -> FeatureList:
        """`Retrieve features`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/getFeaturesByIds>

        Args:
            feature_type : Feature type definition for the features to retrieve.
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            FeatureList: Requested features or None if it does not exist.

        Examples:

            Retrieve one feature by its external id:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature_type = c.geospatial.retrieve_feature_types(external_id="my_feature_type")
                >>> c.geospatial.retrieve_feature(my_feature_type, external_id="my_feature")
        """
        resource_path = self._feature_resource_path(feature_type)
        return self._retrieve_multiple(
            wrap_ids=True, external_ids=external_id, resource_path=resource_path, cls=FeatureList
        )

    @_with_cognite_domain
    def update_features(self, feature_type: FeatureType, feature: Union[Feature, List[Feature]]) -> FeatureList:
        """`Update features`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/updateFeatures>

        Args:
            feature_type : Feature type definition for the features to update.
            feature (Union[Feature, List[Feature]]): feature or list of features

        Returns:
            FeatureList: Updated features

        Examples:

            Update one feature:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature_type = c.geospatial.retrieve_feature_types(external_id="my_feature_type")
                >>> my_feature = c.geospatial.create_features(my_feature_type, Feature(external_id="my_feature", temperature=12.4))
                >>> # do some stuff
                >>> my_updated_feature = c.geospatial.update_features(my_feature_type, Feature(external_id="my_feature", temperature=6.237))
        """
        # updates for feature are not following the patch structure from other resources
        # they are more like a replace so an update looks like a feature creation (yeah, borderline ?)
        resource_path = self._feature_resource_path(feature_type) + "/update"
        return self._create_multiple(feature, resource_path=resource_path, cls=FeatureList)

    @_with_cognite_domain
    def search_features(
        self, feature_type: FeatureType, filter: Dict[str, Any], attributes: Dict[str, Any] = None, limit: int = 100
    ) -> FeatureList:
        """`Search for features`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/searchFeatures>

        Args:
            feature_type: the feature type to search for
            filter (Dict[str, Any]): the search filter
            limit (int): maximum number of results
            attributes (Dict[str, Any]): the output attribute selection

        Returns:
            FeatureList: the filtered features

        Examples:

            Search for features:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature_type = c.geospatial.retrieve_feature_types(external_id="my_feature_type")
                >>> my_feature = c.geospatial.create_features(my_feature_type, Feature(external_id="my_feature", temperature=12.4))
                >>> res = c.geospatial.search_features(my_feature_type, filter={"range": {"attribute": "temperature", "gt": 12.0}})
                >>> for f in res:
                ...     # do something with the features

            Search for features and select output attributes:

                >>> res = c.geospatial.search_features(my_feature_type, filter={}, attributes={"temperature": {}, "pressure": {}})

        """
        resource_path = self._feature_resource_path(feature_type) + "/search"
        cls = FeatureList
        resource_path = resource_path
        res = self._post(
            url_path=resource_path, json={"filter": filter, "limit": limit, "output": {"attributes": attributes}}
        )
        return cls._load(res.json()["items"], cognite_client=self._cognite_client)

    def stream_features(
        self, feature_type: FeatureType, filter: Dict[str, Any], attributes: Dict[str, Any] = None
    ) -> Generator[Feature, None, None]:
        """`Stream features`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/streamFeatures>

        Args:
            feature_type: the feature type to search for
            filter (Dict[str, Any]): the search filter
            attributes (Dict[str, Any]): the output attribute selection

        Returns:
            Generator[Feature]: a generator for the filtered features

        Examples:

            Stream features:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> my_feature_type = c.geospatial.retrieve_feature_types(external_id="my_feature_type")
                >>> my_feature = c.geospatial.create_features(my_feature_type, Feature(external_id="my_feature", temperature=12.4))
                >>> features = c.geospatial.stream_features(my_feature_type, filter={"range": {"attribute": "temperature", "gt": 12.0}})
                >>> for f in features:
                ...     # do something with the features

            Stream features and select output attributes:

                >>> features = c.geospatial.stream_features(my_feature_type, filter={}, attributes={"temperature": {}, "pressure": {}})
                >>> for f in features:
                ...     # do something with the features

        """
        resource_path = self._feature_resource_path(feature_type) + "/search-streaming"
        resource_path = resource_path
        json = {"filter": filter, "output": {"attributes": attributes, "jsonStreamFormat": "NEW_LINE_DELIMITED"}}

        self._config.headers.pop(self.X_COGNITE_DOMAIN, None)
        if self._cognite_domain is not None:
            self._config.headers.update({self.X_COGNITE_DOMAIN: feature_type._cognite_domain})
        res = self._do_request("POST", url_path=resource_path, json=json, timeout=self._config.timeout, stream=True)
        self._config.headers.pop(self.X_COGNITE_DOMAIN, None)

        for line in res.iter_lines():
            yield Feature._load(complexjson.loads(line))

    def get_coordinate_reference_systems(self, srids: Union[int, List[int]] = None) -> CoordinateReferenceSystemList:
        """`Get Coordinate Reference Systems`
        <https://pr-1323.specs.preview.cogniteapp.com/v1.json.html#operation/getCRS>

        Args:
            srids: (Union[int, List[int]]): SRID or list of SRIDs

        Returns:
            CoordinateReferenceSystemList: Requested CRSs.

        Examples:

            Get two CRS definitions:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> crs = c.geospatial.get_coordinate_reference_systems(srids=[4326, 4327])
        """
        if isinstance(srids, numbers.Integral):
            srids = [srids]

        res = self._post(url_path="/spatial/crs/byids", json={"items": [{"srid": srid} for srid in srids]})
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def list_coordinate_reference_systems(self, only_custom=False) -> CoordinateReferenceSystemList:
        """`Retrieve features`

        Args:
            only_custom: list only custom CRSs or not

        Returns:
            CoordinateReferenceSystemList: list of CRSs.

        Examples:

            Fetch all custom CRSs:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> crs = c.geospatial.list_coordinate_reference_systems(only_custom=True)
        """
        res = self._get(url_path="/spatial/crs/list", params={"filterCustom": only_custom})
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def create_coordinate_reference_systems(
        self, crs: Union[CoordinateReferenceSystem, List[CoordinateReferenceSystem]]
    ) -> CoordinateReferenceSystemList:
        """`Create Coordinate Reference System`

        Args:
            crs: a CoordinateReferenceSystem or a list of CoordinateReferenceSystem

        Returns:
            CoordinateReferenceSystemList: list of CRSs.

        Examples:

            Create a custom CRS:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> custom_crs = CoordinateReferenceSystem(srid = 121111, wkt="wkt", proj_string="proj")
                >>> crs = c.geospatial.create_coordinate_reference_systems(custom_crs)
        """
        if isinstance(crs, CoordinateReferenceSystem):
            crs = [crs]

        res = self._post(url_path="/spatial/crs", json={"items": [it.dump(camel_case=True) for it in crs]})
        return CoordinateReferenceSystemList._load(res.json()["items"], cognite_client=self._cognite_client)

    def delete_coordinate_reference_systems(self, srids: Union[int, List[int]] = None) -> None:
        """`Delete Coordinate Reference System`

        Args:
            srids: (Union[int, List[int]]): SRID or list of SRIDs

        Returns:
            None

        Examples:

            Delete a custom CRS:

                >>> from cognite.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> crs = c.geospatial.delete_coordinate_reference_systems(srids=[121111])
        """
        if isinstance(srids, numbers.Integral):
            srids = [srids]

        self._post(url_path="/spatial/crs/delete", json={"items": [{"srid": srid} for srid in srids]})
