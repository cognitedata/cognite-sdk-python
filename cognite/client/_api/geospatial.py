
import json as complexjson
import numbers
import urllib.parse
import warnings
from requests.exceptions import ChunkedEncodingError
from cognite.client._api_client import APIClient
from cognite.client.data_classes.geospatial import CoordinateReferenceSystem, CoordinateReferenceSystemList, Feature, FeatureAggregateList, FeatureList, FeatureType, FeatureTypeList, FeatureTypePatch, FeatureTypeUpdate, GeospatialComputedResponse, GeospatialComputeFunction, OrderSpec, RasterMetadata
from cognite.client.exceptions import CogniteConnectionError
from cognite.client.utils._identifier import IdentifierSequence

class GeospatialAPI(APIClient):
    _RESOURCE_PATH = '/geospatial'

    @staticmethod
    def _feature_resource_path(feature_type_external_id):
        return f'{GeospatialAPI._RESOURCE_PATH}/featuretypes/{feature_type_external_id}/features'

    @staticmethod
    def _raster_resource_path(feature_type_external_id, feature_external_id, raster_property_name):
        encoded_feature_external_id = urllib.parse.quote(feature_external_id, safe='')
        encoded_raster_property_name = urllib.parse.quote(raster_property_name, safe='')
        return (GeospatialAPI._feature_resource_path(feature_type_external_id) + f'/{encoded_feature_external_id}/rasters/{encoded_raster_property_name}')

    @staticmethod
    def _compute_path():
        return f'{GeospatialAPI._RESOURCE_PATH}/compute'

    @overload
    def create_feature_types(self, feature_type):
        ...

    @overload
    def create_feature_types(self, feature_type):
        ...

    def create_feature_types(self, feature_type):
        return self._create_multiple(list_cls=FeatureTypeList, resource_cls=FeatureType, items=feature_type, resource_path=f'{self._RESOURCE_PATH}/featuretypes')

    def delete_feature_types(self, external_id, recursive=False):
        extra_body_fields = ({'recursive': True} if recursive else {})
        return self._delete_multiple(identifiers=IdentifierSequence.load(external_ids=external_id), wrap_ids=True, resource_path=f'{self._RESOURCE_PATH}/featuretypes', extra_body_fields=extra_body_fields)

    def list_feature_types(self):
        return self._list(list_cls=FeatureTypeList, resource_cls=FeatureType, method='POST', resource_path=f'{self._RESOURCE_PATH}/featuretypes')

    @overload
    def retrieve_feature_types(self, external_id):
        ...

    @overload
    def retrieve_feature_types(self, external_id):
        ...

    def retrieve_feature_types(self, external_id):
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_id)
        return self._retrieve_multiple(list_cls=FeatureTypeList, resource_cls=FeatureType, identifiers=(identifiers.as_singleton() if identifiers.is_singleton() else identifiers), resource_path=f'{self._RESOURCE_PATH}/featuretypes')

    def update_feature_types(self, update):
        warnings.warn('update_feature_types is deprecated, use patch_feature_types instead.', DeprecationWarning)
        if isinstance(update, FeatureTypeUpdate):
            update = [update]

        def mapper(it: FeatureTypeUpdate) -> Dict[(str, Any)]:
            add_properties = (it.add.properties if hasattr(it, 'add') else None)
            remove_properties = (it.remove.properties if hasattr(it, 'remove') else None)
            add_search_spec = (it.add.search_spec if hasattr(it, 'add') else None)
            remove_search_spec = (it.remove.search_spec if hasattr(it, 'remove') else None)
            properties_update = {'add': add_properties, 'remove': remove_properties}
            search_spec_update = {'add': add_search_spec, 'remove': remove_search_spec}
            return {'properties': properties_update, 'searchSpec': search_spec_update}
        json = {'items': [{'externalId': it.external_id, 'update': mapper(it)} for it in update]}
        res = self._post(url_path=f'{self._RESOURCE_PATH}/featuretypes/update', json=json)
        return FeatureTypeList._load(res.json()['items'], cognite_client=self._cognite_client)

    def patch_feature_types(self, patch):
        if isinstance(patch, FeatureTypePatch):
            patch = [patch]
        json = {'items': [{'externalId': it.external_id, 'update': {'properties': it.property_patches, 'searchSpec': it.search_spec_patches}} for it in patch]}
        res = self._post(url_path=f'{self._RESOURCE_PATH}/featuretypes/update', json=json)
        return FeatureTypeList._load(res.json()['items'], cognite_client=self._cognite_client)

    @overload
    def create_features(self, feature_type_external_id, feature, allow_crs_transformation=False, chunk_size=None):
        ...

    @overload
    def create_features(self, feature_type_external_id, feature, allow_crs_transformation=False, chunk_size=None):
        ...

    def create_features(self, feature_type_external_id, feature, allow_crs_transformation=False, chunk_size=None):
        if ((chunk_size is not None) and ((chunk_size < 1) or (chunk_size > self._CREATE_LIMIT))):
            raise ValueError(f'The chunk_size must be strictly positive and not exceed {self._CREATE_LIMIT}')
        if isinstance(feature, FeatureList):
            feature = list(feature)
        resource_path = self._feature_resource_path(feature_type_external_id)
        extra_body_fields = ({'allowCrsTransformation': 'true'} if allow_crs_transformation else {})
        return self._create_multiple(list_cls=FeatureList, resource_cls=Feature, items=feature, resource_path=resource_path, extra_body_fields=extra_body_fields, limit=chunk_size)

    def delete_features(self, feature_type_external_id, external_id=None):
        resource_path = self._feature_resource_path(feature_type_external_id)
        self._delete_multiple(identifiers=IdentifierSequence.load(external_ids=external_id), resource_path=resource_path, wrap_ids=True)

    @overload
    def retrieve_features(self, feature_type_external_id, external_id, properties=None):
        ...

    @overload
    def retrieve_features(self, feature_type_external_id, external_id, properties=None):
        ...

    def retrieve_features(self, feature_type_external_id, external_id, properties=None):
        resource_path = self._feature_resource_path(feature_type_external_id)
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_id)
        return self._retrieve_multiple(list_cls=FeatureList, resource_cls=Feature, identifiers=(identifiers.as_singleton() if identifiers.is_singleton() else identifiers), resource_path=resource_path, other_params={'output': {'properties': properties}})

    def update_features(self, feature_type_external_id, feature, allow_crs_transformation=False, chunk_size=None):
        if ((chunk_size is not None) and ((chunk_size < 1) or (chunk_size > self._UPDATE_LIMIT))):
            raise ValueError(f'The chunk_size must be strictly positive and not exceed {self._UPDATE_LIMIT}')
        if isinstance(feature, FeatureList):
            feature = list(feature)
        resource_path = (self._feature_resource_path(feature_type_external_id) + '/update')
        extra_body_fields = ({'allowCrsTransformation': 'true'} if allow_crs_transformation else {})
        return cast(FeatureList, self._create_multiple(list_cls=FeatureList, resource_cls=Feature, items=feature, resource_path=resource_path, extra_body_fields=extra_body_fields, limit=chunk_size))

    def list_features(self, feature_type_external_id, filter=None, properties=None, limit=100, allow_crs_transformation=False):
        return self._list(list_cls=FeatureList, resource_cls=Feature, resource_path=self._feature_resource_path(feature_type_external_id), method='POST', limit=limit, filter=filter, other_params={'allowCrsTransformation': (True if allow_crs_transformation else None), 'output': {'properties': properties}})

    def search_features(self, feature_type_external_id, filter=None, properties=None, limit=100, order_by=None, allow_crs_transformation=False):
        resource_path = (self._feature_resource_path(feature_type_external_id) + '/search')
        cls = FeatureList
        order = (None if (order_by is None) else [f'{item.property}:{item.direction}' for item in order_by])
        res = self._post(url_path=resource_path, json={'filter': (filter or {}), 'limit': limit, 'output': {'properties': properties}, 'sort': order, 'allowCrsTransformation': (True if allow_crs_transformation else None)})
        return cls._load(res.json()['items'], cognite_client=self._cognite_client)

    def stream_features(self, feature_type_external_id, filter=None, properties=None, allow_crs_transformation=False):
        resource_path = (self._feature_resource_path(feature_type_external_id) + '/search-streaming')
        json = {'filter': (filter or {}), 'output': {'properties': properties, 'jsonStreamFormat': 'NEW_LINE_DELIMITED'}}
        params = ({'allowCrsTransformation': 'true'} if allow_crs_transformation else None)
        res = self._do_request('POST', url_path=resource_path, json=json, timeout=self._config.timeout, stream=True, params=params)
        try:
            for line in res.iter_lines():
                (yield Feature._load(complexjson.loads(line)))
        except (ChunkedEncodingError, ConnectionError) as e:
            raise CogniteConnectionError(e)

    def aggregate_features(self, feature_type_external_id, property=None, aggregates=None, filter=None, group_by=None, order_by=None, output=None):
        if (property or aggregates):
            warnings.warn('property and aggregates are deprecated, use output instead.', DeprecationWarning)
        resource_path = (self._feature_resource_path(feature_type_external_id) + '/aggregate')
        cls = FeatureAggregateList
        order = (None if (order_by is None) else [f'{item.property}:{item.direction}' for item in order_by])
        res = self._post(url_path=resource_path, json={'filter': (filter or {}), 'property': property, 'aggregates': aggregates, 'groupBy': group_by, 'sort': order, 'output': output})
        return cls._load(res.json()['items'], cognite_client=self._cognite_client)

    def get_coordinate_reference_systems(self, srids):
        if isinstance(srids, (int, numbers.Integral)):
            srids_processed: Sequence[Union[(numbers.Integral, int)]] = [srids]
        else:
            srids_processed = srids
        res = self._post(url_path=f'{self._RESOURCE_PATH}/crs/byids', json={'items': [{'srid': srid} for srid in srids_processed]})
        return CoordinateReferenceSystemList._load(res.json()['items'], cognite_client=self._cognite_client)

    def list_coordinate_reference_systems(self, only_custom=False):
        res = self._get(url_path=f'{self._RESOURCE_PATH}/crs', params={'filterCustom': only_custom})
        return CoordinateReferenceSystemList._load(res.json()['items'], cognite_client=self._cognite_client)

    def create_coordinate_reference_systems(self, crs):
        if isinstance(crs, CoordinateReferenceSystem):
            crs = [crs]
        res = self._post(url_path=f'{self._RESOURCE_PATH}/crs', json={'items': [it.dump(camel_case=True) for it in crs]})
        return CoordinateReferenceSystemList._load(res.json()['items'], cognite_client=self._cognite_client)

    def delete_coordinate_reference_systems(self, srids):
        if isinstance(srids, (int, numbers.Integral)):
            srids_processed: Sequence[Union[(numbers.Integral, int)]] = [srids]
        else:
            srids_processed = srids
        self._post(url_path=f'{self._RESOURCE_PATH}/crs/delete', json={'items': [{'srid': srid} for srid in srids_processed]})

    def put_raster(self, feature_type_external_id, feature_external_id, raster_property_name, raster_format, raster_srid, file, allow_crs_transformation=False, raster_scale_x=None, raster_scale_y=None):
        query_params = f'format={raster_format}&srid={raster_srid}'
        if allow_crs_transformation:
            query_params += '&allowCrsTransformation=true'
        if raster_scale_x:
            query_params += f'&scaleX={raster_scale_x}'
        if raster_scale_y:
            query_params += f'&scaleY={raster_scale_y}'
        url_path = (self._raster_resource_path(feature_type_external_id, feature_external_id, raster_property_name) + f'?{query_params}')
        res = self._do_request('PUT', url_path, data=open(file, 'rb').read(), headers={'Content-Type': 'application/binary'}, timeout=self._config.timeout)
        return RasterMetadata._load(res.json(), cognite_client=self._cognite_client)

    def delete_raster(self, feature_type_external_id, feature_external_id, raster_property_name):
        url_path = (self._raster_resource_path(feature_type_external_id, feature_external_id, raster_property_name) + '/delete')
        self._do_request('POST', url_path, timeout=self._config.timeout)

    def get_raster(self, feature_type_external_id, feature_external_id, raster_property_name, raster_format, raster_options=None, raster_srid=None, raster_scale_x=None, raster_scale_y=None, allow_crs_transformation=False):
        url_path = self._raster_resource_path(feature_type_external_id, feature_external_id, raster_property_name)
        res = self._do_request('POST', url_path, timeout=self._config.timeout, json={'format': raster_format, 'options': raster_options, 'allowCrsTransformation': (True if allow_crs_transformation else None), 'srid': raster_srid, 'scaleX': raster_scale_x, 'scaleY': raster_scale_y})
        return res.content

    def compute(self, output):
        url_path = self._compute_path()
        res = self._do_request('POST', url_path, timeout=self._config.timeout, json={'output': {k: v.to_json_payload() for (k, v) in output.items()}})
        json = res.json()
        return GeospatialComputedResponse._load(json, cognite_client=self._cognite_client)
