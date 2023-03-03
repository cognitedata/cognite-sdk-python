from cognite.client._api.transformations.jobs import TransformationJobsAPI
from cognite.client._api.transformations.notifications import TransformationNotificationsAPI
from cognite.client._api.transformations.schedules import TransformationSchedulesAPI
from cognite.client._api.transformations.schema import TransformationSchemaAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Transformation, TransformationJob, TransformationList
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.transformations import (
    NonceCredentials,
    TagsFilter,
    TransformationFilter,
    TransformationPreviewResult,
    TransformationUpdate,
)
from cognite.client.utils._identifier import IdentifierSequence


class TransformationsAPI(APIClient):
    _RESOURCE_PATH = "/transformations"
    _LIST_CLASS = TransformationList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jobs = TransformationJobsAPI(*args, **kwargs)
        self.schedules = TransformationSchedulesAPI(*args, **kwargs)
        self.schema = TransformationSchemaAPI(*args, **kwargs)
        self.notifications = TransformationNotificationsAPI(*args, **kwargs)

    def create(self, transformation):
        if isinstance(transformation, Sequence):
            sessions: Dict[(str, NonceCredentials)] = {}
            transformation = [t.copy() for t in transformation]
            for t in transformation:
                t._cognite_client = self._cognite_client
                t._process_credentials(sessions_cache=sessions)
        elif isinstance(transformation, Transformation):
            transformation = transformation.copy()
            transformation._cognite_client = self._cognite_client
            transformation._process_credentials()
        else:
            raise TypeError("transformation must be Sequence[Transformation] or Transformation")
        return self._create_multiple(list_cls=TransformationList, resource_cls=Transformation, items=transformation)

    def delete(self, id=None, external_id=None, ignore_unknown_ids=False):
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    def list(
        self,
        include_public=True,
        name_regex=None,
        query_regex=None,
        destination_type=None,
        conflict_mode=None,
        cdf_project_name=None,
        has_blocked_error=None,
        created_time=None,
        last_updated_time=None,
        data_set_ids=None,
        data_set_external_ids=None,
        tags=None,
        limit=25,
    ):
        ds_ids: Optional[List[Dict[(str, Any)]]] = None
        if data_set_ids and data_set_external_ids:
            ds_ids = [*[{"id": i} for i in data_set_ids], *[{"externalId": i} for i in data_set_external_ids]]
        elif data_set_ids:
            ds_ids = [{"id": i} for i in data_set_ids]
        elif data_set_external_ids:
            ds_ids = [{"externalId": i} for i in data_set_external_ids]
        filter = TransformationFilter(
            include_public=include_public,
            name_regex=name_regex,
            query_regex=query_regex,
            destination_type=destination_type,
            conflict_mode=conflict_mode,
            cdf_project_name=cdf_project_name,
            has_blocked_error=has_blocked_error,
            created_time=created_time,
            last_updated_time=last_updated_time,
            tags=tags,
            data_set_ids=ds_ids,
        ).dump(camel_case=True)
        return self._list(
            list_cls=TransformationList,
            resource_cls=Transformation,
            method="POST",
            url_path=f"{self._RESOURCE_PATH}/filter",
            limit=limit,
            filter=filter,
        )

    def retrieve(self, id=None, external_id=None):
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=TransformationList, resource_cls=Transformation, identifiers=identifiers
        )

    def retrieve_multiple(self, ids=None, external_ids=None, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TransformationList,
            resource_cls=Transformation,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def update(self, item):
        if isinstance(item, Sequence):
            item = list(item).copy()
            sessions: Dict[(str, NonceCredentials)] = {}
            for (i, t) in enumerate(item):
                if isinstance(t, Transformation):
                    t = t.copy()
                    item[i] = t
                    t._cognite_client = self._cognite_client
                    t._process_credentials(sessions_cache=sessions, keep_none=True)
        elif isinstance(item, Transformation):
            item = item.copy()
            item._cognite_client = self._cognite_client
            item._process_credentials(keep_none=True)
        elif not isinstance(item, TransformationUpdate):
            raise TypeError(
                "item must be Sequence[Transformation], Transformation, Sequence[TransformationUpdate] or TransformationUpdate"
            )
        return self._update_multiple(
            list_cls=TransformationList, resource_cls=Transformation, update_cls=TransformationUpdate, items=item
        )

    def run(self, transformation_id=None, transformation_external_id=None, wait=True, timeout=None):
        IdentifierSequence.load(transformation_id, transformation_external_id).assert_singleton()
        id = {"externalId": transformation_external_id, "id": transformation_id}
        response = self._post(json=id, url_path=(self._RESOURCE_PATH + "/run"))
        job = TransformationJob._load(response.json(), cognite_client=self._cognite_client)
        if wait:
            return job.wait(timeout=timeout)
        return job

    def run_async(self, transformation_id=None, transformation_external_id=None, timeout=None):
        job = self.run(
            transformation_id=transformation_id, transformation_external_id=transformation_external_id, wait=False
        )
        return job.wait_async(timeout=timeout)

    def cancel(self, transformation_id=None, transformation_external_id=None):
        IdentifierSequence.load(transformation_id, transformation_external_id).assert_singleton()
        id = {"externalId": transformation_external_id, "id": transformation_id}
        self._post(json=id, url_path=(self._RESOURCE_PATH + "/cancel"))

    def preview(self, query=None, convert_to_string=False, limit=100, source_limit=100, infer_schema_limit=1000):
        request_body = {
            "query": query,
            "convertToString": convert_to_string,
            "limit": limit,
            "sourceLimit": source_limit,
            "inferSchemaLimit": infer_schema_limit,
        }
        response = self._post(url_path=(self._RESOURCE_PATH + "/query/run"), json=request_body)
        result = TransformationPreviewResult._load(response.json(), cognite_client=self._cognite_client)
        return result
