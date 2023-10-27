from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from cognite.client._api.transformations.jobs import TransformationJobsAPI
from cognite.client._api.transformations.notifications import TransformationNotificationsAPI
from cognite.client._api.transformations.schedules import TransformationSchedulesAPI
from cognite.client._api.transformations.schema import TransformationSchemaAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import Transformation, TransformationJob, TransformationList
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.transformations import (
    TagsFilter,
    TransformationFilter,
    TransformationPreviewResult,
    TransformationUpdate,
)
from cognite.client.data_classes.transformations.common import NonceCredentials
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig

__all__ = [
    "TransformationSchemaAPI",
    "TransformationsAPI",
    "TransformationSchedulesAPI",
    "TransformationNotificationsAPI",
    "TransformationJobsAPI",
]


class TransformationsAPI(APIClient):
    _RESOURCE_PATH = "/transformations"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.jobs = TransformationJobsAPI(config, api_version, cognite_client)
        self.schedules = TransformationSchedulesAPI(config, api_version, cognite_client)
        self.schema = TransformationSchemaAPI(config, api_version, cognite_client)
        self.notifications = TransformationNotificationsAPI(config, api_version, cognite_client)

    def create(self, transformation: Transformation | Sequence[Transformation]) -> Transformation | TransformationList:
        """`Create one or more transformations. <https://developer.cognite.com/api#tag/Transformations/operation/createTransformations>`_

        Args:
            transformation (Transformation | Sequence[Transformation]): Transformation or list of transformations to create.

        Returns:
            Transformation | TransformationList: Created transformation(s)

        Examples:

            Create new transformations:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Transformation, TransformationDestination
                >>> from cognite.client.data_classes.transformations.common import ViewInfo, EdgeType, DataModelInfo
                >>> c = CogniteClient()
                >>> transformations = [
                >>>     Transformation(
                >>>         name="transformation1",
                >>>         destination=TransformationDestination.assets()
                >>>     ),
                >>>     Transformation(
                >>>         name="transformation2",
                >>>         destination=TransformationDestination.raw("myDatabase", "myTable")
                >>>     ),
                >>>      Transformation(
                >>>      name="transformation3",
                >>>      view = ViewInfo(space="TypeSpace", external_id="TypeExtId", version="version"),
                >>>      destination=TransformationDestination.nodes(view, "InstanceSpace")
                >>>      ),
                >>>      Transformation(
                >>>      name="transformation4",
                >>>      view = ViewInfo(space="TypeSpace", external_id="TypeExtId", version="version"),
                >>>      destination=TransformationDestination.edges(view, "InstanceSpace")
                >>>      ),
                >>>      Transformation(
                >>>      name="transformation5",
                >>>      edge_type = EdgeType(space="TypeSpace", external_id="TypeExtId"),
                >>>      destination=TransformationDestination.edges(edge_type,"InstanceSpace")
                >>>      ),
                >>>      Transformation(
                >>>      name="transformation6",
                >>>      data_model = DataModelInfo(space="modelSpace", external_id="modelExternalId",version="modelVersion",destination_type="viewExternalId"),
                >>>      destination=TransformationDestination.instances(data_model,"InstanceSpace")
                >>>      ),
                >>>      Transformation(
                >>>      name="transformation7",
                >>>      data_model = DataModelInfo(space="modelSpace", external_id="modelExternalId",version="modelVersion",destination_type="viewExternalId", destination_relationship_from_type="connectionPropertyName"),
                >>>      destination=TransformationDestination.instances(data_model,"InstanceSpace")
                >>>      ),
                >>> ]
                >>> res = c.transformations.create(transformations)

        """
        if isinstance(transformation, Sequence):
            sessions: dict[str, NonceCredentials] = {}
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

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more transformations. <https://developer.cognite.com/api#tag/Transformations/operation/deleteTransformations>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids.
            external_id (str | Sequence[str] | None): External ID or list of external ids.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Example:

            Delete transformations by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.transformations.delete(id=[1,2,3], external_id="function3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    def list(
        self,
        include_public: bool = True,
        name_regex: str | None = None,
        query_regex: str | None = None,
        destination_type: str | None = None,
        conflict_mode: str | None = None,
        cdf_project_name: str | None = None,
        has_blocked_error: bool | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        data_set_ids: list[int] | None = None,
        data_set_external_ids: list[str] | None = None,
        tags: TagsFilter | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TransformationList:
        """`List all transformations. <https://developer.cognite.com/api#tag/Transformations/operation/getTransformations>`_

        Args:
            include_public (bool): Whether public transformations should be included in the results. (default true).
            name_regex (str | None): Regex expression to match the transformation name
            query_regex (str | None): Regex expression to match the transformation query
            destination_type (str | None): Transformation destination resource name to filter by.
            conflict_mode (str | None): Filters by a selected transformation action type: abort/create, upsert, update, delete
            cdf_project_name (str | None): Project name to filter by configured source and destination project
            has_blocked_error (bool | None): Whether only the blocked transformations should be included in the results.
            created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            data_set_ids (list[int] | None): Return only transformations in the specified data sets with these ids.
            data_set_external_ids (list[str] | None): Return only transformations in the specified data sets with these external ids.
            tags (TagsFilter | None): Return only the resource matching the specified tags constraints. It only supports ContainsAny as of now.
            limit (int | None): Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.

        Returns:
            TransformationList: List of transformations

        Example:

            List transformations::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> transformations_list = c.transformations.list()
        """
        ds_ids: list[dict[str, Any]] | None = None
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

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> Transformation | None:
        """`Retrieve a single transformation by id. <https://developer.cognite.com/api#tag/Transformations/operation/getTransformationsByIds>`_

        Args:
            id (int | None): ID
            external_id (str | None): No description.

        Returns:
            Transformation | None: Requested transformation or None if it does not exist.

        Examples:

            Get transformation by id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.retrieve(id=1)

            Get transformation by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=TransformationList,
            resource_cls=Transformation,
            identifiers=identifiers,
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> TransformationList:
        """`Retrieve multiple transformations. <https://developer.cognite.com/api#tag/Transformations/operation/getTransformationsByIds>`_

        Args:
            ids (Sequence[int] | None): List of ids to retrieve.
            external_ids (Sequence[str] | None): List of external ids to retrieve.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            TransformationList: Requested transformation or None if it does not exist.

        Examples:

            Get multiple transformations:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.transformations.retrieve_multiple(ids=[1,2,3], external_ids=['transform-1','transform-2'])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TransformationList,
            resource_cls=Transformation,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def update(
        self, item: Transformation | TransformationUpdate | Sequence[Transformation | TransformationUpdate]
    ) -> Transformation | TransformationList:
        """`Update one or more transformations <https://developer.cognite.com/api#tag/Transformations/operation/updateTransformations>`_

        Args:
            item (Transformation | TransformationUpdate | Sequence[Transformation | TransformationUpdate]): Transformation(s) to update

        Returns:
            Transformation | TransformationList: Updated transformation(s)

        Examples:

            Update a transformation that you have fetched. This will perform a full update of the transformation::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> transformation = c.transformations.retrieve(id=1)
                >>> transformation.query = "SELECT * FROM _cdf.assets"
                >>> res = c.transformations.update(transformation)

            Perform a partial update on a transformation, updating the query and making it private::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationUpdate
                >>> c = CogniteClient()
                >>> my_update = TransformationUpdate(id=1).query.set("SELECT * FROM _cdf.assets").is_public.set(False)
                >>> res = c.transformations.update(my_update)
        """

        if isinstance(item, Sequence):
            item = list(item).copy()
            sessions: dict[str, NonceCredentials] = {}
            for i, t in enumerate(item):
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

    def run(
        self,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        wait: bool = True,
        timeout: float | None = None,
    ) -> TransformationJob:
        """`Run a transformation. <https://developer.cognite.com/api#tag/Transformations/operation/runTransformation>`_

        Args:
            transformation_id (int | None): Transformation internal id
            transformation_external_id (str | None): Transformation external id
            wait (bool): Wait until the transformation run is finished. Defaults to True.
            timeout (float | None): maximum time (s) to wait, default is None (infinite time). Once the timeout is reached, it returns with the current status. Won't have any effect if wait is False.

        Returns:
            TransformationJob: Created transformation job

        Examples:

            Run transformation to completion by id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>>
                >>> res = c.transformations.run(transformation_id = 1)

            Start running transformation by id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>>
                >>> res = c.transformations.run(transformation_id = 1, wait = False)
        """
        IdentifierSequence.load(transformation_id, transformation_external_id).assert_singleton()

        id = {"externalId": transformation_external_id, "id": transformation_id}

        response = self._post(json=id, url_path=self._RESOURCE_PATH + "/run")
        job = TransformationJob._load(response.json(), cognite_client=self._cognite_client)

        if wait:
            return job.wait(timeout=timeout)

        return job

    async def run_async(
        self,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        timeout: float | None = None,
    ) -> TransformationJob:
        """`Run a transformation to completion asynchronously. <https://developer.cognite.com/api#tag/Transformations/operation/runTransformation>`_

        Args:
            transformation_id (int | None): internal Transformation id
            transformation_external_id (str | None): external Transformation id
            timeout (float | None): maximum time (s) to wait, default is None (infinite time). Once the timeout is reached, it returns with the current status.

        Returns:
            TransformationJob: Completed (if finished) or running (if timeout reached) transformation job.

        Examples:

            Run transformation asynchronously by id:

                >>> import asyncio
                >>> from cognite.client import CogniteClient
                >>>
                >>> c = CogniteClient()
                >>>
                >>> async def run_transformation():
                >>>     res = await c.transformations.run_async(id = 1)
                >>>
                >>> loop = asyncio.get_event_loop()
                >>> loop.run_until_complete(run_transformation())
                >>> loop.close()
        """

        job = self.run(
            transformation_id=transformation_id, transformation_external_id=transformation_external_id, wait=False
        )
        return await job.wait_async(timeout=timeout)

    def cancel(self, transformation_id: int | None = None, transformation_external_id: str | None = None) -> None:
        """`Cancel a running transformation. <https://developer.cognite.com/api#tag/Transformations/operation/postApiV1ProjectsProjectTransformationsCancel>`_

        Args:
            transformation_id (int | None): Transformation internal id
            transformation_external_id (str | None): Transformation external id

        Examples:

            Wait transformation for 1 minute and cancel if still running:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationJobStatus
                >>> c = CogniteClient()
                >>>
                >>> res = c.transformations.run(id = 1, timeout = 60.0)
                >>> if res.status == TransformationJobStatus.RUNNING:
                >>>     res.cancel()
        """
        IdentifierSequence.load(transformation_id, transformation_external_id).assert_singleton()

        id = {"externalId": transformation_external_id, "id": transformation_id}

        self._post(json=id, url_path=self._RESOURCE_PATH + "/cancel")

    def preview(
        self,
        query: str | None = None,
        convert_to_string: bool = False,
        limit: int = 100,
        source_limit: int | None = 100,
        infer_schema_limit: int | None = 1000,
    ) -> TransformationPreviewResult:
        """`Preview the result of a query. <https://developer.cognite.com/api#tag/Query/operation/runPreview>`_

        Args:
            query (str | None): SQL query to run for preview.
            convert_to_string (bool): Stringify values in the query results, default is False.
            limit (int): Maximum number of rows to return in the final result, default is 100.
            source_limit (int | None): Maximum number of items to read from the data source or None to run without limit, default is 100.
            infer_schema_limit (int | None): Limit for how many rows that are used for inferring result schema, default is 1000.

        Returns:
            TransformationPreviewResult: Result of the executed query

        Examples:

            Preview transformation results as schema and list of rows:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>>
                >>> query_result = c.transformations.preview(query="select * from _cdf.assets")

            Preview transformation results as pandas dataframe:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>>
                >>> df = c.transformations.preview(query="select * from _cdf.assets").to_pandas()
        """
        request_body = {
            "query": query,
            "convertToString": convert_to_string,
            "limit": limit,
            "sourceLimit": source_limit,
            "inferSchemaLimit": infer_schema_limit,
        }

        response = self._post(url_path=self._RESOURCE_PATH + "/query/run", json=request_body)
        result = TransformationPreviewResult._load(response.json(), cognite_client=self._cognite_client)

        return result
