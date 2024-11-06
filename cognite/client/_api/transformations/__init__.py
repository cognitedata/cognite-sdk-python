from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

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
    TransformationWrite,
)
from cognite.client.data_classes.transformations.common import NonceCredentials
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

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

    @overload
    def __call__(
        self,
        chunk_size: None = None,
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
        limit: int | None = None,
    ) -> Iterator[Transformation]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
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
        limit: int | None = None,
    ) -> Iterator[TransformationList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        include_public: bool = True,
        name_regex: str | None = None,
        query_regex: str | None = None,
        destination_type: str | None = None,
        conflict_mode: str | None = None,
        cdf_project_name: str | None = None,
        has_blocked_error: bool | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        data_set_ids: int | list[int] | None = None,
        data_set_external_ids: str | list[str] | None = None,
        tags: TagsFilter | None = None,
        limit: int | None = None,
    ) -> Iterator[Transformation] | Iterator[TransformationList]:
        """Iterate over transformations

        Args:
            chunk_size (int | None): Number of transformations to return in each chunk. Defaults to yielding one transformation a time.
            include_public (bool): Whether public transformations should be included in the results. (default true).
            name_regex (str | None): Regex expression to match the transformation name
            query_regex (str | None): Regex expression to match the transformation query
            destination_type (str | None): Transformation destination resource name to filter by.
            conflict_mode (str | None): Filters by a selected transformation action type: abort/create, upsert, update, delete
            cdf_project_name (str | None): Project name to filter by configured source and destination project
            has_blocked_error (bool | None): Whether only the blocked transformations should be included in the results.
            created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            data_set_ids (int | list[int] | None): Return only transformations in the specified data sets with these id(s).
            data_set_external_ids (str | list[str] | None): Return only transformations in the specified data sets with these external id(s).
            tags (TagsFilter | None): Return only the resource matching the specified tags constraints. It only supports ContainsAny as of now.
            limit (int | None): Limits the number of results to be returned. Defaults to yielding all transformations.

        Returns:
            Iterator[Transformation] | Iterator[TransformationList]: Yields transformations in chunks if chunk_size is specified, otherwise one transformation at a time.
        """
        ds_ids = IdentifierSequence.load(data_set_ids, data_set_external_ids, id_name="data_set").as_dicts()

        filter_ = TransformationFilter(
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
            data_set_ids=ds_ids or None,
        ).dump(camel_case=True)

        return self._list_generator(
            method="POST",
            url_path=f"{self._RESOURCE_PATH}/filter",
            limit=limit,
            chunk_size=chunk_size,
            filter=filter_,
            resource_cls=Transformation,
            list_cls=TransformationList,
        )

    def __iter__(self) -> Iterator[Transformation]:
        """Iterate over all transformations"""
        return self()

    @overload
    def create(self, transformation: Transformation | TransformationWrite) -> Transformation: ...

    @overload
    def create(
        self, transformation: Sequence[Transformation] | Sequence[TransformationWrite]
    ) -> TransformationList: ...

    def create(
        self,
        transformation: Transformation | TransformationWrite | Sequence[Transformation] | Sequence[TransformationWrite],
    ) -> Transformation | TransformationList:
        """`Create one or more transformations. <https://developer.cognite.com/api#tag/Transformations/operation/createTransformations>`_

        Args:
            transformation (Transformation | TransformationWrite | Sequence[Transformation] | Sequence[TransformationWrite]): Transformation or list of transformations to create.

        Returns:
            Transformation | TransformationList: Created transformation(s)

        Examples:

            Create new transformations:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationWrite, TransformationDestination
                >>> from cognite.client.data_classes.transformations.common import ViewInfo, EdgeType, DataModelInfo
                >>> client = CogniteClient()
                >>> transformations = [
                >>>     TransformationWrite(
                >>>         external_id="transformation1",
                >>>         name="transformation1",
                >>>         ignore_null_fields=False,
                >>>         destination=TransformationDestination.assets()
                >>>     ),
                >>>     TransformationWrite(
                >>>         external_id="transformation2",
                >>>         name="transformation2",
                >>>         ignore_null_fields=False,
                >>>         destination=TransformationDestination.raw("myDatabase", "myTable")
                >>>     ),
                >>>      TransformationWrite(
                >>>          external_id="transformation3",
                >>>          name="transformation3",
                >>>          ignore_null_fields=False,
                >>>          view = ViewInfo(space="TypeSpace", external_id="TypeExtId", version="version"),
                >>>          destination=TransformationDestination.nodes(view, "InstanceSpace")
                >>>      ),
                >>>      TransformationWrite(
                >>>          external_id="transformation4",
                >>>          name="transformation4",
                >>>          ignore_null_fields=False,
                >>>          view = ViewInfo(space="TypeSpace", external_id="TypeExtId", version="version"),
                >>>          destination=TransformationDestination.edges(view, "InstanceSpace")
                >>>      ),
                >>>      TransformationWrite(
                >>>          external_id="transformation5",
                >>>          name="transformation5",
                >>>          ignore_null_fields=False,
                >>>          edge_type = EdgeType(space="TypeSpace", external_id="TypeExtId"),
                >>>          destination=TransformationDestination.edges(edge_type,"InstanceSpace")
                >>>      ),
                >>>      TransformationWrite(
                >>>          external_id="transformation6",
                >>>          name="transformation6",
                >>>          ignore_null_fields=False,
                >>>          data_model = DataModelInfo(space="modelSpace", external_id="modelExternalId",version="modelVersion",destination_type="viewExternalId"),
                >>>          destination=TransformationDestination.instances(data_model,"InstanceSpace")
                >>>      ),
                >>>      TransformationWrite(
                >>>          external_id="transformation7",
                >>>          name="transformation7",
                >>>          ignore_null_fields=False,
                >>>          data_model = DataModelInfo(space="modelSpace", external_id="modelExternalId",version="modelVersion",destination_type="viewExternalId", destination_relationship_from_type="connectionPropertyName"),
                >>>          destination=TransformationDestination.instances(data_model,"InstanceSpace")
                >>>      ),
                >>> ]
                >>> res = client.transformations.create(transformations)

        """
        if isinstance(transformation, Sequence):
            sessions: dict[str, NonceCredentials] = {}
            # When calling as_write() the transformation is copied
            transformation = [t.as_write() if isinstance(t, Transformation) else t.copy() for t in transformation]
            for t in transformation:
                t._process_credentials(cognite_client=self._cognite_client, sessions_cache=sessions)
        elif isinstance(transformation, Transformation):
            transformation = transformation.as_write()
            transformation._process_credentials(self._cognite_client)
        elif isinstance(transformation, TransformationWrite):
            transformation = transformation.copy()
            transformation._process_credentials(self._cognite_client)
        else:
            raise TypeError(
                "transformation must be Sequence[Transformation] or Sequence[TransformationWrite] or Transformation or TransformationWrite"
            )

        return self._create_multiple(
            list_cls=TransformationList,
            resource_cls=Transformation,
            items=transformation,
            input_resource_cls=TransformationWrite,
        )

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more transformations. <https://developer.cognite.com/api#tag/Transformations/operation/deleteTransformations>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids.
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Example:

            Delete transformations by id or external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.transformations.delete(id=[1,2,3], external_id="function3")
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
        data_set_ids: int | list[int] | None = None,
        data_set_external_ids: str | list[str] | None = None,
        tags: TagsFilter | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TransformationList:
        """`List all transformations. <https://developer.cognite.com/api#tag/Transformations/operation/filterTransformations>`_

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
            data_set_ids (int | list[int] | None): Return only transformations in the specified data sets with these id(s).
            data_set_external_ids (str | list[str] | None): Return only transformations in the specified data sets with these external id(s).
            tags (TagsFilter | None): Return only the resource matching the specified tags constraints. It only supports ContainsAny as of now.
            limit (int | None): Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.

        Returns:
            TransformationList: List of transformations

        Example:

            List transformations::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> transformations_list = client.transformations.list()
        """
        ds_ids = IdentifierSequence.load(data_set_ids, data_set_external_ids, id_name="data_set").as_dicts()

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
            data_set_ids=ds_ids or None,
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
                >>> client = CogniteClient()
                >>> res = client.transformations.retrieve(id=1)

            Get transformation by external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.transformations.retrieve(external_id="1")
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
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> TransformationList:
        """`Retrieve multiple transformations. <https://developer.cognite.com/api#tag/Transformations/operation/getTransformationsByIds>`_

        Args:
            ids (Sequence[int] | None): List of ids to retrieve.
            external_ids (SequenceNotStr[str] | None): List of external ids to retrieve.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            TransformationList: Requested transformation or None if it does not exist.

        Examples:

            Get multiple transformations:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.transformations.retrieve_multiple(ids=[1,2,3], external_ids=['transform-1','transform-2'])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TransformationList,
            resource_cls=Transformation,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    @overload
    def update(
        self,
        item: Transformation | TransformationWrite | TransformationUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Transformation: ...

    @overload
    def update(
        self,
        item: Sequence[Transformation | TransformationWrite | TransformationUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> TransformationList: ...

    def update(
        self,
        item: Transformation
        | TransformationWrite
        | TransformationUpdate
        | Sequence[Transformation | TransformationWrite | TransformationUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Transformation | TransformationList:
        """`Update one or more transformations <https://developer.cognite.com/api#tag/Transformations/operation/updateTransformations>`_

        Args:
            item (Transformation | TransformationWrite | TransformationUpdate | Sequence[Transformation | TransformationWrite | TransformationUpdate]): Transformation(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (Transformation or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Transformation | TransformationList: Updated transformation(s)

        Examples:

            Update a transformation that you have fetched. This will perform a full update of the transformation::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> transformation = client.transformations.retrieve(id=1)
                >>> transformation.query = "SELECT * FROM _cdf.assets"
                >>> res = client.transformations.update(transformation)

            Perform a partial update on a transformation, updating the query and making it private::

                >>> from cognite.client.data_classes import TransformationUpdate
                >>> my_update = TransformationUpdate(id=1).query.set("SELECT * FROM _cdf.assets").is_public.set(False)
                >>> res = client.transformations.update(my_update)

            Update the session used for reading (source) and writing (destination) when authenticating for all
            transformations in a given data set:

                >>> from cognite.client.data_classes import NonceCredentials
                >>> to_update = client.transformations.list(data_set_external_ids=["foo"])
                >>> new_session = client.iam.sessions.create()
                >>> new_nonce = NonceCredentials(
                ...     session_id=new_session.id,
                ...     nonce=new_session.nonce,
                ...     cdf_project_name=client.config.project
                ... )
                >>> for tr in to_update:
                ...     tr.source_nonce = new_nonce
                ...     tr.destination_nonce = new_nonce
                >>> res = client.transformations.update(to_update)
        """
        if isinstance(item, Sequence):
            item = list(item)
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
                "item must be one of: TransformationUpdate, Transformation, Sequence[TransformationUpdate | Transformation]."
            )

        return self._update_multiple(
            list_cls=TransformationList,
            resource_cls=Transformation,
            update_cls=TransformationUpdate,
            items=item,
            mode=mode,
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
                >>> client = CogniteClient()
                >>>
                >>> res = client.transformations.run(transformation_id = 1)

            Start running transformation by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>>
                >>> res = client.transformations.run(transformation_id = 1, wait = False)
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
                >>> client = CogniteClient()
                >>>
                >>> async def run_transformation():
                >>>     res = await client.transformations.run_async(id = 1)
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
                >>> client = CogniteClient()
                >>>
                >>> res = client.transformations.run(id = 1, timeout = 60.0)
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
        limit: int | None = 100,
        source_limit: int | None = 100,
        infer_schema_limit: int | None = 10_000,
        timeout: int | None = 240,
    ) -> TransformationPreviewResult:
        """`Preview the result of a query. <https://developer.cognite.com/api#tag/Query/operation/runPreview>`_

        Args:
            query (str | None): SQL query to run for preview.
            convert_to_string (bool): Stringify values in the query results, default is False.
            limit (int | None): Maximum number of rows to return in the final result, default is 100.
            source_limit (int | None): Maximum number of items to read from the data source or None to run without limit, default is 100.
            infer_schema_limit (int | None): Limit for how many rows that are used for inferring result schema, default is 10 000.
            timeout (int | None): Number of seconds to wait before cancelling a query. The default, and maximum, is 240.

        Returns:
            TransformationPreviewResult: Result of the executed query

        Examples:

            Preview transformation results as schema and list of rows:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>>
                >>> query_result = client.transformations.preview(query="select * from _cdf.assets")

            Preview transformation results as pandas dataframe:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>>
                >>> df = client.transformations.preview(query="select * from _cdf.assets").to_pandas()

            Notice that the results are limited both by the `limit` and `source_limit` parameters. If you have
            a query that converts one source row to one result row, you may need to increase the `source_limit`.
            For example, given that you have a query that reads from a raw table with 10,903 rows

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>>
                >>> result = client.transformations.preview(query="select * from my_raw_db.my_raw_table", limit=None)
                >>> print(result.results)
                100

            To get all rows, you also need to set the `source_limit` to None:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>>
                >>> result = client.transformations.preview(query="select * from my_raw_db.my_raw_table", limit=None, source_limit=None)
                >>> print(result.results)
                10903

        """
        request_body = {
            "query": query,
            "convertToString": convert_to_string,
            "limit": limit,
            "sourceLimit": source_limit,
            "inferSchemaLimit": infer_schema_limit,
            "timeout": timeout,
        }
        response = self._post(url_path=self._RESOURCE_PATH + "/query/run", json=request_body)
        result = TransformationPreviewResult._load(response.json(), cognite_client=self._cognite_client)
        return result
