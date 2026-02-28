"""
===============================================================================
1a494e5ef4495baf6939256abff06c0f
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.transformations.jobs import SyncTransformationJobsAPI
from cognite.client._sync_api.transformations.notifications import SyncTransformationNotificationsAPI
from cognite.client._sync_api.transformations.schedules import SyncTransformationSchedulesAPI
from cognite.client._sync_api.transformations.schema import SyncTransformationSchemaAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import Transformation, TransformationJob, TransformationList
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.transformations import (
    TagsFilter,
    TransformationPreviewResult,
    TransformationUpdate,
    TransformationWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncTransformationsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.jobs = SyncTransformationJobsAPI(async_client)
        self.schedules = SyncTransformationSchedulesAPI(async_client)
        self.schema = SyncTransformationSchemaAPI(async_client)
        self.notifications = SyncTransformationNotificationsAPI(async_client)

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        include_public: bool = True,
        name_regex: str | None = None,
        query_regex: str | None = None,
        destination_type: str | None = None,
        conflict_mode: Literal["abort", "delete", "update", "upsert"] | None = None,
        cdf_project_name: str | None = None,
        has_blocked_error: bool | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        data_set_ids: int | list[int] | None = None,
        data_set_external_ids: str | list[str] | None = None,
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
        conflict_mode: Literal["abort", "delete", "update", "upsert"] | None = None,
        cdf_project_name: str | None = None,
        has_blocked_error: bool | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        data_set_ids: int | list[int] | None = None,
        data_set_external_ids: str | list[str] | None = None,
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
        conflict_mode: Literal["abort", "delete", "update", "upsert"] | None = None,
        cdf_project_name: str | None = None,
        has_blocked_error: bool | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        data_set_ids: int | list[int] | None = None,
        data_set_external_ids: str | list[str] | None = None,
        tags: TagsFilter | None = None,
        limit: int | None = None,
    ) -> Iterator[Transformation] | Iterator[TransformationList]:
        """
        Iterate over transformations

        Args:
            chunk_size (int | None): Number of transformations to return in each chunk. Defaults to yielding one transformation a time.
            include_public (bool): Whether public transformations should be included in the results. (default true).
            name_regex (str | None): Regex expression to match the transformation name
            query_regex (str | None): Regex expression to match the transformation query
            destination_type (str | None): Transformation destination resource name to filter by.
            conflict_mode (Literal['abort', 'delete', 'update', 'upsert'] | None): Filters by a selected transformation action type: abort/create, upsert, update, delete
            cdf_project_name (str | None): Project name to filter by configured source and destination project
            has_blocked_error (bool | None): Whether only the blocked transformations should be included in the results.
            created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            data_set_ids (int | list[int] | None): Return only transformations in the specified data sets with these id(s).
            data_set_external_ids (str | list[str] | None): Return only transformations in the specified data sets with these external id(s).
            tags (TagsFilter | None): Return only the resource matching the specified tags constraints. It only supports ContainsAny as of now.
            limit (int | None): Limits the number of results to be returned. Defaults to yielding all transformations.

        Yields:
            Transformation | TransformationList: Yields transformations in chunks if chunk_size is specified, otherwise one transformation at a time.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.transformations(
                chunk_size=chunk_size,
                include_public=include_public,
                name_regex=name_regex,
                query_regex=query_regex,
                destination_type=destination_type,
                conflict_mode=conflict_mode,
                cdf_project_name=cdf_project_name,
                has_blocked_error=has_blocked_error,
                created_time=created_time,
                last_updated_time=last_updated_time,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                tags=tags,
                limit=limit,
            )
        )  # type: ignore [misc]

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
        """
        `Create one or more transformations. <https://api-docs.cognite.com/20230101/tag/Transformations/operation/createTransformations>`_

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
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(self.__async_client.transformations.create(transformation=transformation))

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """
        `Delete one or more transformations. <https://api-docs.cognite.com/20230101/tag/Transformations/operation/deleteTransformations>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids.
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Example:

            Delete transformations by id or external id::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.transformations.delete(id=[1,2,3], external_id="function3")
        """
        return run_sync(
            self.__async_client.transformations.delete(
                id=id, external_id=external_id, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def list(
        self,
        include_public: bool = True,
        name_regex: str | None = None,
        query_regex: str | None = None,
        destination_type: str | None = None,
        conflict_mode: Literal["abort", "delete", "update", "upsert"] | None = None,
        cdf_project_name: str | None = None,
        has_blocked_error: bool | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        data_set_ids: int | list[int] | None = None,
        data_set_external_ids: str | list[str] | None = None,
        tags: TagsFilter | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TransformationList:
        """
        `List all transformations. <https://api-docs.cognite.com/20230101/tag/Transformations/operation/filterTransformations>`_

        Args:
            include_public (bool): Whether public transformations should be included in the results. (default true).
            name_regex (str | None): Regex expression to match the transformation name
            query_regex (str | None): Regex expression to match the transformation query
            destination_type (str | None): Transformation destination resource name to filter by.
            conflict_mode (Literal['abort', 'delete', 'update', 'upsert'] | None): Filters by a selected transformation action type: abort/create, upsert, update, delete
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

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> transformations_list = client.transformations.list()
        """
        return run_sync(
            self.__async_client.transformations.list(
                include_public=include_public,
                name_regex=name_regex,
                query_regex=query_regex,
                destination_type=destination_type,
                conflict_mode=conflict_mode,
                cdf_project_name=cdf_project_name,
                has_blocked_error=has_blocked_error,
                created_time=created_time,
                last_updated_time=last_updated_time,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                tags=tags,
                limit=limit,
            )
        )

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> Transformation | None:
        """
        `Retrieve a single transformation by id. <https://api-docs.cognite.com/20230101/tag/Transformations/operation/getTransformationsByIds>`_

        Args:
            id (int | None): ID
            external_id (str | None): No description.

        Returns:
            Transformation | None: Requested transformation or None if it does not exist.

        Examples:

            Get transformation by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.transformations.retrieve(id=1)

            Get transformation by external id:

                >>> res = client.transformations.retrieve(external_id="1")
        """
        return run_sync(self.__async_client.transformations.retrieve(id=id, external_id=external_id))

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> TransformationList:
        """
        `Retrieve multiple transformations. <https://api-docs.cognite.com/20230101/tag/Transformations/operation/getTransformationsByIds>`_

        Args:
            ids (Sequence[int] | None): List of ids to retrieve.
            external_ids (SequenceNotStr[str] | None): List of external ids to retrieve.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            TransformationList: Requested transformation or None if it does not exist.

        Examples:

            Get multiple transformations:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.transformations.retrieve_multiple(ids=[1,2,3], external_ids=['transform-1','transform-2'])
        """
        return run_sync(
            self.__async_client.transformations.retrieve_multiple(
                ids=ids, external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
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
        """
        `Update one or more transformations <https://api-docs.cognite.com/20230101/tag/Transformations/operation/updateTransformations>`_

        Args:
            item (Transformation | TransformationWrite | TransformationUpdate | Sequence[Transformation | TransformationWrite | TransformationUpdate]): Transformation(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (Transformation or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Transformation | TransformationList: Updated transformation(s)

        Examples:

            Update a transformation that you have fetched. This will perform a full update of the transformation:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> transformation = client.transformations.retrieve(id=1)
                >>> transformation.query = "SELECT * FROM _cdf.assets"
                >>> res = client.transformations.update(transformation)

            Perform a partial update on a transformation, updating the query and making it private:

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
        return run_sync(self.__async_client.transformations.update(item=item, mode=mode))

    def run(
        self,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
        wait: bool = True,
        timeout: float | None = None,
    ) -> TransformationJob:
        """
        `Run a transformation. <https://api-docs.cognite.com/20230101/tag/Transformations/operation/runTransformation>`_

        Args:
            transformation_id (int | None): Transformation internal id
            transformation_external_id (str | None): Transformation external id
            wait (bool): Wait until the transformation run is finished. Defaults to True.
            timeout (float | None): maximum time (s) to wait, default is None (infinite time). Once the timeout is reached, it returns with the current status. Won't have any effect if wait is False.

        Returns:
            TransformationJob: Created transformation job

        Examples:

            Run transformation to completion by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>>
                >>> res = client.transformations.run(transformation_id=1)

            Start running transformation by id:

                >>> res = client.transformations.run(transformation_id=1, wait=False)
        """
        return run_sync(
            self.__async_client.transformations.run(
                transformation_id=transformation_id,
                transformation_external_id=transformation_external_id,
                wait=wait,
                timeout=timeout,
            )
        )

    def cancel(self, transformation_id: int | None = None, transformation_external_id: str | None = None) -> None:
        """
        `Cancel a running transformation. <https://api-docs.cognite.com/20230101/tag/Transformations/operation/postApiV1ProjectsProjectTransformationsCancel>`_

        Args:
            transformation_id (int | None): Transformation internal id
            transformation_external_id (str | None): Transformation external id

        Examples:

            Wait transformation for 1 minute and cancel if still running:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationJobStatus
                >>> client = CogniteClient()
                >>>
                >>> res = client.transformations.run(transformation_id=1, timeout=60.0)
                >>> if res.status == TransformationJobStatus.RUNNING:
                >>>     res.cancel()
        """
        return run_sync(
            self.__async_client.transformations.cancel(
                transformation_id=transformation_id, transformation_external_id=transformation_external_id
            )
        )

    def preview(
        self,
        query: str | None = None,
        convert_to_string: bool = False,
        limit: int | None = 100,
        source_limit: int | None = 100,
        infer_schema_limit: int | None = 10000,
        timeout: int | None = 240,
    ) -> TransformationPreviewResult:
        """
        `Preview the result of a query. <https://api-docs.cognite.com/20230101/tag/Query/operation/runPreview>`_

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

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>>
                >>> query_result = client.transformations.preview(query="select * from _cdf.assets")

            Preview transformation results as pandas dataframe:

                >>> df = client.transformations.preview(query="select * from _cdf.assets").to_pandas()

            Notice that the results are limited both by the `limit` and `source_limit` parameters. If you have
            a query that converts one source row to one result row, you may need to increase the `source_limit`.
            For example, given that you have a query that reads from a raw table with 10,903 rows

                >>> result = client.transformations.preview(query="select * from my_raw_db.my_raw_table", limit=None)
                >>> print(result.results)  # 100

            To get all rows, you also need to set the `source_limit` to None:

                >>> result = client.transformations.preview(query="select * from my_raw_db.my_raw_table", limit=None, source_limit=None)
                >>> print(result.results)  # 10903
        """
        return run_sync(
            self.__async_client.transformations.preview(
                query=query,
                convert_to_string=convert_to_string,
                limit=limit,
                source_limit=source_limit,
                infer_schema_limit=infer_schema_limit,
                timeout=timeout,
            )
        )
