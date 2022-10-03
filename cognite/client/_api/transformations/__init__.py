from typing import Any, Awaitable, Dict, List, Optional, Sequence, Union

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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.jobs = TransformationJobsAPI(*args, **kwargs)
        self.schedules = TransformationSchedulesAPI(*args, **kwargs)
        self.schema = TransformationSchemaAPI(*args, **kwargs)
        self.notifications = TransformationNotificationsAPI(*args, **kwargs)

    def create(
        self, transformation: Union[Transformation, Sequence[Transformation]]
    ) -> Union[Transformation, TransformationList]:
        """`Create one or more transformations. <https://docs.cognite.com/api/v1/#operation/createTransformations>`_

        Args:
            transformation (Union[Transformation, List[Transformation]]): Transformation or list of transformations to create.

        Returns:
            Created transformation(s)

        Examples:

            Create new transformations:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Transformation, TransformationDestination
                >>> c = CogniteClient()
                >>> transformations = [
                >>>     Transformation(
                >>>         name="transformation1",
                >>>         destination=TransformationDestination.assets()
                >>>     ),
                >>>     Transformation(
                >>>         name="transformation2",
                >>>         destination=TransformationDestination.raw("myDatabase", "myTable"),
                >>>     ),
                >>> ]
                >>> res = c.transformations.create(transformations)
        """
        if isinstance(transformation, Sequence):
            sessions: Dict[str, NonceCredentials] = {}
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
        id: Union[int, Sequence[int]] = None,
        external_id: Union[str, Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more transformations. <https://docs.cognite.com/api/v1/#operation/deleteTransformations>`_

        Args:
            id (Union[int, List[int]): Id or list of ids.
            external_id (Union[str, List[str]]): External ID or list of external ids.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            None

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
        name_regex: str = None,
        query_regex: str = None,
        destination_type: str = None,
        conflict_mode: str = None,
        cdf_project_name: str = None,
        has_blocked_error: bool = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        data_set_ids: List[int] = None,
        data_set_external_ids: List[str] = None,
        tags: Optional[TagsFilter] = None,
        limit: Optional[int] = 25,
    ) -> TransformationList:
        """`List all transformations. <https://docs.cognite.com/api/v1/#operation/getTransformations>`_

        Args:
            include_public (bool): Whether public transformations should be included in the results. (default true).
            name_regex (str): Regex expression to match the transformation name
            query_regex (str): Regex expression to match the transformation query
            destination_type (str): Transformation destination resource name to filter by.
            conflict_mode (str): Filters by a selected transformation action type: abort/create, upsert, update, delete
            cdf_project_name (str): Project name to filter by configured source and destination project
            has_blocked_error (bool): Whether only the blocked transformations should be included in the results.
            created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps
            last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps
            data_set_ids (List[int]): Return only transformations in the specified data sets with these ids.
            data_set_external_ids (List[str]): Return only transformations in the specified data sets with these external ids.
            tags (TagsFilter): Return only the resource matching the specified tags constraints. It only supports ContainsAny as of now.
            limit (int): Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.

        Returns:
            TransformationList: List of transformations

        Example:

            List transformations::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> transformations_list = c.transformations.list()
        """
        ds_ids: Optional[List[Dict[str, Any]]] = None
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

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Transformation]:
        """`Retrieve a single transformation by id. <https://docs.cognite.com/api/v1/#operation/getTransformationsByIds>`_

        Args:
            id (int, optional): ID

        Returns:
            Optional[Transformation]: Requested transformation or None if it does not exist.

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
        self, ids: Sequence[int] = None, external_ids: Sequence[str] = None, ignore_unknown_ids: bool = False
    ) -> TransformationList:
        """`Retrieve multiple transformations. <https://docs.cognite.com/api/v1/#operation/getTransformationsByIds>`_

        Args:
            ids (List[int]): List of ids to retrieve.
            external_ids (List[str]): List of external ids to retrieve.
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
        self, item: Union[Transformation, TransformationUpdate, Sequence[Union[Transformation, TransformationUpdate]]]
    ) -> Union[Transformation, TransformationList]:
        """`Update one or more transformations <https://docs.cognite.com/api/v1/#operation/updateTransformations>`_

        Args:
            item (Union[Transformation, TransformationUpdate, List[Union[Transformation, TransformationUpdate]]]): Transformation(s) to update

        Returns:
            Union[Transformation, TransformationList]: Updated transformation(s)

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
            sessions: Dict[str, NonceCredentials] = {}
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

    def run(
        self,
        transformation_id: int = None,
        transformation_external_id: str = None,
        wait: bool = True,
        timeout: Optional[float] = None,
    ) -> TransformationJob:
        """`Run a transformation. <https://docs.cognite.com/api/v1/#operation/runTransformation>`_

        Args:
            transformation_id (int): Transformation internal id
            transformation_external_id (str): Transformation external id
            wait (bool): Wait until the transformation run is finished. Defaults to True.
            timeout (Optional[float]): maximum time (s) to wait, default is None (infinite time). Once the timeout is reached, it returns with the current status. Won't have any effect if wait is False.

        Returns:
            Created transformation job

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

    def run_async(
        self, transformation_id: int = None, transformation_external_id: str = None, timeout: Optional[float] = None
    ) -> Awaitable[TransformationJob]:
        """`Run a transformation to completion asynchronously. <https://docs.cognite.com/api/v1/#operation/runTransformation>`_

        Args:
            transformation_id (int): internal Transformation id
            transformation_external_id (str): external Transformation id
            timeout (Optional[float]): maximum time (s) to wait, default is None (infinite time). Once the timeout is reached, it returns with the current status.

        Returns:
            Completed (if finished) or running (if timeout reached) transformation job.

        Examples:

            Run transformation asyncronously by id:

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
        return job.wait_async(timeout=timeout)

    def cancel(self, transformation_id: int = None, transformation_external_id: str = None) -> None:
        """`Cancel a running transformation. <https://docs.cognite.com/api/v1/#operation/cancelTransformation>`_

        Args:
            transformation_id (int): Transformation internal id
            transformation_external_id (str): Transformation external id

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
        query: str = None,
        convert_to_string: bool = False,
        limit: int = 100,
        source_limit: Optional[int] = 100,
        infer_schema_limit: Optional[int] = 1000,
    ) -> TransformationPreviewResult:
        """`Preview the result of a query. <https://docs.cognite.com/api/v1/#operation/runPreview>`_

        Args:
            query (str): SQL query to run for preview.
            convert_to_string (bool): Stringify values in the query results, default is False.
            limit (int): Maximum number of rows to return in the final result, default is 100.
            source_limit (Union[int,str]): Maximum number of items to read from the data source or None to run without limit, default is 100.
            infer_schema_limit: Limit for how many rows that are used for inferring result schema, default is 1000.

        Returns:
            Result of the executed query

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
