"""
===============================================================================
1c4cb4580aff601bbc0db5a94295b802
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    TransformationJob,
    TransformationJobList,
    TransformationJobMetricList,
)
from cognite.client.utils._async_helpers import run_sync


class SyncTransformationJobsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        transformation_id: int | None = None,
        transformation_external_id: str | None = None,
    ) -> TransformationJobList:
        """
        `List all running transformation jobs. <https://developer.cognite.com/api#tag/Transformation-Jobs/operation/getTransformationJobs>`_

        Args:
            limit: Limits the number of results to be returned. To retrieve all results use limit=-1, default limit is 25.
            transformation_id: Filters the results by the internal transformation id.
            transformation_external_id: Filters the results by the external transformation id.

        Returns:
            List of transformation jobs

        Example:

            List transformation jobs::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> transformation_jobs_list = client.transformations.jobs.list()

            List transformation jobs of a single transformation::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> transformation_jobs_list = client.transformations.jobs.list(transformation_id=1)
        """
        return run_sync(
            self.__async_client.transformations.jobs.list(
                limit=limit, transformation_id=transformation_id, transformation_external_id=transformation_external_id
            )
        )

    def retrieve(self, id: int) -> TransformationJob | None:
        """
        `Retrieve a single transformation job by id. <https://developer.cognite.com/api#tag/Transformation-Jobs/operation/getTransformationJobsByIds>`_

        Args:
            id: Job internal Id

        Returns:
            Requested transformation job or None if it does not exist.

        Examples:

            Get transformation job by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.transformations.jobs.retrieve(id=1)
        """
        return run_sync(self.__async_client.transformations.jobs.retrieve(id=id))

    def list_metrics(self, id: int) -> TransformationJobMetricList:
        """
        `List the metrics of a single transformation job. <https://developer.cognite.com/api#tag/Transformation-Jobs/operation/getTransformationJobsMetrics>`_

        Args:
            id: Job internal Id

        Returns:
            List of updated metrics of the given job.

        Examples:

            Get metrics by transformation job id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.transformations.jobs.list_metrics(id=1)
        """
        return run_sync(self.__async_client.transformations.jobs.list_metrics(id=id))

    def retrieve_multiple(self, ids: Sequence[int], ignore_unknown_ids: bool = False) -> TransformationJobList:
        """
        `Retrieve multiple transformation jobs by id. <https://developer.cognite.com/api#tag/Transformation-Jobs/operation/getTransformationJobsByIds>`_

        Args:
            ids: Job internal Ids
            ignore_unknown_ids: Ignore IDs that are not found rather than throw an exception.

        Returns:
            Requested transformation jobs.

        Examples:

            Get jobs by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.transformations.jobs.retrieve_multiple(ids=[1, 2, 3])
        """
        return run_sync(
            self.__async_client.transformations.jobs.retrieve_multiple(ids=ids, ignore_unknown_ids=ignore_unknown_ids)
        )
