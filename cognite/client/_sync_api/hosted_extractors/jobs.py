"""
===============================================================================
f542ffd1d02db6ccc5d458d6e28d107c
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.hosted_extractors.jobs import (
    Job,
    JobList,
    JobLogsList,
    JobMetricsList,
    JobUpdate,
    JobWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncJobsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Job]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[JobList]: ...

    def __call__(self, chunk_size: int | None = None, limit: int | None = None) -> Iterator[Job | JobList]:
        """
        Iterate over jobs

        Fetches jobs as they are iterated over, so you keep a limited number of jobs in memory.

        Args:
            chunk_size (int | None): Number of jobs to return in each chunk. Defaults to yielding one job a time.
            limit (int | None): Maximum number of jobs to return. Defaults to returning all items.

        Yields:
            Job | JobList: yields Job one by one if chunk_size is not specified, else JobList objects.
        """
        yield from SyncIterator(self.__async_client.hosted_extractors.jobs(chunk_size=chunk_size, limit=limit))  # type: ignore [misc]

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Job | None: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> JobList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Job | None | JobList:
        """
        `Retrieve one or more jobs. <https://api-docs.cognite.com/20230101-beta/tag/Jobs/operation/retrieve_jobs>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the job type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found

        Returns:
            Job | None | JobList: Requested jobs

        Examples:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.hosted_extractors.jobs.retrieve('myJob')

            Get multiple jobs by id:

                >>> res = client.hosted_extractors.jobs.retrieve(["myJob", "myOtherJob"], ignore_unknown_ids=True)
        """
        return run_sync(
            self.__async_client.hosted_extractors.jobs.retrieve(
                external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def delete(self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """
        `Delete one or more jobs <https://api-docs.cognite.com/20230101-beta/tag/Jobs/operation/delete_jobs>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found
        Examples:

            Delete jobs by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.hosted_extractors.jobs.delete(["myMQTTJob", "MyEventHubJob"])
        """
        return run_sync(
            self.__async_client.hosted_extractors.jobs.delete(
                external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    @overload
    def create(self, items: JobWrite) -> Job: ...

    @overload
    def create(self, items: Sequence[JobWrite]) -> JobList: ...

    def create(self, items: JobWrite | Sequence[JobWrite]) -> Job | JobList:
        """
        `Create one or more jobs. <https://api-docs.cognite.com/20230101-beta/tag/Jobs/operation/create_jobs>`_

        Args:
            items (JobWrite | Sequence[JobWrite]): Job(s) to create.

        Returns:
            Job | JobList: Created job(s)

        Examples:

            Create new job:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import EventHubSourceWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> job_write = EventHubSourceWrite('my_event_hub', 'http://myeventhub.com', "My EventHub", 'my_key', 'my_value')
                >>> job = client.hosted_extractors.jobs.create(job_write)
        """
        return run_sync(self.__async_client.hosted_extractors.jobs.create(items=items))

    @overload
    def update(
        self,
        items: JobWrite | JobUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Job: ...

    @overload
    def update(
        self,
        items: Sequence[JobWrite | JobUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> JobList: ...

    def update(
        self,
        items: JobWrite | JobUpdate | Sequence[JobWrite | JobUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Job | JobList:
        """
        `Update one or more jobs. <https://api-docs.cognite.com/20230101-beta/tag/Jobs/operation/update_jobs>`_

        Args:
            items (JobWrite | JobUpdate | Sequence[JobWrite | JobUpdate]): Job(s) to update.
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (JobWrite). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Job | JobList: Updated job(s)

        Examples:

            Update job:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import EventHubSourceUpdate
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> job = EventHubSourceUpdate('my_event_hub').event_hub_name.set("My Updated EventHub")
                >>> updated_job = client.hosted_extractors.jobs.update(job)
        """
        return run_sync(self.__async_client.hosted_extractors.jobs.update(items=items, mode=mode))

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> JobList:
        """
        `List jobs <https://api-docs.cognite.com/20230101-beta/tag/Jobs/operation/list_jobs>`_

        Args:
            limit (int | None): Maximum number of jobs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            JobList: List of requested jobs

        Examples:

            List jobs:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> job_list = client.hosted_extractors.jobs.list(limit=5)

            Iterate over jobs, one-by-one:

                >>> for job in client.hosted_extractors.jobs():
                ...     job  # do something with the job

            Iterate over chunks of jobs to reduce memory load:

                >>> for job_list in client.hosted_extractors.jobs(chunk_size=25):
                ...     job_list # do something with the jobs
        """
        return run_sync(self.__async_client.hosted_extractors.jobs.list(limit=limit))

    def list_logs(
        self,
        job: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> JobLogsList:
        """
        `List job logs. <https://api-docs.cognite.com/20230101-beta/tag/Jobs/operation/get_job_logs>`_

        Args:
            job (str | None): Require returned logs to belong to the job given by this external ID.
            source (str | None): Require returned logs to belong to the any job with source given by this external ID.
            destination (str | None): Require returned logs to belong to the any job with destination given by this external ID.
            limit (int | None): Maximum number of logs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            JobLogsList: List of requested job logs

        Examples:

            Reqests logs for a specific job:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.hosted_extractors.jobs.list_logs(job="myJob")
        """
        return run_sync(
            self.__async_client.hosted_extractors.jobs.list_logs(
                job=job, source=source, destination=destination, limit=limit
            )
        )

    def list_metrics(
        self,
        job: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> JobMetricsList:
        """
        `List job metrics. <https://api-docs.cognite.com/20230101-beta/tag/Jobs/operation/get_job_metrics>`_

        Args:
            job (str | None): Require returned metrics to belong to the job given by this external ID.
            source (str | None): Require returned metrics to belong to the any job with source given by this external ID.
            destination (str | None): Require returned metrics to belong to the any job with destination given by this external ID.
            limit (int | None): Maximum number of metrics to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            JobMetricsList: List of requested job metrics

        Examples:

            Reqests metrics for a specific job:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.hosted_extractors.jobs.list_metrics(job="myJob")
        """
        return run_sync(
            self.__async_client.hosted_extractors.jobs.list_metrics(
                job=job, source=source, destination=destination, limit=limit
            )
        )
