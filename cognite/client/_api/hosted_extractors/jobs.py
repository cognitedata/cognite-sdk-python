from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Sequence, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.hosted_extractors.jobs import (
    Job,
    JobList,
    JobLog,
    JobLogList,
    JobMetric,
    JobMetricList,
    JobUpdate,
    JobWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class JobsAPI(APIClient):
    _RESOURCE_PATH = "/hostedextractors/jobs"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="alpha", sdk_maturity="alpha", feature_name="Hosted Extractors"
        )
        self._CREATE_LIMIT = 100
        self._LIST_LIMIT = 100
        self._RETRIEVE_LIMIT = 100
        self._DELETE_LIMIT = 100
        self._UPDATE_LIMIT = 100

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
    ) -> Iterator[Job]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[JobList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[Job] | Iterator[JobList]:
        """Iterate over jobs

        Fetches jobs as they are iterated over, so you keep a limited number of spaces in memory.

        Args:
            chunk_size (int | None): Number of jobs to return in each chunk. Defaults to yielding one job a time.
            limit (int | None): Maximum number of jobs to return. Defaults to returning all items.

        Returns:
            Iterator[Job] | Iterator[JobList]: yields Job one by one if chunk_size is not specified, else JobList objects.
        """
        self._warning.warn()

        return self._list_generator(
            list_cls=JobList,
            resource_cls=Job,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def __iter__(self) -> Iterator[Job]:
        """Iterate over jobs

        Fetches jobs as they are iterated over, so you keep a limited number of spaces in memory.

        Returns:
            Iterator[Job]: yields Job one by one.
        """
        return self()

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Job | None: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> JobList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Job | None | JobList:
        """`Retrieve one or more jobs. <https://developer.cognite.com/api#tag/Jobs/operation/retrieve_jobs>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the rejob type.
            ignore_unknown_ids (bool): No description.

        Returns:
            Job | None | JobList: Requested jobs

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.hosted_extractors.jobs.retrieve('myJob')

            Get multiple spaces by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.hosted_extractors.jobs.retrieve(["myJob", "myOtherJob"] ignore_unknown_ids=True)

        """
        self._warning.warn()
        return self._retrieve_multiple(
            list_cls=JobList,
            resource_cls=Job,
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            ignore_unknown_ids=ignore_unknown_ids,
            headers={"cdf-version": "beta"},
        )

    def delete(
        self,
        external_ids: str | SequenceNotStr[str],
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more jobs <https://developer.cognite.com/api#tag/Jobs/operation/delete_jobs>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found
        Examples:

            Delete jobs by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.hosted_extractors.jobs.delete(["myMQTTJob", "MyEvenHubJob"])
        """
        self._warning.warn()
        extra_body_fields: dict[str, Any] = {}
        if ignore_unknown_ids:
            extra_body_fields["ignoreUnknownIds"] = True

        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            wrap_ids=True,
            returns_items=False,
            headers={"cdf-version": "beta"},
            extra_body_fields=extra_body_fields or None,
        )

    @overload
    def create(self, items: JobWrite) -> Job: ...

    @overload
    def create(self, items: Sequence[JobWrite]) -> JobList: ...

    def create(self, items: JobWrite | Sequence[JobWrite]) -> Job | JobList:
        """`Create one or more jobs. <https://developer.cognite.com/api#tag/Jobs/operation/create_jobs>`_

        Args:
            items (JobWrite | Sequence[JobWrite]): Space | Sequence[Space]): Job(s) to create.

        Returns:
            Job | JobList: Created job(s)

        Examples:

            Create new job:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import EventHubSourceWrite
                >>> client = CogniteClient()
                >>> job = EventHubSourceWrite('my_event_hub', 'http://myeventhub.com', "My EventHub", 'my_key', 'my_value')
                >>> res = client.hosted_extractors.jobs.create(job)
        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=JobList,
            resource_cls=Job,
            items=items,
            input_resource_cls=JobWrite,
            headers={"cdf-version": "beta"},
        )

    @overload
    def update(self, items: JobWrite | JobUpdate) -> Job: ...

    @overload
    def update(self, items: Sequence[JobWrite | JobUpdate]) -> JobList: ...

    def update(self, items: JobWrite | JobUpdate | Sequence[JobWrite | JobUpdate]) -> Job | JobList:
        """`Update one or more jobs. <https://developer.cognite.com/api#tag/Jobs/operation/update_jobs>`_

        Args:
            items (JobWrite | JobUpdate | Sequence[JobWrite | JobUpdate]): Space | Sequence[Space]): Job(s) to update.

        Returns:
            Job | JobList: Updated job(s)

        Examples:

            Update job:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import EventHubSourceUpdate
                >>> client = CogniteClient()
                >>> job = EventHubSourceUpdate('my_event_hub').event_hub_name.set("My Updated EventHub")
                >>> res = client.hosted_extractors.jobs.update(job)
        """
        self._warning.warn()
        return self._update_multiple(
            items=items,
            list_cls=JobList,
            resource_cls=Job,
            update_cls=JobUpdate,
            headers={"cdf-version": "beta"},
        )

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> JobList:
        """`List jobs <https://developer.cognite.com/api#tag/Jobs/operation/list_jobs>`_

        Args:
            limit (int | None): Maximum number of jobs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            JobList: List of requested jobs

        Examples:

            List jobs:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> space_list = client.hosted_extractors.jobs.list(limit=5)

            Iterate over jobs::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for job in client.hosted_extractors.jobs:
                ...     job # do something with the job

            Iterate over chunks of jobs to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for job_list in client.hosted_extractors.jobs(chunk_size=25):
                ...     job_list # do something with the spaces
        """
        self._warning.warn()
        return self._list(
            list_cls=JobList,
            resource_cls=Job,  # type: ignore[type-abstract]
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def list_logs(
        self,
        job: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> JobLogList:
        """`List job logs. <https://developer.cognite.com/api#tag/Jobs/operation/get_job_logs>`_

        Args:
            job (str | None): Require returned logs to belong to the job given by this external ID.
            source (str | None): Require returned logs to belong to the any job with source given by this external ID.
            destination (str | None): Require returned logs to belong to the any job with destination given by this external ID.
            limit (int | None): Maximum number of logs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.



        Returns:
            JobLogList: List of requested job logs

        Examples:

            Reqests logs for a specific job::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.hosted_extractors.jobs.list_logs(job="myJob")
        """
        self._warning.warn()
        filter_: dict[str, Any] = {}
        if job:
            filter_["job"] = job
        if source:
            filter_["source"] = source
        if destination:
            filter_["destination"] = destination

        return self._list(
            list_cls=JobLogList,
            resource_cls=JobLog,
            filter=filter_ or None,
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def list_metrics(
        self,
        job: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> JobMetricList:
        """`List job metrics. <https://developer.cognite.com/api#tag/Jobs/operation/get_job_logs>`_

        Args:
            job (str | None): Require returned metrics to belong to the job given by this external ID.
            source (str | None): Require returned metrics to belong to the any job with source given by this external ID.
            destination (str | None): Require returned metrics to belong to the any job with destination given by this external ID.
            limit (int | None): Maximum number of metrics to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            JobMetricList: List of requested job metrics

        Examples:

            Reqests metrics for a specific job::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.hosted_extractors.jobs.list_metrics(job="myJob")
        """
        self._warning.warn()
        filter_: dict[str, Any] = {}
        if job:
            filter_["job"] = job
        if source:
            filter_["source"] = source
        if destination:
            filter_["destination"] = destination

        return self._list(
            list_cls=JobMetricList,
            resource_cls=JobMetric,
            filter=filter_ or None,
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
