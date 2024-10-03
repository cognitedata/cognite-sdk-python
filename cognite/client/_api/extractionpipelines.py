from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    ExtractionPipeline,
    ExtractionPipelineConfig,
    ExtractionPipelineConfigRevisionList,
    ExtractionPipelineList,
    ExtractionPipelineRun,
    ExtractionPipelineRunFilter,
    ExtractionPipelineRunList,
    ExtractionPipelineUpdate,
    TimestampRange,
)
from cognite.client.data_classes.extractionpipelines import (
    ExtractionPipelineConfigWrite,
    ExtractionPipelineCore,
    ExtractionPipelineRunCore,
    ExtractionPipelineRunWrite,
    ExtractionPipelineWrite,
    StringFilter,
)
from cognite.client.utils import timestamp_to_ms
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


RunStatus: TypeAlias = Literal["success", "failure", "seen"]


class ExtractionPipelinesAPI(APIClient):
    _RESOURCE_PATH = "/extpipes"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.runs = ExtractionPipelineRunsAPI(config, api_version, cognite_client)
        self.config = ExtractionPipelineConfigsAPI(config, api_version, cognite_client)

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[ExtractionPipeline]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[ExtractionPipelineList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[ExtractionPipeline] | Iterator[ExtractionPipelineList]:
        """Iterate over extraction pipelines

        Args:
            chunk_size (int | None): Number of extraction pipelines to yield per chunk. Defaults to yielding extraction pipelines one by one.
            limit (int | None): Limits the number of results to be returned. Defaults to yielding all extraction pipelines.

        Returns:
            Iterator[ExtractionPipeline] | Iterator[ExtractionPipelineList]: Yields extraction pipelines one by one or in chunks up to the chunk size.

        """
        return self._list_generator(
            method="GET",
            limit=limit,
            chunk_size=chunk_size,
            resource_cls=ExtractionPipeline,
            list_cls=ExtractionPipelineList,
        )

    def __iter__(self) -> Iterator[ExtractionPipeline]:
        """Iterate over all extraction pipelines"""
        return self()

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> ExtractionPipeline | None:
        """`Retrieve a single extraction pipeline by id. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/showExtPipe>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            ExtractionPipeline | None: Requested extraction pipeline or None if it does not exist.

        Examples:

            Get extraction pipeline by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.retrieve(id=1)

            Get extraction pipeline by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.retrieve(external_id="1")
        """

        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, identifiers=identifiers
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtractionPipelineList:
        """`Retrieve multiple extraction pipelines by ids and external ids. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/byidsExtPipes>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            ExtractionPipelineList: The requested ExtractionPipelines.

        Examples:

            Get ExtractionPipelines by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.retrieve_multiple(ids=[1, 2, 3])

            Get assets by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.retrieve_multiple(external_ids=["abc", "def"], ignore_unknown_ids=True)
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> ExtractionPipelineList:
        """`List extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/listExtPipes>`_

        Args:
            limit (int | None): Maximum number of ExtractionPipelines to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ExtractionPipelineList: List of requested ExtractionPipelines

        Examples:

            List ExtractionPipelines::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> ep_list = client.extraction_pipelines.list(limit=5)
        """

        return self._list(list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, method="GET", limit=limit)

    @overload
    def create(self, extraction_pipeline: ExtractionPipeline | ExtractionPipelineWrite) -> ExtractionPipeline: ...

    @overload
    def create(
        self, extraction_pipeline: Sequence[ExtractionPipeline] | Sequence[ExtractionPipelineWrite]
    ) -> ExtractionPipelineList: ...

    def create(
        self,
        extraction_pipeline: ExtractionPipeline
        | ExtractionPipelineWrite
        | Sequence[ExtractionPipeline]
        | Sequence[ExtractionPipelineWrite],
    ) -> ExtractionPipeline | ExtractionPipelineList:
        """`Create one or more extraction pipelines. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/createExtPipes>`_

        You can create an arbitrary number of extraction pipelines, and the SDK will split the request into multiple requests if necessary.

        Args:
            extraction_pipeline (ExtractionPipeline | ExtractionPipelineWrite | Sequence[ExtractionPipeline] | Sequence[ExtractionPipelineWrite]): Extraction pipeline or list of extraction pipelines to create.

        Returns:
            ExtractionPipeline | ExtractionPipelineList: Created extraction pipeline(s)

        Examples:

            Create new extraction pipeline::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineWrite
                >>> client = CogniteClient()
                >>> extpipes = [ExtractionPipelineWrite(name="extPipe1",...), ExtractionPipelineWrite(name="extPipe2",...)]
                >>> res = client.extraction_pipelines.create(extpipes)
        """
        assert_type(extraction_pipeline, "extraction_pipeline", [ExtractionPipelineCore, Sequence])

        return self._create_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            items=extraction_pipeline,
            input_resource_cls=ExtractionPipelineWrite,
        )

    def delete(
        self, id: int | Sequence[int] | None = None, external_id: str | SequenceNotStr[str] | None = None
    ) -> None:
        """`Delete one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/deleteExtPipes>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids

        Examples:

            Delete extraction pipelines by id or external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.extraction_pipelines.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(id, external_id), wrap_ids=True, extra_body_fields={})

    @overload
    def update(
        self, item: ExtractionPipeline | ExtractionPipelineWrite | ExtractionPipelineUpdate
    ) -> ExtractionPipeline: ...

    @overload
    def update(
        self, item: Sequence[ExtractionPipeline | ExtractionPipelineWrite | ExtractionPipelineUpdate]
    ) -> ExtractionPipelineList: ...

    def update(
        self,
        item: ExtractionPipeline
        | ExtractionPipelineWrite
        | ExtractionPipelineUpdate
        | Sequence[ExtractionPipeline | ExtractionPipelineWrite | ExtractionPipelineUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> ExtractionPipeline | ExtractionPipelineList:
        """`Update one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/updateExtPipes>`_

        Args:
            item (ExtractionPipeline | ExtractionPipelineWrite | ExtractionPipelineUpdate | Sequence[ExtractionPipeline | ExtractionPipelineWrite | ExtractionPipelineUpdate]): Extraction pipeline(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (ExtractionPipeline or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            ExtractionPipeline | ExtractionPipelineList: Updated extraction pipeline(s)

        Examples:

            Update an extraction pipeline that you have fetched. This will perform a full update of the extraction pipeline::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineUpdate
                >>> client = CogniteClient()
                >>> update = ExtractionPipelineUpdate(id=1)
                >>> update.description.set("Another new extpipe")
                >>> res = client.extraction_pipelines.update(update)
        """
        return self._update_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            update_cls=ExtractionPipelineUpdate,
            items=item,
            mode=mode,
        )


class ExtractionPipelineRunsAPI(APIClient):
    _RESOURCE_PATH = "/extpipes/runs"

    def list(
        self,
        external_id: str,
        statuses: RunStatus | Sequence[RunStatus] | SequenceNotStr[str] | None = None,
        message_substring: str | None = None,
        created_time: dict[str, Any] | TimestampRange | str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ExtractionPipelineRunList:
        """`List runs for an extraction pipeline with given external_id <https://developer.cognite.com/api#tag/Extraction-Pipelines-Runs/operation/filterRuns>`_

        Args:
            external_id (str): Extraction pipeline external Id.
            statuses (RunStatus | Sequence[RunStatus] | SequenceNotStr[str] | None): One or more among "success" / "failure" / "seen".
            message_substring (str | None): Failure message part.
            created_time (dict[str, Any] | TimestampRange | str | None): Range between two timestamps. Possible keys are `min` and `max`, with values given as timestamps in ms.
                If a string is passed, it is assumed to be the minimum value.
            limit (int | None): Maximum number of ExtractionPipelines to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ExtractionPipelineRunList: List of requested extraction pipeline runs

        Tip:
            The ``created_time`` parameter can also be passed as a string, to support the most typical usage pattern
            of fetching the most recent runs, meaning it is implicitly assumed to be the minimum created time. The
            format is "N[timeunit]-ago", where timeunit is w,d,h,m (week, day, hour, minute), e.g. "12d-ago".

        Examples:

            List extraction pipeline runs::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> runsList = client.extraction_pipelines.runs.list(external_id="test ext id", limit=5)

            Filter extraction pipeline runs on a given status::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> runs_list = client.extraction_pipelines.runs.list(external_id="test ext id", statuses=["seen"], limit=5)

            Get all failed pipeline runs in the last 24 hours for pipeline 'extId':

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineRun
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.runs.list(external_id="extId", statuses="failure", created_time="24h-ago")
        """
        if isinstance(created_time, str):
            created_time = TimestampRange(min=timestamp_to_ms(created_time))

        if statuses is not None or message_substring is not None or created_time is not None:
            filter = ExtractionPipelineRunFilter(
                external_id=external_id,
                statuses=cast(SequenceNotStr[str] | None, [statuses] if isinstance(statuses, str) else statuses),
                message=StringFilter(substring=message_substring),
                created_time=created_time,
            ).dump(camel_case=True)
            method: Literal["POST", "GET"] = "POST"
        else:
            filter = {"externalId": external_id}
            method = "GET"

        res = self._list(
            list_cls=ExtractionPipelineRunList,
            resource_cls=ExtractionPipelineRun,
            method=method,
            limit=limit,
            filter=filter,
        )
        for run in res:
            run.extpipe_external_id = external_id
        return res

    @overload
    def create(self, run: ExtractionPipelineRun | ExtractionPipelineRunWrite) -> ExtractionPipelineRun: ...

    @overload
    def create(
        self, run: Sequence[ExtractionPipelineRun] | Sequence[ExtractionPipelineRunWrite]
    ) -> ExtractionPipelineRunList: ...

    def create(
        self,
        run: ExtractionPipelineRun
        | ExtractionPipelineRunWrite
        | Sequence[ExtractionPipelineRun]
        | Sequence[ExtractionPipelineRunWrite],
    ) -> ExtractionPipelineRun | ExtractionPipelineRunList:
        """`Create one or more extraction pipeline runs. <https://developer.cognite.com/api#tag/Extraction-Pipelines-Runs/operation/createRuns>`_

        You can create an arbitrary number of extraction pipeline runs, and the SDK will split the request into multiple requests.

        Args:
            run (ExtractionPipelineRun | ExtractionPipelineRunWrite | Sequence[ExtractionPipelineRun] | Sequence[ExtractionPipelineRunWrite]): ExtractionPipelineRun| ExtractionPipelineRunWrite | Sequence[ExtractionPipelineRun]  | Sequence[ExtractionPipelineRunWrite]): Extraction pipeline or list of extraction pipeline runs to create.

        Returns:
            ExtractionPipelineRun | ExtractionPipelineRunList: Created extraction pipeline run(s)

        Examples:

            Report a new extraction pipeline run::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineRunWrite
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.runs.create(
                ...     ExtractionPipelineRunWrite(status="success", extpipe_external_id="extId"))
        """
        assert_type(run, "run", [ExtractionPipelineRunCore, Sequence])
        return self._create_multiple(
            list_cls=ExtractionPipelineRunList,
            resource_cls=ExtractionPipelineRun,
            items=run,
            input_resource_cls=ExtractionPipelineRunWrite,
        )


class ExtractionPipelineConfigsAPI(APIClient):
    _RESOURCE_PATH = "/extpipes/config"

    def retrieve(
        self, external_id: str, revision: int | None = None, active_at_time: int | None = None
    ) -> ExtractionPipelineConfig:
        """`Retrieve a specific configuration revision, or the latest by default <https://developer.cognite.com/api#tag/Extraction-Pipelines-Config/operation/getExtPipeConfigRevision>`

        By default the latest configuration revision is retrieved, or you can specify a timestamp or a revision number.

        Args:
            external_id (str): External id of the extraction pipeline to retrieve config from.
            revision (int | None): Optionally specify a revision number to retrieve.
            active_at_time (int | None): Optionally specify a timestamp the configuration revision should be active.

        Returns:
            ExtractionPipelineConfig: Retrieved extraction pipeline configuration revision

        Examples:

            Retrieve latest config revision::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.config.retrieve("extId")
        """
        response = self._get(
            self._RESOURCE_PATH,
            params={"externalId": external_id, "activeAtTime": active_at_time, "revision": revision},
        )
        return ExtractionPipelineConfig._load(response.json(), cognite_client=self._cognite_client)

    def list(self, external_id: str) -> ExtractionPipelineConfigRevisionList:
        """`Retrieve all configuration revisions from an extraction pipeline <https://developer.cognite.com/api#tag/Extraction-Pipelines-Config/operation/listExtPipeConfigRevisions>`

        Args:
            external_id (str): External id of the extraction pipeline to retrieve config from.

        Returns:
            ExtractionPipelineConfigRevisionList: Retrieved extraction pipeline configuration revisions

        Examples:

            Retrieve a list of config revisions::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.config.list("extId")
        """
        response = self._get(f"{self._RESOURCE_PATH}/revisions", params={"externalId": external_id})
        return ExtractionPipelineConfigRevisionList._load(response.json()["items"], cognite_client=self._cognite_client)

    def create(self, config: ExtractionPipelineConfig | ExtractionPipelineConfigWrite) -> ExtractionPipelineConfig:
        """`Create a new configuration revision <https://developer.cognite.com/api#tag/Extraction-Pipelines-Config/operation/createExtPipeConfig>`

        Args:
            config (ExtractionPipelineConfig | ExtractionPipelineConfigWrite): Configuration revision to create.

        Returns:
            ExtractionPipelineConfig: Created extraction pipeline configuration revision

        Examples:

            Create a config revision::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineConfigWrite
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.config.create(ExtractionPipelineConfigWrite(external_id="extId", config="my config contents"))
        """
        if isinstance(config, ExtractionPipelineConfig):
            config = config.as_write()
        response = self._post(self._RESOURCE_PATH, json=config.dump(camel_case=True))
        return ExtractionPipelineConfig._load(response.json(), cognite_client=self._cognite_client)

    def revert(self, external_id: str, revision: int) -> ExtractionPipelineConfig:
        """`Revert to a previous configuration revision <https://developer.cognite.com/api#tag/Extraction-Pipelines-Config/operation/revertExtPipeConfigRevision>`

        Args:
            external_id (str): External id of the extraction pipeline to revert revision for.
            revision (int): Revision to revert to.

        Returns:
            ExtractionPipelineConfig: New latest extraction pipeline configuration revision.

        Examples:

            Revert a config revision::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.extraction_pipelines.config.revert("extId", 5)
        """
        response = self._post(f"{self._RESOURCE_PATH}/revert", json={"externalId": external_id, "revision": revision})
        return ExtractionPipelineConfig._load(response.json(), cognite_client=self._cognite_client)
