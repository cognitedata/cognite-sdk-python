from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Sequence, overload

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
from cognite.client.data_classes.extractionpipelines import StringFilter
from cognite.client.utils._auxiliary import assert_type
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class ExtractionPipelinesAPI(APIClient):
    _RESOURCE_PATH = "/extpipes"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.runs = ExtractionPipelineRunsAPI(config, api_version, cognite_client)
        self.config = ExtractionPipelineConfigsAPI(config, api_version, cognite_client)

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
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.retrieve(id=1)

            Get extraction pipeline by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.retrieve(external_id="1")
        """

        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, identifiers=identifiers
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtractionPipelineList:
        """`Retrieve multiple extraction pipelines by ids and external ids. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/byidsExtPipes>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (Sequence[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            ExtractionPipelineList: The requested ExtractionPipelines.

        Examples:

            Get ExtractionPipelines by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.retrieve_multiple(ids=[1, 2, 3])

            Get assets by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.retrieve_multiple(external_ids=["abc", "def"], ignore_unknown_ids=True)
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
                >>> c = CogniteClient()
                >>> ep_list = c.extraction_pipelines.list(limit=5)
        """

        return self._list(list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, method="GET", limit=limit)

    @overload
    def create(self, extraction_pipeline: ExtractionPipeline) -> ExtractionPipeline:
        ...

    @overload
    def create(self, extraction_pipeline: Sequence[ExtractionPipeline]) -> ExtractionPipelineList:
        ...

    def create(
        self, extraction_pipeline: ExtractionPipeline | Sequence[ExtractionPipeline]
    ) -> ExtractionPipeline | ExtractionPipelineList:
        """`Create one or more extraction pipelines. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/createExtPipes>`_

        You can create an arbitrary number of extraction pipelines, and the SDK will split the request into multiple requests if necessary.

        Args:
            extraction_pipeline (ExtractionPipeline | Sequence[ExtractionPipeline]): Extraction pipeline or list of extraction pipelines to create.

        Returns:
            ExtractionPipeline | ExtractionPipelineList: Created extraction pipeline(s)

        Examples:

            Create new extraction pipeline::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipeline
                >>> c = CogniteClient()
                >>> extpipes = [ExtractionPipeline(name="extPipe1",...), ExtractionPipeline(name="extPipe2",...)]
                >>> res = c.extraction_pipelines.create(extpipes)
        """
        assert_type(extraction_pipeline, "extraction_pipeline", [ExtractionPipeline, Sequence])
        return self._create_multiple(
            list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, items=extraction_pipeline
        )

    def delete(self, id: int | Sequence[int] | None = None, external_id: str | Sequence[str] | None = None) -> None:
        """`Delete one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/deleteExtPipes>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | Sequence[str] | None): External ID or list of external ids

        Examples:

            Delete extraction pipelines by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.extraction_pipelines.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(id, external_id), wrap_ids=True, extra_body_fields={})

    @overload
    def update(self, item: ExtractionPipeline | ExtractionPipelineUpdate) -> ExtractionPipeline:
        ...

    @overload
    def update(self, item: Sequence[ExtractionPipeline | ExtractionPipelineUpdate]) -> ExtractionPipelineList:
        ...

    def update(
        self,
        item: ExtractionPipeline | ExtractionPipelineUpdate | Sequence[ExtractionPipeline | ExtractionPipelineUpdate],
    ) -> ExtractionPipeline | ExtractionPipelineList:
        """`Update one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/updateExtPipes>`_

        Args:
            item (ExtractionPipeline | ExtractionPipelineUpdate | Sequence[ExtractionPipeline | ExtractionPipelineUpdate]): Extraction pipeline(s) to update

        Returns:
            ExtractionPipeline | ExtractionPipelineList: Updated extraction pipeline(s)

        Examples:

            Update an extraction pipeline that you have fetched. This will perform a full update of the extraction pipeline::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> update = ExtractionPipelineUpdate(id=1)
                >>> update.description.set("Another new extpipe")
                >>> res = c.extraction_pipelines.update(update)
        """
        return self._update_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            update_cls=ExtractionPipelineUpdate,
            items=item,
        )


class ExtractionPipelineRunsAPI(APIClient):
    _RESOURCE_PATH = "/extpipes/runs"

    def list(
        self,
        external_id: str,
        statuses: Sequence[str] | None = None,
        message_substring: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ExtractionPipelineRunList:
        """`List runs for an extraction pipeline with given external_id <https://developer.cognite.com/api#tag/Extraction-Pipelines-Runs/operation/filterRuns>`_

        Args:
            external_id (str): Extraction pipeline external Id.
            statuses (Sequence[str] | None): One or more among "success" / "failure" / "seen".
            message_substring (str | None): Failure message part.
            created_time (dict[str, Any] | TimestampRange | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
            limit (int | None): Maximum number of ExtractionPipelines to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ExtractionPipelineRunList: List of requested extraction pipeline runs

        Examples:

            List extraction pipeline runs::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> runsList = c.extraction_pipelines.runs.list(external_id="test ext id", limit=5)

            Filter extraction pipeline runs on a given status::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> runsList = c.extraction_pipelines.runs.list(external_id="test ext id", statuses=["seen"], statuslimit=5)
        """

        if statuses is not None or message_substring is not None or created_time is not None:
            filter = ExtractionPipelineRunFilter(
                external_id=external_id,
                statuses=statuses,
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
    def create(self, run: ExtractionPipelineRun) -> ExtractionPipelineRun:
        ...

    @overload
    def create(self, run: Sequence[ExtractionPipelineRun]) -> ExtractionPipelineRunList:
        ...

    def create(
        self, run: ExtractionPipelineRun | Sequence[ExtractionPipelineRun]
    ) -> ExtractionPipelineRun | ExtractionPipelineRunList:
        """`Create one or more extraction pipeline runs. <https://developer.cognite.com/api#tag/Extraction-Pipelines-Runs/operation/createRuns>`_

        You can create an arbitrary number of extraction pipeline runs, and the SDK will split the request into multiple requests.

        Args:
            run (ExtractionPipelineRun | Sequence[ExtractionPipelineRun]): Extraction pipeline or list of extraction pipeline runs to create.

        Returns:
            ExtractionPipelineRun | ExtractionPipelineRunList: Created extraction pipeline run(s)

        Examples:

            Report a new extraction pipeline run::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineRun
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.runs.create(
                ...     ExtractionPipelineRun(status="success", extpipe_external_id="extId"))
        """
        assert_type(run, "run", [ExtractionPipelineRun, Sequence])
        return self._create_multiple(list_cls=ExtractionPipelineRunList, resource_cls=ExtractionPipelineRun, items=run)


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
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.config.retrieve("extId")
        """
        response = self._get(
            "/extpipes/config", params={"externalId": external_id, "activeAtTime": active_at_time, "revision": revision}
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
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.config.list("extId")
        """
        response = self._get("/extpipes/config/revisions", params={"externalId": external_id})
        return ExtractionPipelineConfigRevisionList._load(response.json()["items"], cognite_client=self._cognite_client)

    def create(self, config: ExtractionPipelineConfig) -> ExtractionPipelineConfig:
        """`Create a new configuration revision <https://developer.cognite.com/api#tag/Extraction-Pipelines-Config/operation/createExtPipeConfig>`

        Args:
            config (ExtractionPipelineConfig): Configuration revision to create.

        Returns:
            ExtractionPipelineConfig: Created extraction pipeline configuration revision

        Examples:

            Create a config revision::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineConfig
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.config.create(ExtractionPipelineConfig(external_id="extId", config="my config contents"))
        """
        response = self._post("/extpipes/config", json=config.dump(camel_case=True))
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
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.config.revert("extId", 5)
        """
        response = self._post("/extpipes/config/revert", json={"externalId": external_id, "revision": revision})
        return ExtractionPipelineConfig._load(response.json(), cognite_client=self._cognite_client)
