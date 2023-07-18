from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Sequence, Union, overload

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
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
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class ExtractionPipelinesAPI(APIClient):
    _RESOURCE_PATH = "/extpipes"

    def __init__(self, config: ClientConfig, api_version: Optional[str], cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.runs = ExtractionPipelineRunsAPI(config, api_version, cognite_client)
        self.config = ExtractionPipelineConfigsAPI(config, api_version, cognite_client)

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[ExtractionPipeline]:
        """`Retrieve a single extraction pipeline by id. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/showExtPipe>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[ExtractionPipeline]: Requested extraction pipeline or None if it does not exist.

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
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtractionPipelineList:
        """`Retrieve multiple extraction pipelines by ids and external ids. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/byidsExtPipes>`_

        Args:
            ids (Sequence[int], optional): IDs
            external_ids (Sequence[str], optional): External IDs
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

    def list(self, limit: int = LIST_LIMIT_DEFAULT) -> ExtractionPipelineList:
        """`List extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/listExtPipes>`_

        Args:
            limit (int, optional): Maximum number of ExtractionPipelines to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

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
        self, extraction_pipeline: Union[ExtractionPipeline, Sequence[ExtractionPipeline]]
    ) -> Union[ExtractionPipeline, ExtractionPipelineList]:
        """`Create one or more extraction pipelines. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/createExtPipes>`_

        You can create an arbitrary number of extraction pipelines, and the SDK will split the request into multiple requests if necessary.

        Args:
            extraction_pipeline (Union[ExtractionPipeline, List[ExtractionPipeline]]): Extraction pipeline or list of extraction pipelines to create.

        Returns:
            Union[ExtractionPipeline, ExtractionPipelineList]: Created extraction pipeline(s)

        Examples:

            Create new extraction pipeline::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipeline
                >>> c = CogniteClient()
                >>> extpipes = [ExtractionPipeline(name="extPipe1",...), ExtractionPipeline(name="extPipe2",...)]
                >>> res = c.extraction_pipelines.create(extpipes)
        """
        utils._auxiliary.assert_type(extraction_pipeline, "extraction_pipeline", [ExtractionPipeline, Sequence])
        return self._create_multiple(
            list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, items=extraction_pipeline
        )

    def delete(
        self, id: Optional[Union[int, Sequence[int]]] = None, external_id: Optional[Union[str, Sequence[str]]] = None
    ) -> None:
        """`Delete one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/deleteExtPipes>`_

        Args:
            id (Union[int, Sequence[int]): Id or list of ids
            external_id (Union[str, Sequence[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete extraction pipelines by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.extraction_pipelines.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(id, external_id), wrap_ids=True, extra_body_fields={})

    @overload
    def update(self, item: Union[ExtractionPipeline, ExtractionPipelineUpdate]) -> ExtractionPipeline:
        ...

    @overload
    def update(self, item: Sequence[Union[ExtractionPipeline, ExtractionPipelineUpdate]]) -> ExtractionPipelineList:
        ...

    def update(
        self,
        item: Union[
            ExtractionPipeline, ExtractionPipelineUpdate, Sequence[Union[ExtractionPipeline, ExtractionPipelineUpdate]]
        ],
    ) -> Union[ExtractionPipeline, ExtractionPipelineList]:
        """`Update one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/updateExtPipes>`_

        Args:
            item (Union[ExtractionPipeline, ExtractionPipelineUpdate, Sequence[Union[ExtractionPipeline, ExtractionPipelineUpdate]]]): Extraction pipeline(s) to update

        Returns:
            Union[ExtractionPipeline, ExtractionPipelineList]: Updated extraction pipeline(s)

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
        statuses: Optional[Sequence[str]] = None,
        message_substring: Optional[str] = None,
        created_time: Optional[Union[Dict[str, Any], TimestampRange]] = None,
        limit: int = LIST_LIMIT_DEFAULT,
    ) -> ExtractionPipelineRunList:
        """`List runs for an extraction pipeline with given external_id <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/filterRuns>`_

        Args:
            external_id (str): Extraction pipeline external Id.
            statuses (Sequence[str]): One or more among "success" / "failure" / "seen".
            message_substring (str): Failure message part.
            created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
            limit (int, optional): Maximum number of ExtractionPipelines to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

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
        self, run: Union[ExtractionPipelineRun, Sequence[ExtractionPipelineRun]]
    ) -> Union[ExtractionPipelineRun, ExtractionPipelineRunList]:
        """`Create one or more extraction pipeline runs. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/createRuns>`_

        You can create an arbitrary number of extraction pipeline runs, and the SDK will split the request into multiple requests.

        Args:
            run (Union[ExtractionPipelineRun, Sequence[ExtractionPipelineRun]]): Extraction pipeline or list of extraction pipeline runs to create.

        Returns:
            Union[ExtractionPipelineRun, ExtractionPipelineRunList]: Created extraction pipeline run(s)

        Examples:

            Report a new extraction pipeline run::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineRun
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.runs.create(
                ...     ExtractionPipelineRun(status="success", extpipe_external_id="extId"))
        """
        utils._auxiliary.assert_type(run, "run", [ExtractionPipelineRun, Sequence])
        return self._create_multiple(list_cls=ExtractionPipelineRunList, resource_cls=ExtractionPipelineRun, items=run)


class ExtractionPipelineConfigsAPI(APIClient):
    _RESOURCE_PATH = "/extpipes/config"

    def retrieve(
        self, external_id: str, revision: Optional[int] = None, active_at_time: Optional[int] = None
    ) -> ExtractionPipelineConfig:
        """`Retrieve a specific configuration revision, or the latest by default <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/getExtPipeConfigRevision>`

        By default the latest configuration revision is retrieved, or you can specify a timestamp or a revision number.

        Args:
            external_id (str): External id of the extraction pipeline to retrieve config from.
            revision (Optional[int]): Optionally specify a revision number to retrieve.
            active_at_time (Optional[int]): Optionally specify a timestamp the configuration revision should be active.

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
        """`Retrieve all configuration revisions from an extraction pipeline <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/listExtPipeConfigRevisions>`

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
        """`Create a new configuration revision <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/createExtPipeConfig>`

        Args:
            config (ExtractionPipelineConfig): Configuration revision to create.

        Returns:
            ExtractionPipelineConfig: Created extraction pipeline configuration revision

        Examples:

            Create a config revision::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.config import ExtractionPipelineConfig
                >>> c = CogniteClient()
                >>> res = c.extraction_pipelines.config.create(ExtractionPipelineConfig(external_id="extId", config="my config contents"))
        """
        response = self._post("/extpipes/config", json=config.dump(camel_case=True))
        return ExtractionPipelineConfig._load(response.json(), cognite_client=self._cognite_client)

    def revert(self, external_id: str, revision: int) -> ExtractionPipelineConfig:
        """`Revert to a previous configuration revision <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/createExtPipeConfig>`

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
