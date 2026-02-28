"""
===============================================================================
990fd1dcf9185a7f006b22b1245e506e
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.extractionpipelines.configs import SyncExtractionPipelineConfigsAPI
from cognite.client._sync_api.extractionpipelines.runs import SyncExtractionPipelineRunsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import ExtractionPipeline, ExtractionPipelineList, ExtractionPipelineUpdate
from cognite.client.data_classes.extractionpipelines import ExtractionPipelineWrite
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncExtractionPipelinesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.runs = SyncExtractionPipelineRunsAPI(async_client)
        self.config = SyncExtractionPipelineConfigsAPI(async_client)

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[ExtractionPipeline]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[ExtractionPipelineList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[ExtractionPipeline] | Iterator[ExtractionPipelineList]:
        """
        Iterate over extraction pipelines

        Args:
            chunk_size (int | None): Number of extraction pipelines to yield per chunk. Defaults to yielding extraction pipelines one by one.
            limit (int | None): Limits the number of results to be returned. Defaults to yielding all extraction pipelines.

        Yields:
            ExtractionPipeline | ExtractionPipelineList: Yields extraction pipelines one by one or in chunks up to the chunk size.
        """  # noqa: DOC404
        yield from SyncIterator(self.__async_client.extraction_pipelines(chunk_size=chunk_size, limit=limit))  # type: ignore [misc]

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> ExtractionPipeline | None:
        """
        `Retrieve a single extraction pipeline by id. <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines/operation/showExtPipe>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            ExtractionPipeline | None: Requested extraction pipeline or None if it does not exist.

        Examples:

            Get extraction pipeline by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.extraction_pipelines.retrieve(id=1)

            Get extraction pipeline by external id:

                >>> res = client.extraction_pipelines.retrieve(external_id="1")
        """
        return run_sync(self.__async_client.extraction_pipelines.retrieve(id=id, external_id=external_id))

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtractionPipelineList:
        """
        `Retrieve multiple extraction pipelines by ids and external ids. <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines/operation/byidsExtPipes>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            ExtractionPipelineList: The requested ExtractionPipelines.

        Examples:

            Get ExtractionPipelines by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.extraction_pipelines.retrieve_multiple(ids=[1, 2, 3])

            Get assets by external id:

                >>> res = client.extraction_pipelines.retrieve_multiple(external_ids=["abc", "def"], ignore_unknown_ids=True)
        """
        return run_sync(
            self.__async_client.extraction_pipelines.retrieve_multiple(
                ids=ids, external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> ExtractionPipelineList:
        """
        `List extraction pipelines <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines/operation/listExtPipes>`_

        Args:
            limit (int | None): Maximum number of ExtractionPipelines to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ExtractionPipelineList: List of requested ExtractionPipelines

        Examples:

            List ExtractionPipelines:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> ep_list = client.extraction_pipelines.list(limit=5)
        """
        return run_sync(self.__async_client.extraction_pipelines.list(limit=limit))

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
        """
        `Create one or more extraction pipelines. <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines/operation/createExtPipes>`_

        You can create an arbitrary number of extraction pipelines, and the SDK will split the request into multiple requests if necessary.

        Args:
            extraction_pipeline (ExtractionPipeline | ExtractionPipelineWrite | Sequence[ExtractionPipeline] | Sequence[ExtractionPipelineWrite]): Extraction pipeline or list of extraction pipelines to create.

        Returns:
            ExtractionPipeline | ExtractionPipelineList: Created extraction pipeline(s)

        Examples:

            Create new extraction pipeline:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> extpipes = [ExtractionPipelineWrite(name="extPipe1",...), ExtractionPipelineWrite(name="extPipe2",...)]
                >>> res = client.extraction_pipelines.create(extpipes)
        """
        return run_sync(self.__async_client.extraction_pipelines.create(extraction_pipeline=extraction_pipeline))

    def delete(
        self, id: int | Sequence[int] | None = None, external_id: str | SequenceNotStr[str] | None = None
    ) -> None:
        """
        `Delete one or more extraction pipelines <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines/operation/deleteExtPipes>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids

        Examples:

            Delete extraction pipelines by id or external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.extraction_pipelines.delete(id=[1,2,3], external_id="3")
        """
        return run_sync(self.__async_client.extraction_pipelines.delete(id=id, external_id=external_id))

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
        """
        `Update one or more extraction pipelines <https://api-docs.cognite.com/20230101/tag/Extraction-Pipelines/operation/updateExtPipes>`_

        Args:
            item (ExtractionPipeline | ExtractionPipelineWrite | ExtractionPipelineUpdate | Sequence[ExtractionPipeline | ExtractionPipelineWrite | ExtractionPipelineUpdate]): Extraction pipeline(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (ExtractionPipeline or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            ExtractionPipeline | ExtractionPipelineList: Updated extraction pipeline(s)

        Examples:

            Update an extraction pipeline that you have fetched. This will perform a full update of the extraction pipeline:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineUpdate
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> update = ExtractionPipelineUpdate(id=1)
                >>> update.description.set("Another new extpipe")
                >>> res = client.extraction_pipelines.update(update)
        """
        return run_sync(self.__async_client.extraction_pipelines.update(item=item, mode=mode))  # type: ignore [call-overload]
