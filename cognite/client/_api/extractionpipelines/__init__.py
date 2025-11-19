from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Literal, TypeAlias, overload

from cognite.client._api.extractionpipelines.configs import ExtractionPipelineConfigsAPI
from cognite.client._api.extractionpipelines.runs import ExtractionPipelineRunsAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    ExtractionPipeline,
    ExtractionPipelineList,
    ExtractionPipelineUpdate,
)
from cognite.client.data_classes.extractionpipelines import (
    ExtractionPipelineCore,
    ExtractionPipelineWrite,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


RunStatus: TypeAlias = Literal["success", "failure", "seen"]


class ExtractionPipelinesAPI(APIClient):
    _RESOURCE_PATH = "/extpipes"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.runs = ExtractionPipelineRunsAPI(config, api_version, cognite_client)
        self.config = ExtractionPipelineConfigsAPI(config, api_version, cognite_client)

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> AsyncIterator[ExtractionPipeline]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> AsyncIterator[ExtractionPipelineList]: ...

    async def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> AsyncIterator[ExtractionPipeline | ExtractionPipelineList]:
        """Iterate over extraction pipelines

        Args:
            chunk_size (int | None): Number of extraction pipelines to yield per chunk. Defaults to yielding extraction pipelines one by one.
            limit (int | None): Limits the number of results to be returned. Defaults to yielding all extraction pipelines.

        Yields:
            ExtractionPipeline | ExtractionPipelineList: Yields extraction pipelines one by one or in chunks up to the chunk size.

        """
        async for item in self._list_generator(
            method="GET",
            limit=limit,
            chunk_size=chunk_size,
            resource_cls=ExtractionPipeline,
            list_cls=ExtractionPipelineList,
        ):
            yield item

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> ExtractionPipeline | None:
        """`Retrieve a single extraction pipeline by id. <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/showExtPipe>`_

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

        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, identifiers=identifiers
        )

    async def retrieve_multiple(
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

            Get ExtractionPipelines by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.extraction_pipelines.retrieve_multiple(ids=[1, 2, 3])

            Get assets by external id:

                >>> res = client.extraction_pipelines.retrieve_multiple(external_ids=["abc", "def"], ignore_unknown_ids=True)
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return await self._retrieve_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> ExtractionPipelineList:
        """`List extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/listExtPipes>`_

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

        return await self._list(
            list_cls=ExtractionPipelineList, resource_cls=ExtractionPipeline, method="GET", limit=limit
        )

    @overload
    async def create(self, extraction_pipeline: ExtractionPipeline | ExtractionPipelineWrite) -> ExtractionPipeline: ...

    @overload
    async def create(
        self, extraction_pipeline: Sequence[ExtractionPipeline] | Sequence[ExtractionPipelineWrite]
    ) -> ExtractionPipelineList: ...

    async def create(
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

            Create new extraction pipeline:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> extpipes = [ExtractionPipelineWrite(name="extPipe1",...), ExtractionPipelineWrite(name="extPipe2",...)]
                >>> res = client.extraction_pipelines.create(extpipes)
        """
        assert_type(extraction_pipeline, "extraction_pipeline", [ExtractionPipelineCore, Sequence])

        return await self._create_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            items=extraction_pipeline,
            input_resource_cls=ExtractionPipelineWrite,
        )

    async def delete(
        self, id: int | Sequence[int] | None = None, external_id: str | SequenceNotStr[str] | None = None
    ) -> None:
        """`Delete one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/deleteExtPipes>`_

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
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id), wrap_ids=True, extra_body_fields={}
        )

    @overload
    async def update(
        self, item: ExtractionPipeline | ExtractionPipelineWrite | ExtractionPipelineUpdate
    ) -> ExtractionPipeline: ...

    @overload
    async def update(
        self, item: Sequence[ExtractionPipeline | ExtractionPipelineWrite | ExtractionPipelineUpdate]
    ) -> ExtractionPipelineList: ...

    async def update(
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

            Update an extraction pipeline that you have fetched. This will perform a full update of the extraction pipeline:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ExtractionPipelineUpdate
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> update = ExtractionPipelineUpdate(id=1)
                >>> update.description.set("Another new extpipe")
                >>> res = client.extraction_pipelines.update(update)
        """
        return await self._update_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            update_cls=ExtractionPipelineUpdate,
            items=item,
            mode=mode,
        )
