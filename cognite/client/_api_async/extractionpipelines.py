from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    ExtractionPipeline,
    ExtractionPipelineList,
    ExtractionPipelineUpdate,
    ExtractionPipelineWrite,
    TimestampRange,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncExtractionPipelinesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/extpipes"

    async def list(
        self,
        name: str | None = None,
        description: str | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        schedule: dict[str, Any] | None = None,
        source: str | None = None,
        last_seen: dict[str, Any] | TimestampRange | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ExtractionPipelineList:
        """`List extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/listExtPipes>`_"""
        filter = {
            "name": name,
            "description": description,
            "dataSetIds": data_set_ids,
            "dataSetExternalIds": data_set_external_ids,
            "schedule": schedule,
            "source": source,
            "lastSeen": last_seen,
            "createdTime": created_time,
            "lastUpdatedTime": last_updated_time,
            "externalIdPrefix": external_id_prefix,
        }
        # Remove None values
        filter = {k: v for k, v in filter.items() if v is not None}

        return await self._list(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            method="POST",
            limit=limit,
            filter=filter,
        )

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> ExtractionPipeline | None:
        """`Retrieve a single extraction pipeline by id <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/byIdsExtPipes>`_"""
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtractionPipelineList:
        """`Retrieve multiple extraction pipelines by id <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/byIdsExtPipes>`_"""
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    @overload
    async def create(self, extraction_pipeline: Sequence[ExtractionPipeline] | Sequence[ExtractionPipelineWrite]) -> ExtractionPipelineList: ...

    @overload
    async def create(self, extraction_pipeline: ExtractionPipeline | ExtractionPipelineWrite) -> ExtractionPipeline: ...

    async def create(self, extraction_pipeline: ExtractionPipeline | ExtractionPipelineWrite | Sequence[ExtractionPipeline] | Sequence[ExtractionPipelineWrite]) -> ExtractionPipeline | ExtractionPipelineList:
        """`Create one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/createExtPipes>`_"""
        return await self._create_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            items=extraction_pipeline,
        )

    async def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/deleteExtPipes>`_"""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(self, item: Sequence[ExtractionPipeline | ExtractionPipelineUpdate]) -> ExtractionPipelineList: ...

    @overload
    async def update(self, item: ExtractionPipeline | ExtractionPipelineUpdate) -> ExtractionPipeline: ...

    async def update(self, item: ExtractionPipeline | ExtractionPipelineUpdate | Sequence[ExtractionPipeline | ExtractionPipelineUpdate]) -> ExtractionPipeline | ExtractionPipelineList:
        """`Update one or more extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/updateExtPipes>`_"""
        return await self._update_multiple(
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            update_cls=ExtractionPipelineUpdate,
            items=item,
        )

    @overload
    async def upsert(self, item: Sequence[ExtractionPipeline | ExtractionPipelineWrite], mode: Literal["patch", "replace"] = "patch") -> ExtractionPipelineList: ...

    @overload 
    async def upsert(self, item: ExtractionPipeline | ExtractionPipelineWrite, mode: Literal["patch", "replace"] = "patch") -> ExtractionPipeline: ...

    async def upsert(
        self,
        item: ExtractionPipeline | ExtractionPipelineWrite | Sequence[ExtractionPipeline | ExtractionPipelineWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> ExtractionPipeline | ExtractionPipelineList:
        """`Upsert extraction pipelines <https://developer.cognite.com/api#tag/Extraction-Pipelines/operation/createExtPipes>`_"""
        return await self._upsert_multiple(
            items=item,
            list_cls=ExtractionPipelineList,
            resource_cls=ExtractionPipeline,
            update_cls=ExtractionPipelineUpdate,
            mode=mode,
        )
