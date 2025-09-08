from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Annotation,
    AnnotationFilter,
    AnnotationList,
    AnnotationUpdate,
    AnnotationWrite,
    TimestampRange,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncAnnotationsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/annotations"

    async def list(
        self,
        annotated_resource_type: str | None = None,
        annotated_resource_ids: Sequence[dict[str, Any]] | None = None,
        status: str | None = None,
        creating_app: str | None = None,
        creating_app_version: str | None = None,
        creating_user: str | None = None,
        annotation_type: str | None = None,
        data: dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> AnnotationList:
        """`List annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsList>`_"""
        filter = AnnotationFilter(
            annotated_resource_type=annotated_resource_type,
            annotated_resource_ids=annotated_resource_ids,
            status=status,
            creating_app=creating_app,
            creating_app_version=creating_app_version,
            creating_user=creating_user,
            annotation_type=annotation_type,
            data=data,
        ).dump(camel_case=True)

        return await self._list(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            method="POST",
            limit=limit,
            filter=filter,
        )

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> Annotation | None:
        """`Retrieve a single annotation by id <https://developer.cognite.com/api#tag/Annotations/operation/annotationsByIds>`_"""
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> AnnotationList:
        """`Retrieve multiple annotations by id <https://developer.cognite.com/api#tag/Annotations/operation/annotationsByIds>`_"""
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    @overload
    async def create(self, annotation: Sequence[Annotation] | Sequence[AnnotationWrite]) -> AnnotationList: ...

    @overload
    async def create(self, annotation: Annotation | AnnotationWrite) -> Annotation: ...

    async def create(self, annotation: Annotation | AnnotationWrite | Sequence[Annotation] | Sequence[AnnotationWrite]) -> Annotation | AnnotationList:
        """`Create one or more annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsCreate>`_"""
        return await self._create_multiple(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            items=annotation,
        )

    async def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsDelete>`_"""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(self, item: Sequence[Annotation | AnnotationUpdate]) -> AnnotationList: ...

    @overload
    async def update(self, item: Annotation | AnnotationUpdate) -> Annotation: ...

    async def update(self, item: Annotation | AnnotationUpdate | Sequence[Annotation | AnnotationUpdate]) -> Annotation | AnnotationList:
        """`Update one or more annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsUpdate>`_"""
        return await self._update_multiple(
            list_cls=AnnotationList,
            resource_cls=Annotation,
            update_cls=AnnotationUpdate,
            items=item,
        )

    @overload
    async def upsert(self, item: Sequence[Annotation | AnnotationWrite], mode: Literal["patch", "replace"] = "patch") -> AnnotationList: ...

    @overload 
    async def upsert(self, item: Annotation | AnnotationWrite, mode: Literal["patch", "replace"] = "patch") -> Annotation: ...

    async def upsert(
        self,
        item: Annotation | AnnotationWrite | Sequence[Annotation | AnnotationWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> Annotation | AnnotationList:
        """`Upsert annotations <https://developer.cognite.com/api#tag/Annotations/operation/annotationsCreate>`_"""
        return await self._upsert_multiple(
            items=item,
            list_cls=AnnotationList,
            resource_cls=Annotation,
            update_cls=AnnotationUpdate,
            mode=mode,
        )
