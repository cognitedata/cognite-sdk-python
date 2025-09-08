from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Document,
    DocumentList,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncDocumentsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/documents"

    async def list(
        self, 
        external_id_prefix: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ
    ) -> DocumentList:
        """`List documents <https://developer.cognite.com/api#tag/Documents/operation/listDocuments>`_"""
        filter = {}
        if external_id_prefix is not None:
            filter["externalIdPrefix"] = external_id_prefix
        
        return await self._list(
            list_cls=DocumentList,
            resource_cls=Document,
            method="POST",
            limit=limit,
            filter=filter,
        )

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> Document | None:
        """`Retrieve a single document by id <https://developer.cognite.com/api#tag/Documents/operation/byIdsDocuments>`_"""
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=DocumentList,
            resource_cls=Document,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> DocumentList:
        """`Retrieve multiple documents by id <https://developer.cognite.com/api#tag/Documents/operation/byIdsDocuments>`_"""
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=DocumentList,
            resource_cls=Document,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    async def search(
        self,
        query: str,
        filter: dict[str, Any] | None = None,
        highlight: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> DocumentList:
        """`Search for documents <https://developer.cognite.com/api#tag/Documents/operation/searchDocuments>`_"""
        body = {
            "search": {"query": query},
            "highlight": highlight,
            "limit": limit,
        }
        if filter:
            body["filter"] = filter
            
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/search", json=body)
        return DocumentList._load(res.json()["items"], cognite_client=self._cognite_client)

    async def aggregate(
        self, 
        filter: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """`Aggregate documents <https://developer.cognite.com/api#tag/Documents/operation/aggregateDocuments>`_"""
        body = {"filter": filter or {}}
        res = await self._post(url_path=f"{self._RESOURCE_PATH}/aggregate", json=body)
        return res.json()
