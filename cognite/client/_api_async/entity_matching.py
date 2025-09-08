from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    EntityMatchingModel,
    EntityMatchingModelList,
    EntityMatchingModelUpdate,
    ContextualizationJob,
    ContextualizationJobList,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncEntityMatchingAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/context/entitymatching"

    async def fit(
        self,
        sources: list[dict[str, Any]],
        targets: list[dict[str, Any]],
        true_matches: list[dict[str, Any]] | None = None,
        match_fields: list[tuple[str, str]] | None = None,
        name: str | None = None,
        description: str | None = None,
        external_id: str | None = None,
    ) -> EntityMatchingModel:
        """Train a model for entity matching."""
        body = {
            "sources": sources,
            "targets": targets,
            "trueMatches": true_matches or [],
            "matchFields": [{"source": s, "target": t} for s, t in (match_fields or [])],
            "name": name,
            "description": description,
            "externalId": external_id,
        }
        body = {k: v for k, v in body.items() if v is not None}
        
        res = await self._post(url_path=self._RESOURCE_PATH, json=body)
        return EntityMatchingModel._load(res.json(), cognite_client=self._cognite_client)

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> EntityMatchingModel | None:
        """Retrieve entity matching model."""
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=EntityMatchingModelList,
            resource_cls=EntityMatchingModel,
            identifiers=identifiers,
        )

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> EntityMatchingModelList:
        """List entity matching models."""
        return await self._list(
            list_cls=EntityMatchingModelList,
            resource_cls=EntityMatchingModel,
            method="GET",
            limit=limit,
        )

    async def delete(self, id: int | Sequence[int] | None = None, external_id: str | SequenceNotStr[str] | None = None) -> None:
        """Delete entity matching models."""
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
        )

    async def predict(
        self, 
        id: int | None = None,
        external_id: str | None = None,
        sources: list[dict[str, Any]] | None = None,
        targets: list[dict[str, Any]] | None = None,
        num_matches: int = 1,
        score_threshold: float | None = None,
    ) -> dict[str, Any]:
        """Predict entity matches."""
        if id is not None:
            path = f"{self._RESOURCE_PATH}/{id}/predict"
        else:
            path = f"{self._RESOURCE_PATH}/predict"
            
        body = {
            "externalId": external_id,
            "sources": sources or [],
            "targets": targets or [],
            "numMatches": num_matches,
            "scoreThreshold": score_threshold,
        }
        body = {k: v for k, v in body.items() if v is not None}
        
        res = await self._post(url_path=path, json=body)
        return res.json()
