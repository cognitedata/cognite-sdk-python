from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    TemplateGroup,
    TemplateGroupList,
    TemplateGroupVersion,
    TemplateGroupVersionList,
    TemplateInstance,
    TemplateInstanceList,
    TemplateInstanceUpdate,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncTemplatesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/templates"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.groups = AsyncTemplateGroupsAPI(self._config, self._api_version, self._cognite_client)
        self.versions = AsyncTemplateGroupVersionsAPI(self._config, self._api_version, self._cognite_client)
        self.instances = AsyncTemplateInstancesAPI(self._config, self._api_version, self._cognite_client)

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> TemplateGroupList:
        """List template groups."""
        return await self._list(
            list_cls=TemplateGroupList,
            resource_cls=TemplateGroup,
            method="GET",
            limit=limit,
        )


class AsyncTemplateGroupsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/templates/groups"

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> TemplateGroupList:
        return await self._list(
            list_cls=TemplateGroupList,
            resource_cls=TemplateGroup,
            method="GET",
            limit=limit,
        )


class AsyncTemplateGroupVersionsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/templates/groups/versions"

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> TemplateGroupVersionList:
        return await self._list(
            list_cls=TemplateGroupVersionList,
            resource_cls=TemplateGroupVersion,
            method="GET",
            limit=limit,
        )


class AsyncTemplateInstancesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/templates/instances"

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> TemplateInstanceList:
        return await self._list(
            list_cls=TemplateInstanceList,
            resource_cls=TemplateInstance,
            method="GET",
            limit=limit,
        )
