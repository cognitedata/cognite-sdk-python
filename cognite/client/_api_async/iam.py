from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Group,
    GroupList,
    GroupWrite,
    SecurityCategory,
    SecurityCategoryList,
    Session,
    SessionList,
)
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncIAMAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/groups"  # Main resource is groups

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.groups = AsyncGroupsAPI(self._config, self._api_version, self._cognite_client)
        self.security_categories = AsyncSecurityCategoriesAPI(self._config, self._api_version, self._cognite_client)
        self.sessions = AsyncSessionsAPI(self._config, self._api_version, self._cognite_client)

    async def token_inspect(self) -> dict[str, Any]:
        """`Get current login status. <https://developer.cognite.com/api#tag/IAM/operation/status>`_"""
        res = await self._get("/login/status")
        return res.json()


class AsyncGroupsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/groups"

    async def list(self, all: bool = False, limit: int | None = DEFAULT_LIMIT_READ) -> GroupList:
        """`List groups <https://developer.cognite.com/api#tag/IAM/operation/getGroups>`_"""
        params = {}
        if all:
            params["all"] = all

        return await self._list(
            list_cls=GroupList,
            resource_cls=Group,
            method="GET",
            limit=limit,
            other_params=params,
        )

    @overload
    async def create(self, group: Sequence[Group] | Sequence[GroupWrite]) -> GroupList: ...

    @overload
    async def create(self, group: Group | GroupWrite) -> Group: ...

    async def create(self, group: Group | GroupWrite | Sequence[Group] | Sequence[GroupWrite]) -> Group | GroupList:
        """`Create one or more groups. <https://developer.cognite.com/api#tag/IAM/operation/createGroups>`_"""
        return await self._create_multiple(
            list_cls=GroupList,
            resource_cls=Group,
            items=group,
        )

    async def delete(self, id: int | Sequence[int]) -> None:
        """`Delete one or more groups <https://developer.cognite.com/api#tag/IAM/operation/deleteGroups>`_"""
        ids = [id] if isinstance(id, int) else id
        await self._delete_multiple(
            identifiers=[{"id": i} for i in ids],
            wrap_ids=False,
        )


class AsyncSecurityCategoriesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/securitycategories"

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> SecurityCategoryList:
        """`List security categories <https://developer.cognite.com/api#tag/IAM/operation/getSecurityCategories>`_"""
        return await self._list(
            list_cls=SecurityCategoryList,
            resource_cls=SecurityCategory,
            method="GET",
            limit=limit,
        )

    @overload
    async def create(self, security_category: Sequence[SecurityCategory]) -> SecurityCategoryList: ...

    @overload
    async def create(self, security_category: SecurityCategory) -> SecurityCategory: ...

    async def create(self, security_category: SecurityCategory | Sequence[SecurityCategory]) -> SecurityCategory | SecurityCategoryList:
        """`Create one or more security categories. <https://developer.cognite.com/api#tag/IAM/operation/createSecurityCategories>`_"""
        return await self._create_multiple(
            list_cls=SecurityCategoryList,
            resource_cls=SecurityCategory,
            items=security_category,
        )

    async def delete(self, id: int | Sequence[int]) -> None:
        """`Delete one or more security categories <https://developer.cognite.com/api#tag/IAM/operation/deleteSecurityCategories>`_"""
        ids = [id] if isinstance(id, int) else id
        await self._delete_multiple(
            identifiers=[{"id": i} for i in ids],
            wrap_ids=False,
        )


class AsyncSessionsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/sessions"

    async def create(self, session_type: str | None = None) -> dict[str, Any]:
        """`Create session <https://developer.cognite.com/api#tag/IAM/operation/createSessions>`_"""
        body = {}
        if session_type:
            body["sessionType"] = session_type
        
        res = await self._post("/sessions", json=body)
        return res.json()

    async def revoke(self, id: int | Sequence[int]) -> dict[str, Any]:
        """`Revoke sessions <https://developer.cognite.com/api#tag/IAM/operation/revokeSessions>`_"""
        ids = [id] if isinstance(id, int) else id
        res = await self._post("/sessions/revoke", json={"items": [{"id": i} for i in ids]})
        return res.json()

    async def list_active(self, status: str | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> dict[str, Any]:
        """`List active sessions <https://developer.cognite.com/api#tag/IAM/operation/listSessions>`_"""
        params = {}
        if status:
            params["status"] = status
        
        res = await self._get("/sessions", params=params)
        return res.json()