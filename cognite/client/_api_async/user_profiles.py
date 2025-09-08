from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    UserProfile,
    UserProfileList,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncUserProfilesAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/profiles"

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> UserProfileList:
        """`List user profiles <https://developer.cognite.com/api#tag/User-Profiles/operation/listUserProfiles>`_"""
        return await self._list(
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            method="GET",
            limit=limit,
        )

    async def retrieve(self, user_identifier: str) -> UserProfile | None:
        """`Retrieve a single user profile by user identifier <https://developer.cognite.com/api#tag/User-Profiles/operation/searchUserProfiles>`_"""
        try:
            res = await self._post(
                url_path=f"{self._RESOURCE_PATH}/byids",
                json={"items": [{"userIdentifier": user_identifier}]}
            )
            items = res.json()["items"]
            if items:
                return UserProfile._load(items[0], cognite_client=self._cognite_client)
            return None
        except Exception:
            return None

    async def search(
        self, 
        name: str | None = None, 
        job_title: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ
    ) -> UserProfileList:
        """`Search for user profiles <https://developer.cognite.com/api#tag/User-Profiles/operation/searchUserProfiles>`_"""
        search_body = {}
        if name is not None:
            search_body["name"] = name
        if job_title is not None:
            search_body["jobTitle"] = job_title
        
        res = await self._post(
            url_path=f"{self._RESOURCE_PATH}/search",
            json={"search": search_body, "limit": limit}
        )
        return UserProfileList._load(res.json()["items"], cognite_client=self._cognite_client)
