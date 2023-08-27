from __future__ import annotations

from typing import Sequence

from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
from cognite.client.data_classes.user_profiles import UserProfile, UserProfileList
from cognite.client.utils._identifier import UserIdentifierSequence


class UserProfilesAPI(APIClient):
    _RESOURCE_PATH = "/profiles"

    def me(self) -> UserProfile:
        return UserProfile._load(self._get(self._RESOURCE_PATH + "/me").json())

    def list(self, limit: int | None = LIST_LIMIT_DEFAULT, initial_cursor: str | None = None) -> UserProfileList:
        return self._list(
            "GET",
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            limit=limit,
            initial_cursor=initial_cursor,
        )

    def retrieve(self, user_identifier: str) -> UserProfile | None:
        identifier = UserIdentifierSequence.load(user_identifier).as_singleton()
        return self._retrieve_multiple(
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            identifiers=identifier,
        )

    def retrieve_multiple(self, user_identifiers: Sequence[str]) -> UserProfileList:
        identifiers = UserIdentifierSequence.load(user_identifiers)
        return self._retrieve_multiple(
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            identifiers=identifiers,
        )

    def search(self, name: str, limit: int = 25) -> UserProfileList:
        # prefix on name
        return
