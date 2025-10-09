from __future__ import annotations

from typing import overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.user_profiles import UserProfile, UserProfileList, UserProfilesConfiguration
from cognite.client.utils._identifier import UserIdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class UserProfilesAPI(APIClient):
    _RESOURCE_PATH = "/profiles"

    async def enable(self) -> UserProfilesConfiguration:
        """Enable user profiles for the project"""
        res = await self._post("/update", json={"update": {"userProfilesConfiguration": {"set": {"enabled": True}}}})
        return UserProfilesConfiguration._load(res.json()["userProfilesConfiguration"])

    async def disable(self) -> UserProfilesConfiguration:
        """Disable user profiles for the project"""
        res = await self._post("/update", json={"update": {"userProfilesConfiguration": {"set": {"enabled": False}}}})
        return UserProfilesConfiguration._load(res.json()["userProfilesConfiguration"])

    async def me(self) -> UserProfile:
        """`Retrieve your own user profile <https://developer.cognite.com/api#tag/User-profiles/operation/getRequesterUserProfile>`_

        Retrieves the user profile of the principal issuing the request, i.e. the principal *this* AsyncCogniteClient was instantiated with.

        Returns:
            UserProfile: Your own user profile.

        Raises:
            CogniteAPIError: If this principal doesn't have a user profile, you get a not found (404) response code.

        Examples:

            Get your own user profile:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.user_profiles.me()
        """
        return UserProfile._load((await self._get(self._RESOURCE_PATH + "/me")).json())

    @overload
    async def retrieve(self, user_identifier: str) -> UserProfile | None: ...

    @overload
    async def retrieve(self, user_identifier: SequenceNotStr[str]) -> UserProfileList: ...

    async def retrieve(self, user_identifier: str | SequenceNotStr[str]) -> UserProfile | UserProfileList | None:
        """`Retrieve user profiles by user identifier. <https://developer.cognite.com/api#tag/User-profiles/operation/getUserProfilesByIds>`_

        Retrieves one or more user profiles indexed by the user identifier in the same CDF project.

        Args:
            user_identifier (str | SequenceNotStr[str]): The single user identifier (or sequence of) to retrieve profile(s) for.

        Returns:
            UserProfile | UserProfileList | None: UserProfileList if a sequence of user identifier were requested, else UserProfile. If a single user identifier is requested and it is not found, None is returned.

        Raises:
            CogniteNotFoundError: A sequences of user identifiers were requested, but one or more does not exist.

        Examples:

            Get a single user profile:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.user_profiles.retrieve("foo")

            Get multiple user profiles:

                >>> res = client.iam.user_profiles.retrieve(["bar", "baz"])
        """
        identifiers = UserIdentifierSequence.load(user_identifier)
        return await self._retrieve_multiple(
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            identifiers=identifiers,
        )

    async def search(self, name: str, limit: int = DEFAULT_LIMIT_READ) -> UserProfileList:
        """`Search for user profiles <https://developer.cognite.com/api#tag/User-profiles/operation/userProfilesSearch>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, as the result set ordering and match criteria threshold may change over time.

        Args:
            name (str): Prefix search on name.
            limit (int): Maximum number of results to return.

        Returns:
            UserProfileList: User profiles search result

        Examples:

            Search for users with first (or second...) name starting with "Alex":

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.user_profiles.search(name="Alex")
        """
        return await self._search(
            list_cls=UserProfileList,
            search={"name": name},
            filter={},
            limit=limit,
        )

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> UserProfileList:
        """`List user profiles <https://developer.cognite.com/api#tag/User-profiles/operation/listUserProfiles>`_

        List all user profiles in the current CDF project. The results are ordered alphabetically by name.

        Args:
            limit (int | None): Maximum number of user profiles to return. Defaults to 25. Set to -1, float("inf") or None to return all.

        Returns:
            UserProfileList: List of user profiles.

        Examples:

            List all user profiles:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.user_profiles.list(limit=None)
        """
        return await self._list(
            "GET",
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            limit=limit,
        )
