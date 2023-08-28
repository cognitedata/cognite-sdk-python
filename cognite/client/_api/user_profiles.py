from __future__ import annotations

from typing import Sequence

from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
from cognite.client.data_classes.user_profiles import UserProfile, UserProfileList
from cognite.client.utils._identifier import UserIdentifierSequence


class UserProfilesAPI(APIClient):
    _RESOURCE_PATH = "/profiles"

    def me(self) -> UserProfile:
        """`Retrieve your own user profile <https://developer.cognite.com/api#tag/User-profiles/paths/~1profiles~1me/get>`_

        Retrieves the user profile of the principal issuing the request, i.e. the principal *this* CogniteClient was instantiated with.

        Returns:
            UserProfile: Your own user profile.

        Raises:
            CogniteAPIError: If this principal doesn't have a user profile, you get a not found (404) response code.

        Examples:

            Get your own user profile:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.user_profiles.me()
        """
        return UserProfile._load(self._get(self._RESOURCE_PATH + "/me").json())

    def list(self, limit: int | None = LIST_LIMIT_DEFAULT, initial_cursor: str | None = None) -> UserProfileList:
        """`List user profiles <https://developer.cognite.com/api#tag/User-profiles/paths/~1profiles/get>`_

        List all user profiles in the current CDF project. The results are ordered alphabetically by name.

        Args:
            limit (int | None): Maximum number of user profiles to return. Defaults to 25. Set to -1, float("inf") or None to return all.
            initial_cursor (str | None): Start fetching from a specific cursor.

        Returns:
            UserProfileList: List of user profiles.

        Examples:

            List all user profiles:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.user_profiles.list(limit=None)
        """
        return self._list(
            "GET",
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            limit=limit,
            initial_cursor=initial_cursor,
        )

    def retrieve(self, user_identifier: str) -> UserProfile | None:
        """`Retrieve a single user profile by user identifier. <https://developer.cognite.com/api#tag/User-profiles/paths/~1profiles~1byids/post>`_

        Retrieves one user profile as indexed by the user identifier in the same CDF project.

        Args:
            user_identifier (str): The user identifier of the user profile to retrieve.

        Returns:
            UserProfile | None: The requested user profile or None if it doesn't exist.

        Examples:

            Get user profile by user identifier:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.user_profiles.retrieve("foo")
        """
        identifier = UserIdentifierSequence.load(user_identifier).as_singleton()
        return self._retrieve_multiple(
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            identifiers=identifier,
        )

    def retrieve_multiple(self, user_identifiers: Sequence[str]) -> UserProfileList:
        """`Retrieve multiple user profiles by user identifier. <https://developer.cognite.com/api#tag/User-profiles/paths/~1profiles~1byids/post>`_

        Retrieves one or more user profiles indexed by the user identifier in the same CDF project.

        Args:
            user_identifiers (Sequence[str]): The list of user identifiers to retrieve.

        Returns:
            UserProfileList: The requested user profiles.

        Raises:
            CogniteNotFoundError: One or more user identifiers does not exist.

        Examples:

            Get user profiles by user identifier:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.user_profiles.retrieve_multiple(["foo", "bar"])
        """
        identifiers = UserIdentifierSequence.load(user_identifiers)
        return self._retrieve_multiple(
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            identifiers=identifiers,
        )

    def search(self, name: str, limit: int = 100) -> UserProfileList:
        """`Search for user profiles <https://developer.cognite.com/api#tag/User-profiles/paths/~1profiles~1search/post>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, as the result set ordering and match criteria threshold may change over time.

        Args:
            name (str): Prefix search on name.
            limit (int): Maximum number of results to return.

        Returns:
            UserProfileList: User profiles search result

        Examples:

            Search for users with first (or second...) name starting with "Alex":

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.user_profiles.search(name="Alex")
        """
        return self._search(
            list_cls=UserProfileList,
            search={"name": name},
            filter={},
            limit=limit,
        )
