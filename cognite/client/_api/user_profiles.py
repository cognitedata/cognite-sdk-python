from __future__ import annotations

from typing import List, MutableSequence, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
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

    @overload
    def retrieve(self, user_identifier: str) -> UserProfile | None:
        ...

    @overload  # Note, can't use Sequence[str], as str itself matches this requirement...
    def retrieve(self, user_identifier: MutableSequence[str] | tuple[str, ...]) -> UserProfileList:
        ...

    def retrieve(
        self, user_identifier: str | MutableSequence[str] | tuple[str, ...]
    ) -> UserProfile | UserProfileList | None:
        """`Retrieve user profiles by user identifier. <https://developer.cognite.com/api#tag/User-profiles/paths/~1profiles~1byids/post>`_

        Retrieves one or more user profiles indexed by the user identifier in the same CDF project.

        Args:
            user_identifier (str | MutableSequence[str] | tuple[str, ...]): The single user identifier (or sequence of) to retrieve profile(s) for.

        Returns:
            UserProfile | UserProfileList | None: UserProfileList if a sequence of user identifier were requested, else UserProfile. If a single user identifier is requested and it is not found, None is returned.

        Raises:
            CogniteNotFoundError: A sequences of user identifiers were requested, but one or more does not exist.

        Examples:

            Get a single user profile:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.user_profiles.retrieve("foo")

            Get multiple user profiles:

                >>> res = c.iam.user_profiles.retrieve(["bar", "baz"])
        """
        identifiers = UserIdentifierSequence.load(user_identifier)
        profiles = self._retrieve_multiple(
            list_cls=UserProfileList,
            resource_cls=UserProfile,
            identifiers=identifiers,
        )
        if identifiers.is_singleton():
            return profiles
        # TODO: The API does not guarantee any ordering (against style guidelines, no timeline for fix)
        #       so we sort manually for now:
        return UserProfileList(cast(List[UserProfile], [profiles.get(user) for user in user_identifier]))

    def search(self, name: str, limit: int = DEFAULT_LIMIT_READ) -> UserProfileList:
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

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> UserProfileList:
        """`List user profiles <https://developer.cognite.com/api#tag/User-profiles/paths/~1profiles/get>`_

        List all user profiles in the current CDF project. The results are ordered alphabetically by name.

        Args:
            limit (int | None): Maximum number of user profiles to return. Defaults to 25. Set to -1, float("inf") or None to return all.

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
        )
