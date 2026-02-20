"""
===============================================================================
6acd0116cb4337950bd3e077f7b31818
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.postgres_gateway.users import (
    User,
    UserCreated,
    UserCreatedList,
    UserList,
    UserUpdate,
    UserWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncUsersAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[User]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[UserList]: ...

    def __call__(self, chunk_size: int | None = None, limit: int | None = None) -> Iterator[User] | Iterator[UserList]:
        """
        Iterate over users

        Fetches user as they are iterated over, so you keep a limited number of users in memory.

        Args:
            chunk_size: Number of users to return in each chunk. Defaults to yielding one user at a time.
            limit: Maximum number of users to return. Defaults to return all.

        Yields:
            yields User one by one if chunk_size is not specified, else UserList objects.
        """  # noqa: DOC404
        yield from SyncIterator(self.__async_client.postgres_gateway.users(chunk_size=chunk_size, limit=limit))  # type: ignore [misc]

    @overload
    def create(self, user: UserWrite) -> UserCreated: ...

    @overload
    def create(self, user: Sequence[UserWrite]) -> UserCreatedList: ...

    def create(self, user: UserWrite | Sequence[UserWrite]) -> UserCreated | UserCreatedList:
        """
        `Create Users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/create_users>`_

        Create postgres users.

        Args:
            user: The user(s) to create.

        Returns:
            The created user(s)

        Examples:

            Create user:

                >>> import os
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgres_gateway import UserWrite, SessionCredentials
                >>> from cognite.client.data_classes import ClientCredentials
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> session = client.iam.sessions.create(
                ...     ClientCredentials(os.environ["IDP_CLIENT_ID"], os.environ["IDP_CLIENT_SECRET"]),
                ...     session_type="CLIENT_CREDENTIALS"
                ... )
                >>> user = UserWrite(credentials=SessionCredentials(nonce=session.nonce))
                >>> res = client.postgres_gateway.users.create(user)
        """
        return run_sync(self.__async_client.postgres_gateway.users.create(user=user))

    @overload
    def update(self, items: UserUpdate | UserWrite) -> User: ...

    @overload
    def update(self, items: Sequence[UserUpdate | UserWrite]) -> UserList: ...

    def update(self, items: UserUpdate | UserWrite | Sequence[UserUpdate | UserWrite]) -> User | UserList:
        """
        `Update users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/update_users>`_

        Update postgres users

        Args:
            items: The user(s) to update.

        Returns:
            The updated user(s)

        Examples:

            Update user:

                >>> import os
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgres_gateway import UserUpdate, SessionCredentials
                >>> from cognite.client.data_classes import ClientCredentials
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> session = client.iam.sessions.create(
                ...     ClientCredentials(os.environ["IDP_CLIENT_ID"], os.environ["IDP_CLIENT_SECRET"]),
                ...     session_type="CLIENT_CREDENTIALS"
                ... )
                >>> update = UserUpdate('myUser').credentials.set(SessionCredentials(nonce=session.nonce))
                >>> res = client.postgres_gateway.users.update(update)
        """
        return run_sync(self.__async_client.postgres_gateway.users.update(items=items))

    def delete(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """
        `Delete postgres user(s) <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/delete_users>`_

        Delete postgres users

        Args:
            username: Usernames of the users to delete.
            ignore_unknown_ids: Ignore usernames that are not found

        Examples:

            Delete users:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.postgres_gateway.users.delete(["myUser", "myUser2"])
        """
        return run_sync(
            self.__async_client.postgres_gateway.users.delete(username=username, ignore_unknown_ids=ignore_unknown_ids)
        )

    @overload
    def retrieve(self, username: str, ignore_unknown_ids: bool = False) -> User: ...

    @overload
    def retrieve(self, username: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> UserList: ...

    def retrieve(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> User | UserList:
        """
        `Retrieve a list of users by their usernames <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/retreive_users>`_

        Retrieve a list of postgres users by their usernames, optionally ignoring unknown usernames

        Args:
            username: Usernames of the users to retrieve.
            ignore_unknown_ids: Ignore usernames that are not found

        Returns:
            The retrieved user(s).

        Examples:

            Retrieve user:

                    >>> from cognite.client import CogniteClient, AsyncCogniteClient
                    >>> client = CogniteClient()
                    >>> # async_client = AsyncCogniteClient()  # another option
                    >>> res = client.postgres_gateway.users.retrieve("myUser", ignore_unknown_ids=True)
        """
        return run_sync(
            self.__async_client.postgres_gateway.users.retrieve(
                username=username, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> UserList:
        """
        `Fetch scoped users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/filter_users>`_

        List all users in a given project.

        Args:
            limit: Limits the number of results to be returned.

        Returns:
            A list of users

        Examples:

            List users:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> user_list = client.postgres_gateway.users.list(limit=5)

            Iterate over users, one-by-one:

                >>> for user in client.postgres_gateway.users():
                ...     user  # do something with the user

            Iterate over chunks of users to reduce memory load:

                >>> for user_list in client.postgres_gateway.users(chunk_size=25):
                ...     user_list # do something with the users
        """
        return run_sync(self.__async_client.postgres_gateway.users.list(limit=limit))
