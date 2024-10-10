from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.postgres_gateway.users import User, UserList, UserUpdate, UserWrite
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import UsernameSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class UsersAPI(APIClient):
    _RESOURCE_PATH = "/postgresgateway"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Users")

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
    ) -> Iterator[User]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[UserList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[User] | Iterator[UserList]:
        """Iterate over users

        Fetches user as they are iterated over, so you keep a limited number of users in memory.

        Args:
            chunk_size (int | None): Number of users to return in each chunk. Defaults to yielding one user at a time.
            limit (int | None): Maximum number of users to return. Defaults to return all.


        Returns:
            Iterator[User] | Iterator[UserList]: yields User one by one if chunk_size is not specified, else UserList objects.
        """
        self._warning.warn()

        return self._list_generator(
            list_cls=UserList,
            resource_cls=User,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def __iter__(self) -> Iterator[User]:
        """Iterate over users

        Fetches users as they are iterated over, so you keep a
        limited number of users in memory.

        Returns:
            Iterator[User]: yields user one by one.
        """
        return self()

    @overload
    def create(self, user: UserWrite) -> User: ...

    @overload
    def create(self, user: Sequence[UserWrite]) -> UserList: ...

    def create(self, user: UserWrite | Sequence[UserWrite]) -> User | UserList:
        """`Create Users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/create_users>`_

        Create postgres users.

        Args:
            user (UserWrite | Sequence[UserWrite]): The user(s) to create.

        Returns:
            User | UserList: The created user(s)

        Examples:

            Create user:

                >>> import os
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgres_gateway import UserWrite, SessionCredentials
                >>> from cognite.client.data_classes import ClientCredentials
                >>> client = CogniteClient()
                >>> session = client.iam.sessions.create(
                ...     ClientCredentials(os.environ["IDP_CLIENT_ID"], os.environ["IDP_CLIENT_SECRET"]),
                ...     session_type="CLIENT_CREDENTIALS"
                ... )
                >>> user = UserWrite(credentials=SessionCredentials(nonce=session.nonce))
                >>> res = client.postgres_gateway.users.create(user)

        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=UserList,
            resource_cls=User,
            items=user,
            input_resource_cls=UserWrite,
            headers={"cdf-version": "beta"},
        )

    @overload
    def update(self, items: UserUpdate | UserWrite) -> User: ...

    @overload
    def update(self, items: Sequence[UserUpdate | UserWrite]) -> UserList: ...

    def update(self, items: UserUpdate | UserWrite | Sequence[UserUpdate | UserWrite]) -> User | UserList:
        """`Update users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/update_users>`_

        Update postgres users

        Args:
            items (UserUpdate | UserWrite | Sequence[UserUpdate | UserWrite]): The user(s) to update.

        Returns:
            User | UserList: The updated user(s)

        Examples:

            Update user:

                >>> import os
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgres_gateway import UserUpdate, SessionCredentials
                >>> from cognite.client.data_classes import ClientCredentials
                >>> client = CogniteClient()
                >>> session = client.iam.sessions.create(
                ...     ClientCredentials(os.environ["IDP_CLIENT_ID"], os.environ["IDP_CLIENT_SECRET"]),
                ...     session_type="CLIENT_CREDENTIALS"
                ... )
                >>> update = UserUpdate('myUser').credentials.set(SessionCredentials(nonce=session.nonce))
                >>> res = client.postgres_gateway.users.update(update)

        """
        self._warning.warn()
        return self._update_multiple(
            items=items,
            list_cls=UserList,
            resource_cls=User,
            update_cls=UserUpdate,
            headers={"cdf-version": "beta"},
        )

    def delete(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete postgres user(s) <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/delete_users>`_

        Delete postgres users

        Args:
            username (str | SequenceNotStr[str]): Usernames of the users to delete.
            ignore_unknown_ids (bool): Ignore usernames that are not found


        Examples:

            Delete users:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.postgres_gateway.users.delete(["myUser", "myUser2"])


        """
        self._warning.warn()
        extra_body_fields = {"ignore_unknown_ids": ignore_unknown_ids}

        self._delete_multiple(
            identifiers=UsernameSequence.load(usernames=username),
            wrap_ids=True,
            returns_items=False,
            extra_body_fields=extra_body_fields,
            headers={"cdf-version": "beta"},
        )

    @overload
    def retrieve(self, username: str, ignore_unknown_ids: bool = False) -> User: ...

    @overload
    def retrieve(self, username: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> UserList: ...

    def retrieve(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> User | UserList:
        """`Retrieve a list of users by their usernames <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/retreive_users>`_

        Retrieve a list of postgres users by their usernames, optionally ignoring unknown usernames

        Args:
            username (str | SequenceNotStr[str]): Usernames of the users to retrieve.
            ignore_unknown_ids (bool): Ignore usernames that are not found

        Returns:
            User | UserList: The retrieved user(s).

        Examples:

            Retrieve user:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.postgres_gateway.users.retrieve("myUser", ignore_unknown_ids=True)

        """
        self._warning.warn()

        return self._retrieve_multiple(
            list_cls=UserList,
            resource_cls=User,
            identifiers=UsernameSequence.load(usernames=username),
            ignore_unknown_ids=ignore_unknown_ids,
            headers={"cdf-version": "beta"},
        )

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> UserList:
        """`Fetch scoped users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/filter_users>`_

        List all users in a given project.

        Args:
            limit (int): Limits the number of results to be returned.

        Returns:
            UserList: A list of users

        Examples:

            List users:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> user_list = client.postgres_gateway.users.list(limit=5)

            Iterate over users::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for user in client.postgres_gateway.users:
                ...     user # do something with the user

            Iterate over chunks of users to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for user_list in client.postgres_gateway.users(chunk_size=25):
                ...     user_list # do something with the users

        """
        self._warning.warn()
        return self._list(
            list_cls=UserList,
            resource_cls=User,
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
