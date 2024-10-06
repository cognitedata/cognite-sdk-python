from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Sequence, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.postgres_gateway.users import User, UserList, UserUpdate, UserWrite
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import UsernameSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class UsersAPI(APIClient):
    _RESOURCE_PATH = "/postgresgateway/users"

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
    ) -> Iterator[User]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[User] | Iterator[UserList]:
        """Iterate over users

        Fetches user as they are iterated over, so you keep a limited number of users in memory.

        Args:
            chunk_size (int | None): Number of users to return in each chunk. Defaults to yielding one user a time.
            limit (int | None): No description.

        Returns:
            Iterator[User] | Iterator[UserList]: yields FdwUser one by one if chunk_size is not specified, else FdwUserList objects.
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
        limited number of users  in memory.

        Returns:
            Iterator[User]: yields user one by one.
        """
        return self()

    @overload
    def create(self, create_user: UserWrite) -> User: ...

    @overload
    def create(self, create_user: Sequence[UserWrite]) -> UserList: ...

    def create(self, create_user: UserWrite | Sequence[UserWrite]) -> User | UserList:
        """`Create Users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/create_users>`_

        Create postgres users.

        Args:
            create_user (UserWrite | Sequence[UserWrite]): None

        Returns:
            User | UserList: A user

        Examples:

            Create user:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgres_gateway import UserWrite
                >>> client = CogniteClient()
                >>> fdw_user = UserWrite(<MISSING>)
                >>> res = client.postgres_gateways.users.create(fdw_user)

        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=UserList,
            resource_cls=User,
            items=create_user,
            input_resource_cls=UserWrite,
            headers={"cdf-version": "beta"},
        )

    @overload
    def update(self, items: UserUpdate) -> User: ...

    @overload
    def update(self, items: UserWrite) -> User: ...

    def update(self, items: UserUpdate | UserWrite | Sequence[UserUpdate | UserWrite]) -> User | UserList:
        """`Update users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/update_users>`_

        Update postgres users

        Args:
            items (UserUpdate | UserWrite | Sequence[UserUpdate | UserWrite]): No description.

        Returns:
            User | UserList: The updated user(s)

        Examples:

            Update user:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgres_gateway import UserUpdate
                >>> client = CogniteClient()
                >>> update = UserUpdate('myFdwUser').<MISSING>
                >>> res = client.postgres_gateways.users.update.update(update)

        """
        self._warning.warn()
        return self._update_multiple(
            items=items,
            list_cls=UserList,
            resource_cls=User,
            update_cls=UserUpdate,
            headers={"cdf-version": "beta"},
        )

    def delete(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool) -> None:
        """`Delete postgres user(s) <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/delete_users>`_

        Delete postgres users

        Args:
            username (str | SequenceNotStr[str]): Username to authenticate the user on the DB.
            ignore_unknown_ids (bool): Ignore usernames that are not found


        Examples:

            Delete user:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.postgres_gateways.users.delete.delete(["myFdw", "myFdw2"])


        """
        self._warning.warn()
        extra_body_fields: dict[str, Any] = {}
        extra_body_fields["ignore_unknown_ids"] = ignore_unknown_ids

        self._delete_multiple(
            identifiers=UsernameSequence.load(usernames=username),
            wrap_ids=True,
            returns_items=False,
            extra_body_fields=extra_body_fields or None,
            headers={"cdf-version": "beta"},
        )

    @overload
    def retrieve(self, username: str, ignore_unknown_ids: bool) -> User: ...

    @overload
    def retrieve(self, username: SequenceNotStr[str], ignore_unknown_ids: bool) -> UserList: ...

    def retrieve(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool) -> User | UserList:
        """`Retrieve a list of users by their usernames <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/retreive_users>`_

        Retrieve a list of postgres users by their usernames, optionally ignoring unknown usernames

        Args:
            username (str | SequenceNotStr[str]): Username to authenticate the user on the DB.
            ignore_unknown_ids (bool): Ignore usernames that are not found

        Returns:
            User | UserList: A user

        Examples:

            Retrieve user:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.postgres_gateways.users.retrieve("myFdw", ignore_unknown_ids=True)

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

        List all users in a given project. If more than `limit` users exist, a cursor for pagination will be returned
        with the response.

        Args:
            limit (int): Limits the number of results to be returned. The maximum results returned by the server is 100 even if you specify a higher limit.

        Returns:
            UserList: A list of users

        Examples:

            List users:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> fdw_user_list = client.postgres_gateways.users.list(limit=5)

            Iterate over users::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for fdw_user in client.postgres_gateways.users:
                ...     fdw_user # do something with the user

            Iterate over chunks of users to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for fdw_user_list in client.postgres_gateways.users(chunk_size=25):
                ...     fdw_user_list # do something with the users

        """
        self._warning.warn()
        return self._list(
            list_cls=UserList,
            resource_cls=User,
            method="POST",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
