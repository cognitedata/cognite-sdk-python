from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Sequence, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.postgres_gateway.users import FdwUser, FdwUserList, FdwUserUpdate, FdwUserWrite
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
    ) -> Iterator[FdwUser]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[FdwUser]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[FdwUser] | Iterator[FdwUserList]:
        """Iterate over fdw users

        Fetches fdw user as they are iterated over, so you keep a limited number of fdw users in memory.

        Args:
            chunk_size (int | None): Number of fdw users to return in each chunk. Defaults to yielding one fdw user a time.
            limit (int | None): No description.

        Returns:
            Iterator[FdwUser] | Iterator[FdwUserList]: yields FdwUser one by one if chunk_size is not specified, else FdwUserList objects.
        """
        self._warning.warn()

        return self._list_generator(
            list_cls=FdwUserList,
            resource_cls=FdwUser,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def __iter__(self) -> Iterator[FdwUser]:
        """Iterate over fdw users

        Fetches fdw users as they are iterated over, so you keep a
        limited number of fdw users  in memory.

        Returns:
            Iterator[FdwUser]: yields fdw user one by one.
        """
        return self()

    @overload
    def create(self, create_user: FdwUserWrite) -> FdwUser: ...

    @overload
    def create(self, create_user: Sequence[FdwUserWrite]) -> FdwUserList: ...

    def create(self, create_user: FdwUserWrite | Sequence[FdwUserWrite]) -> FdwUser | FdwUserList:
        """`Create Users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/create_users>`_

        Create postgres users.

        Args:
            create_user (FdwUserWrite | Sequence[FdwUserWrite]): None

        Returns:
            FdwUser | FdwUserList: A user

        Examples:

            Create fdw user:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgres_gateway import FdwUserWrite
                >>> client = CogniteClient()
                >>> fdw_user = FdwUserWrite(<MISSING>)
                >>> res = client.postgres_gateways.users.create(fdw_user)

        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=FdwUserList,
            resource_cls=FdwUser,
            items=create_user,
            input_resource_cls=FdwUserWrite,
            headers={"cdf-version": "beta"},
        )

    @overload
    def update(self, items: FdwUserUpdate) -> FdwUser: ...

    @overload
    def update(self, items: FdwUserWrite) -> FdwUser: ...

    def update(
        self, items: FdwUserUpdate | FdwUserWrite | Sequence[FdwUserUpdate | FdwUserWrite]
    ) -> FdwUser | FdwUserList:
        """`Update users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/update_users>`_

        Update postgres users

        Args:
            items (FdwUserUpdate | FdwUserWrite | Sequence[FdwUserUpdate | FdwUserWrite]): No description.

        Returns:
            FdwUser | FdwUserList: The updated user(s)

        Examples:

            Update fdw user:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgres_gateway import FdwUserUpdate
                >>> client = CogniteClient()
                >>> update = FdwUserUpdate('myFdwUser').<MISSING>
                >>> res = client.postgres_gateways.users.update.update(update)

        """
        self._warning.warn()
        return self._update_multiple(
            items=items,
            list_cls=FdwUserList,
            resource_cls=FdwUser,
            update_cls=FdwUserUpdate,
            headers={"cdf-version": "beta"},
        )

    def delete(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool) -> None:
        """`Delete postgres user(s) <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/delete_users>`_

        Delete postgres users

        Args:
            username (str | SequenceNotStr[str]): Username to authenticate the user on the DB.
            ignore_unknown_ids (bool): Ignore usernames that are not found


        Examples:

            Delete fdw user:

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
    def retrieve(self, username: str, ignore_unknown_ids: bool) -> FdwUser: ...

    @overload
    def retrieve(self, username: SequenceNotStr[str], ignore_unknown_ids: bool) -> FdwUserList: ...

    def retrieve(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool) -> FdwUser | FdwUserList:
        """`Retrieve a list of users by their usernames <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/retreive_users>`_

        Retrieve a list of postgres users by their usernames, optionally ignoring unknown usernames

        Args:
            username (str | SequenceNotStr[str]): Username to authenticate the user on the DB.
            ignore_unknown_ids (bool): Ignore usernames that are not found

        Returns:
            FdwUser | FdwUserList: A user

        Examples:

            Retrieve fdw user:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.postgres_gateways.users.retrieve("myFdw", ignore_unknown_ids=True)

        """
        self._warning.warn()

        return self._retrieve_multiple(
            list_cls=FdwUserList,
            resource_cls=FdwUser,
            identifiers=UsernameSequence.load(usernames=username),
            ignore_unknown_ids=ignore_unknown_ids,
            headers={"cdf-version": "beta"},
        )

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> FdwUserList:
        """`Fetch scoped users <https://api-docs.cognite.com/20230101-beta/tag/Postgres-Gateway-Users/operation/filter_users>`_

        List all users in a given project. If more than `limit` users exist, a cursor for pagination will be returned
        with the response.

        Args:
            limit (int): Limits the number of results to be returned. The maximum results returned by the server is 100 even if you specify a higher limit.

        Returns:
            FdwUserList: A list of users

        Examples:

            List fdw users:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> fdw_user_list = client.postgres_gateways.users.list(limit=5)

            Iterate over fdw users::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for fdw_user in client.postgres_gateways.users:
                ...     fdw_user # do something with the fdw user

            Iterate over chunks of fdw users to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for fdw_user_list in client.postgres_gateways.users(chunk_size=25):
                ...     fdw_user_list # do something with the fdw users

        """
        self._warning.warn()
        return self._list(
            list_cls=FdwUserList,
            resource_cls=FdwUser,
            method="POST",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
