from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Sequence, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite_gen.client.data_classes.postgres_gateway.users import FdwUser, FdwUserList, FdwUserUpdate, FdwUserWrite
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class UsersAPI(APIClient):
    _RESOURCE_PATH = "/postgresgateway/users"
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Users"
        )
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

    def create(self, create_user: FdwUserWrite | Sequence[FdwUserWrite]) -> UserCreated:
        """`Create Users <MISSING>`_

        Create postgres users.

        Args: 
            create_user (FdwUserWrite | Sequence[FdwUserWrite]): None
        
        Returns:
            UserCreated: A user

        Examples:

            Create fdw user:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgresgateway import FdwUserWrite
                >>> client = CogniteClient()
                >>> fdw_user = FdwUserWrite(<MISSING>)
                >>> res = client.hosted_extractors.destinations.create(fdw_user)

        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=FdwUserList,
            resource_cls=FdwUser,
            items=create_user,
            input_resource_cls=FdwUserWrite,
            headers={"cdf-version": "beta"},
        )

    def update(self, unknown: FdwUserUpdate) -> FdwUser | FdwUserList[FdwUser]:
        """`Update users <MISSING>`_

        Update postgres users

        Args: 
            unknown (FdwUserUpdate): None
        
        Returns:
            FdwUser | Sequence[FdwUser]: A user

        Examples:

            Update fdw user:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.postgresgateway import FdwUserUpdate
                >>> client = CogniteClient()
                >>> update = FdwUserUpdate('myFdwUser').<MISSING>
                >>> res = client.postgresgateway.users.update.update(update)

        """
        self._warning.warn()
        return self._update_multiple(
            items=unknown,
            list_cls=FdwUserList,
            resource_cls=FdwUser,
            update_cls=FdwUserUpdate,
            headers={"cdf-version": "beta"},
        )

    def delete(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool) -> None:
        """`Delete postgres user(s) <MISSING>`_

        Delete postgres users

        Args: 
            username (str | SequenceNotStr[str]): Username to authenticate the user on the DB.
            ignore_unknown_ids (bool): Ignore usernames that are not found
        

        Examples:

            Delete fdw user:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.postgresgateway.users.delete.delete(["myFdw", "myFdw2"])


        """
        self._warning.warn()
        extra_body_fields: dict[str, Any] = {}
        extra_body_fields['ignore_unknown_ids'] = ignore_unknown_ids

        return self._delete_multiple(
            identifiers=IdentifierSequence.load(usernames=username),
            wrap_ids=True,
            returns_items=False,
            extra_body_fields=extra_body_fields or None,
            headers={"cdf-version": "beta"},
        )

    def filter(self, limit: int, cursor: str) -> FdwUser | FdwUserList[FdwUser]:
        """`List postgres users <MISSING>`_

        List all postgres users for a given project. If more than `limit` users exist, a cursor for pagination will be
        returned with the response.

        Args: 
            limit (int): None
            cursor (str): Cursor for pagination
        
        Returns:
            FdwUser | Sequence[FdwUser]: A user

        Examples:

            <MISSING>

        """
        "<MISSING>"

    def retreive(self, username: str | SequenceNotStr[str], ignore_unknown_ids: bool) -> FdwUser:
        """`Retrieve a list of users by their usernames <MISSING>`_

        Retreive a list of postgres users by their usernames, optionally ignoring unknown usernames

        Args: 
            username (str | SequenceNotStr[str]): Username to authenticate the user on the DB.
            ignore_unknown_ids (bool): Ignore usernames that are not found
        
        Returns:
            FdwUser: A user

        Examples:

            <MISSING>

        """
        "<MISSING>"

    def list(self, limit: int | None = 100) -> FdwUser | FdwUserList[FdwUser]:
        """`Fetch scoped users <MISSING>`_

        List all users in a given project. If more than `limit` users exist, a cursor for pagination will be returned
        with the response.

        Args: 
            limit (int | None): Limits the number of results to be returned. The maximum results returned by the server is 100 even if you specify a higher limit.
        
        Returns:
            FdwUser | Sequence[FdwUser]: A user

        Examples:

            List fdw users:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> fdw_user_list = client.postgresgateway.users.list(limit=5)

            Iterate over fdw users::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for fdw_user in client.postgresgateway.users:
                ...     fdw_user # do something with the fdw user

            Iterate over chunks of fdw users to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for fdw_user_list in client.postgresgateway.users(chunk_size=25):
                ...     fdw_user_list # do something with the fdw users

        """
        self._warning.warn()
        return self._list(
            list_cls=FdwUserList,
            resource_cls=FdwUser,
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
