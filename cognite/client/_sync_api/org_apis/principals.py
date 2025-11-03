"""
===============================================================================
a5de07c7a97bd83049f8d6bb125f430e
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.principals import Principal, PrincipalList
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class SyncPrincipalsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def me(self) -> Principal:
        """
        `Get the current caller's information. <https://developer.cognite.com/api#tag/Principals/operation/getMe>`_

        Returns:
            Principal: The principal of the user running the code, i.e. the
                principal *this* AsyncCogniteClient was instantiated with.

        Examples:
            Get your own principal:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.principals.me()
        """
        return run_sync(self.__async_client.iam.principals.me())

    @overload
    def retrieve(self, id: str, external_id: None = None, ignore_unknown_ids: bool = False) -> Principal | None: ...

    @overload
    def retrieve(self, id: None = None, *, external_id: str, ignore_unknown_ids: bool = False) -> Principal | None: ...

    @overload
    def retrieve(
        self, id: SequenceNotStr[str], external_id: None = None, ignore_unknown_ids: bool = False
    ) -> PrincipalList: ...

    @overload
    def retrieve(
        self, id: None = None, *, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> PrincipalList: ...

    @overload
    def retrieve(
        self, id: None = None, external_id: None = None, ignore_unknown_ids: bool = False
    ) -> PrincipalList: ...

    def retrieve(
        self,
        id: str | SequenceNotStr[str] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> Principal | PrincipalList | None:
        """
        `Retrieve principal by reference in the organization <https://developer.cognite.com/api#tag/Principals/operation/getPrincipalsById>`_

        Args:
            id (str | SequenceNotStr[str] | None): The ID(s) of the principal(s) to retrieve.
            external_id (str | SequenceNotStr[str] | None): The external ID(s) of the principal to retrieve.
            ignore_unknown_ids (bool): This is only relevant when retrieving multiple principals. If set to True, the method will return the principals that were found and ignore the ones that were not found. If set to False, the method will raise a CogniteAPIError if any of the specified principals were not found. Defaults to False.

        Returns:
            Principal | PrincipalList | None: The principal(s) with the specified ID(s) or external ID(s).

        Examples:
            Retrieve a principal by ID:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.principals.retrieve(id="20u3of8-1234-5678-90ab-cdef12345678")

            Retrieve a principal by external ID:
                >>> res = client.iam.principals.retrieve(external_id="my_external_id")
        """
        return run_sync(
            self.__async_client.iam.principals.retrieve(
                id=id,  # type: ignore [arg-type]
                external_id=external_id,  # type: ignore [arg-type]
                ignore_unknown_ids=ignore_unknown_ids,
            )
        )

    def list(self, types: str | Sequence[str] | None = None, limit: int = DEFAULT_LIMIT_READ) -> PrincipalList:
        """
        `List principals in the organization <https://developer.cognite.com/api#tag/Principals/operation/listPrincipals>`_

        Args:
            types (str | Sequence[str] | None): Filter by principal type(s). Defaults to None, which means no filtering.
            limit (int): The maximum number of principals to return. Defaults to 25.

        Returns:
            PrincipalList: The principal of the user running the code, i.e. the principal *this* CogniteClient was instantiated with.

        Examples:
            List principals in the organization:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.principals.list(types="USER", limit=10)
        """
        return run_sync(self.__async_client.iam.principals.list(types=types, limit=limit))
