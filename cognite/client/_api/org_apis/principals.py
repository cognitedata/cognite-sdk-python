from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._org_client import OrgAPIClient
from cognite.client.data_classes.principals import Principal, PrincipalList
from cognite.client.utils._auxiliary import append_url_path
from cognite.client.utils._identifier import PrincipalIdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class PrincipalsAPI(OrgAPIClient):
    _RESOURCE_PATH = "/principals"

    async def me(self) -> Principal:
        """`Get the current caller's information. <https://developer.cognite.com/api#tag/Principals/operation/getMe>`_

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
        # the /me endpoint is not using the /orgs/{org} base path, so we have to construct the URL manually
        path = f"/api/{self._api_version}{self._RESOURCE_PATH}/me"
        full_url = append_url_path(self._AUTH_URL, path)
        response = await self._cognite_client.get(url=full_url)
        return Principal._load(response.json())

    @overload
    async def retrieve(
        self,
        id: str,
        external_id: None = None,
        ignore_unknown_ids: bool = False,
    ) -> Principal | None: ...

    @overload
    async def retrieve(
        self,
        id: None = None,
        *,
        external_id: str,
        ignore_unknown_ids: bool = False,
    ) -> Principal | None: ...

    @overload
    async def retrieve(
        self,
        id: SequenceNotStr[str],
        external_id: None = None,
        ignore_unknown_ids: bool = False,
    ) -> PrincipalList: ...

    @overload
    async def retrieve(
        self,
        id: None = None,
        *,
        external_id: SequenceNotStr[str],
        ignore_unknown_ids: bool = False,
    ) -> PrincipalList: ...

    @overload
    async def retrieve(
        self,
        id: None = None,
        external_id: None = None,
        ignore_unknown_ids: bool = False,
    ) -> PrincipalList: ...

    async def retrieve(
        self,
        id: str | SequenceNotStr[str] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> Principal | PrincipalList | None:
        """`Retrieve principal by reference in the organization <https://developer.cognite.com/api#tag/Principals/operation/getPrincipalsById>`_

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
        identifier = PrincipalIdentifierSequence.load(ids=id, external_ids=external_id)
        return await self._retrieve_multiple(
            list_cls=PrincipalList,
            resource_cls=Principal,  # type: ignore[type-abstract]
            identifiers=identifier,
            other_params={"ignoreUnknownIds": True if identifier.is_singleton() else ignore_unknown_ids},
        )

    async def list(self, types: str | Sequence[str] | None = None, limit: int = DEFAULT_LIMIT_READ) -> PrincipalList:
        """`List principals in the organization <https://developer.cognite.com/api#tag/Principals/operation/listPrincipals>`_

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
        other_params: dict[str, str | list[str]] | None = None
        match types:
            case str():
                other_params = {"types": types.upper()}
            case Sequence():
                other_params = {"types": [t.upper() for t in types]}

        return await self._list(
            method="GET",
            list_cls=PrincipalList,
            # The Principal is abstract, but calling load on it will return a concrete instance:
            resource_cls=Principal,  # type: ignore[type-abstract]
            limit=limit,
            other_params=other_params,
        )
