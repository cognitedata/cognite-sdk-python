from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import overload
from urllib.parse import urljoin

from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._org_client import OrgAPIClient
from cognite.client.data_classes.principals import Principal, PrincipalList
from cognite.client.utils._identifier import PrincipalIdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class PrincipalsAPI(OrgAPIClient):
    _RESOURCE_PATH = "/principals"

    def me(self) -> Principal:
        """`Get the current caller's information. <https://developer.cognite.com/api#tag/Principals/operation/getMe>`_

        Returns:
            Principal: The principal of the user running the code, i.e. the
                principal *this* CogniteClient was instantiated with.

        Examples:
            Get your own principal:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.iam.principals.me()
        """
        # the /me endpoint is not using the /orgs/{org} base path, so we have to construct the URL manually
        path = f"{self._RESOURCE_PATH}/me"
        if self._api_version:
            path = f"/api/{self._api_version}{path}"

        full_url = urljoin(self._auth_url, path)
        headers = self._configure_headers(
            "application/json",
            additional_headers=self._config.headers.copy(),
            api_subversion=self._api_subversion,
        )
        response = self._http_client_with_retry.request(method="GET", url=full_url, headers=headers)
        response.raise_for_status()
        return Principal._load(response.json())

    @overload
    def retrieve(
        self,
        id: str,
        *,
        ignore_unknown_ids: bool = False,
    ) -> Principal | None: ...

    @overload
    def retrieve(
        self,
        *,
        external_id: str,
        ignore_unknown_ids: bool = False,
    ) -> Principal | None: ...

    @overload
    def retrieve(
        self,
        id: SequenceNotStr[str],
        *,
        ignore_unknown_ids: bool = False,
    ) -> PrincipalList: ...

    @overload
    def retrieve(
        self,
        *,
        external_id: SequenceNotStr[str],
        ignore_unknown_ids: bool = False,
    ) -> PrincipalList: ...

    @overload
    def retrieve(
        self,
        id: None = None,
        *,
        ignore_unknown_ids: bool = False,
    ) -> PrincipalList: ...
    @overload
    def retrieve(
        self,
        *,
        external_id: None = None,
        ignore_unknown_ids: bool = False,
    ) -> PrincipalList: ...

    def retrieve(
        self,
        id: str | SequenceNotStr[str] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> Principal | PrincipalList | None:
        """`Retrieve principal by reference in the organization <https://developer.cognite.com/api#tag/Principals/operation/getPrincipalsById>`_

        Args:
            id (str | SequenceNotStr[str] | None): The ID(s) of the principal(s) to retrieve.
            external_id (str | SequenceNotStr[str] | None): The external ID(s) of the principal to retrieve.
            ignore_unknown_ids (bool): This is only relevant when retrieving multiple principals. If set to True,
                the method will return the principals that were found and ignore the ones that were not found.
                If set to False, the method will raise a CogniteAPIError if any of the
                specified principals were not found. Defaults to False.

        Returns:
            Principal | PrincipalList | None: The principal(s) with the specified ID(s) or external ID(s).

        Examples:
            Retrieve a principal by ID:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.iam.principals.retrieve(id="20u3of8-1234-5678-90ab-cdef12345678")
            Retrieve a principal by external ID:
                >>> res = client.iam.principals.retrieve(external_id="my_external_id")

        """
        identifier = PrincipalIdentifierSequence.load(ids=id, external_ids=external_id)
        if identifier.is_singleton() and ignore_unknown_ids:
            warnings.warn(UserWarning("ignore_unknown_ids=True has no effect when retrieving a single principal."))

        return self._retrieve_multiple(
            list_cls=PrincipalList,
            resource_cls=Principal,  # type: ignore[type-abstract]
            identifiers=identifier,
            other_params={"ignoreUnknownIds": True if identifier.is_singleton() else ignore_unknown_ids},
        )

    def list(self, types: str | SequenceNotStr[str] | None = None, limit: int = DEFAULT_LIMIT_READ) -> PrincipalList:
        """`List principals in the organization <https://developer.cognite.com/api#tag/Principals/operation/listPrincipals>`_

        Args:
            types (str | SequenceNotStr[str] | None): Filter by principal type(s). Defaults to None, which means no filtering.
            limit (int): The maximum number of principals to return. Defaults to DEFAULT_LIMIT_READ.

        Returns:
            PrincipalList: The principal of the user running the code, i.e. the principal *this* CogniteClient was instantiated with.

        Examples:
            List principals in the organization:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.iam.principals.list(types="USER", limit=10)

        """
        other_params: dict[str, object] | None = None
        if isinstance(types, str):
            other_params = {"types": types.upper()}
        elif isinstance(types, Sequence):
            other_params = {"types": [t.upper() for t in types]}

        return self._list(
            # The Principal is abstract, but calling load on it will return a concrete instance.
            method="GET",
            list_cls=PrincipalList,
            resource_cls=Principal,  # type: ignore[type-abstract]
            limit=limit,
            other_params=other_params,
        )
