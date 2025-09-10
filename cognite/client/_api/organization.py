from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from functools import cached_property
from urllib.parse import urljoin

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.principals import Principal, PrincipalList
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._identifier import PrincipalIdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class OrgAPI(APIClient, ABC):
    _auth_url = "https://auth.cognite.com"

    def _get_base_url_with_base_path(self) -> str:
        """Get base URL with base path including organization and api version if applicable"""
        base_path = ""
        if self._api_version:
            base_path = f"/api/{self._api_version}/orgs/{self._organization}"
        # The OrganizationAPi uses the auth_url as the base for these endpoints instead of the
        # base_url like the rest of the SDK.
        return urljoin(self._auth_url, base_path)

    @cached_property
    def _organization(self) -> str:
        headers = self._configure_headers(
            "application/json",
            additional_headers=self._config.headers.copy(),
            api_subversion=self._api_subversion,
        )
        # This is an internal endpoint, not part of the public API
        full_url = urljoin(self._config.base_url, f"/api/v1/projects/{self._config.project}")
        response = self._http_client_with_retry.request(method="GET", url=full_url, headers=headers)
        if response.status_code != 200:
            raise CogniteAPIError(
                "Could not look-up organization", response.status_code, response.headers.get("x-request-id")
            )
        return response.json()["organization"]


class PrincipalsAPI(OrgAPI):
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
        path = "/principals/me"
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
            ignore_unknown_ids (bool): If True, unknown IDs will be ignored and not raise an error. Defaults to False
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
        return self._retrieve_multiple(
            list_cls=PrincipalList,
            resource_cls=Principal,  # type: ignore[type-abstract]
            identifiers=identifier,
            other_params={"ignoreUnknownIds": ignore_unknown_ids},
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
            other_params = {"types": ",".join(types).upper()}

        return self._list(
            # The Principal is abstract, but calling load on it will return a concrete instance.
            method="GET",
            list_cls=PrincipalList,
            resource_cls=Principal,  # type: ignore[type-abstract]
            limit=limit,
            other_params=other_params,
        )
