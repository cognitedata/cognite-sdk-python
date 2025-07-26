from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from urllib.parse import urljoin

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.principals import Principal, PrincipalList
from cognite.client.utils._identifier import PrincipalIdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class OrgAPI(APIClient, ABC):
    def _get_base_url_with_base_path(self) -> str:
        if self._config.organization is None:
            raise RuntimeError(
                f"Organization is not set in the configuration. Please set it before using {type(self).__name__}."
            )
        base_path = ""
        if self._api_version:
            base_path = f"/api/{self._api_version}/orgs/{self._config.organization}"
        return urljoin(self._config.auth_url, base_path)


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
        res = self._get("https://auth.cognite.com/api/v1/principals/me")
        return Principal._load(res.json())

    def retrieve(
        self,
        id: str | None,
        external_id: str | None = None,
    ) -> Principal | None:
        """`Retrieve principal by reference in the organization <https://developer.cognite.com/api#tag/Principals/operation/getPrincipalsById>`_

        Args:
            id (str | None): The ID(s) of the principal(s) to retrieve.
            external_id (str | None): The external ID(s) of the principal to retrieve.

        Returns:
            Principal | None: The principal(s) with the specified ID(s) or external ID(s).

        Examples:

            Retrieve a principal by ID:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.iam.principals.retrieve(id="20u3of8-1234-5678-90ab-cdef12345678")

            Retrieve a principal by external ID:
                >>> res = client.iam.principals.retrieve(external_id="my_external_id")

        """
        identifier = PrincipalIdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=PrincipalList,
            resource_cls=Principal,  # type: ignore[type-abstract]
            identifiers=identifier,
            other_params={"ignoreUnknownIds": True},
        )

    def retrieve_multiple(
        self,
        ids: SequenceNotStr[str] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> PrincipalList:
        """`Retrieve principals by reference in the organization <https://developer.cognite.com/api#tag/Principals/operation/getPrincipalsById>`_

        Args:
            ids (SequenceNotStr[str] | None): IDs of the principals to retrieve.
            external_ids (SequenceNotStr[str] | None): External IDs of the principals to retrieve.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            PrincipalList: A list of principals with the specified IDs or external IDs.

        Examples:

            Get multiple principals by ID:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.iam.principals.retrieve_multiple(ids=["20u3of8-1234-5678-90ab-cdef12345678", "30u3of8-1234-5678-90ab-cdef12345678"])

            Get multiple principals by ID and external ID:

                >>> res = client.assets.retrieve_multiple(ids=["20u3of8-1234-5678-90ab-cdef12345678"], external_ids=["my_external_id_1", "my_external_id_2"])
        """
        identifiers = PrincipalIdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=PrincipalList,
            resource_cls=Principal,  # type: ignore[type-abstract]
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
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
