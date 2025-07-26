from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from urllib.parse import urljoin

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.principals import Principal, PrincipalList
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
        id: int | Sequence[int] | None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> Principal | PrincipalList | None:
        raise NotImplementedError()

    def list(self, types: str | SequenceNotStr[str] | None = None, limit: int = DEFAULT_LIMIT_READ) -> PrincipalList:
        raise NotImplementedError()
