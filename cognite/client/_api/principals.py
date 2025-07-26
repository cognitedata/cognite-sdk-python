from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from typing import overload
from urllib.parse import urljoin

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.principals import Principal, PrincipalList
from cognite.client.utils._identifier import UserIdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

class OrgAPI(APIClient, ABC):
    def _get_base_url_with_base_path(self) -> str:
        base_path = ""
        if self._api_version:
            base_path = f"/api/{self._api_version}/orgs/{}"
        return urljoin(self._config.auth_url, base_path)



class PrincipalsAPI(OrgAPI):
    def me(self) -> Principal:
        raise NotImplementedError()

    def retrieve(self, id: int | Sequence[int] | None , external_id: str | SequenceNotStr[str] | None = None, ignore_unknown_ids: bool = False) -> Principal | PrincipalList | None:
        raise NotImplementedError()

    def list(self, types: str | SequenceNotStr[str] | None = None, limit: int = DEFAULT_LIMIT_READ) -> PrincipalList:
        raise NotImplementedError()




