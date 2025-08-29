from __future__ import annotations

from abc import ABC
from functools import cached_property

from cognite.client._api_client import APIClient


class OrgAPI(APIClient, ABC):
    def _get_base_url_with_base_path(self) -> str:
        raise NotImplementedError()

    @cached_property
    def _organization(self) -> str:
        raise NotImplementedError()
