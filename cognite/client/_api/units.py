from __future__ import annotations

from typing import TYPE_CHECKING, MutableSequence

from cognite.client._api_client import APIClient
from cognite.client.data_classes.unit import (
    Unit,
    UnitList,
    UnitSystemList,
)

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class UnitAPI(APIClient):
    _RESOURCE_PATH = "/units"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"

    def retrieve(self, external_id: str | MutableSequence[str], ignore_unknown_ids: bool = True) -> Unit | UnitList:
        raise NotImplementedError

    def list_systems(self) -> UnitSystemList:
        raise NotImplementedError

    def list(self) -> UnitList:
        return self._list(method="GET", list_cls=UnitList, resource_cls=Unit)
