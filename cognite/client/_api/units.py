from __future__ import annotations

from typing import TYPE_CHECKING, MutableSequence, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.unit import (
    Unit,
    UnitList,
    UnitSystem,
    UnitSystemList,
)
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class UnitSystemAPI(APIClient):
    _RESOURCE_PATH = "/units/systems"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"

    def list(self) -> UnitSystemList:
        return self._list(method="GET", list_cls=UnitSystemList, resource_cls=UnitSystem)


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
        self.systems = UnitSystemAPI(config, api_version, cognite_client)

    @overload
    def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> None | Unit:
        ...

    @overload
    def retrieve(self, external_id: MutableSequence[str], ignore_unknown_ids: bool = False) -> UnitList:
        ...

    def retrieve(
        self, external_id: str | MutableSequence[str], ignore_unknown_ids: bool = False
    ) -> None | Unit | UnitList:
        if isinstance(external_id, str):
            is_single = True
            external_id = [external_id]
        else:
            is_single = False

        identifier = IdentifierSequence.load(external_ids=external_id)
        retrieved = self._retrieve_multiple(
            identifiers=identifier,
            list_cls=UnitList,
            resource_cls=Unit,
            ignore_unknown_ids=ignore_unknown_ids or is_single,
        )
        if is_single and len(retrieved) == 0:
            return None
        elif is_single:
            return retrieved[0]
        else:
            return retrieved

    def list(self) -> UnitList:
        return self._list(method="GET", list_cls=UnitList, resource_cls=Unit)
