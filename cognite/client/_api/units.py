from __future__ import annotations

from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.units import (
    Unit,
    UnitList,
    UnitSystem,
    UnitSystemList,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

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
        self.systems = UnitSystemAPI(config, api_version, cognite_client)

    @overload
    def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> None | Unit: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> UnitList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Unit | UnitList | None:
        """`Retrieve one or more unit <https://developer.cognite.com/api#tag/Units/operation/byIdsUnits>`_

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            Unit | UnitList | None: If a single external ID is specified: the requested unit, or None if it does not exist. If several external IDs are specified: the requested units.

        Examples:

            Retrive unit 'temperature:deg_c'::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.units.retrieve('temperature:deg_c')

            Retrive units 'temperature:deg_c' and 'pressure:bar'::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.units.retrieve(['temperature:deg_c', 'pressure:bar'])

        """
        identifier = IdentifierSequence.load(external_ids=external_id)
        return self._retrieve_multiple(
            identifiers=identifier,
            list_cls=UnitList,
            resource_cls=Unit,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(self) -> UnitList:
        """`List all supported units <https://developer.cognite.com/api#tag/Units/operation/listUnits>`_

        Returns:
            UnitList: List of units

        Examples:

            List all supported unit in CDF::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.units.list()
        """
        return self._list(method="GET", list_cls=UnitList, resource_cls=Unit)


class UnitSystemAPI(APIClient):
    _RESOURCE_PATH = "/units/systems"

    def list(self) -> UnitSystemList:
        """`List all supported unit systems <https://developer.cognite.com/api#tag/Unit-Systems/operation/listUnitSystems>`_

        Returns:
            UnitSystemList: List of unit systems

        Examples:

            List all supported unit systems in CDF::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.units.systems.list()

        """
        return self._list(method="GET", list_cls=UnitSystemList, resource_cls=UnitSystem)
