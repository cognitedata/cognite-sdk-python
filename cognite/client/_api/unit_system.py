from __future__ import annotations

from cognite.client._api_client import APIClient
from cognite.client.data_classes.units import (
    UnitSystem,
    UnitSystemList,
)


class UnitSystemAPI(APIClient):
    _RESOURCE_PATH = "/units/systems"

    def list(self) -> UnitSystemList:
        """`List all supported unit systems <https://developer.cognite.com/api#tag/Unit-Systems/operation/listUnitSystems>`_

        Returns:
            UnitSystemList: List of unit systems

        Examples:

            List all supported unit systems in CDF:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.units.systems.list()

        """
        return self._list(method="GET", list_cls=UnitSystemList, resource_cls=UnitSystem)
