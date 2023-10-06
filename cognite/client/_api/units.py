from __future__ import annotations

from typing import TYPE_CHECKING, MutableSequence, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.units import (
    Unit,
    UnitList,
    UnitSystem,
    UnitSystemList,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence

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
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="beta", feature_name="Unit Catalogue")
        self.systems = UnitSystemAPI(config, api_version, cognite_client, self._warning)

    @overload
    def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> None | Unit:
        ...

    @overload
    def retrieve(self, external_id: MutableSequence[str], ignore_unknown_ids: bool = False) -> UnitList:
        ...

    def retrieve(
        self, external_id: str | MutableSequence[str], ignore_unknown_ids: bool = False
    ) -> Unit | UnitList | None:
        """`Retrieve one or more unit <https://pr-50.units-api.preview.cogniteapp.com/#tag/Units/operation/retrieve_units_by_ids_api_v1_projects__project__units_byids_post>`_

        Args:
            external_id (str | MutableSequence[str]): External ID or list of external IDs
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            Unit | UnitList | None: If a single external ID is specified: the requested unit, or None if it does not exist. If several external IDs are specified: the requested units.

        Examples:
            Retrive unit 'temperature:deg_c'

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.units.retrieve('temperature:deg_c')

            Retrive units 'temperature:deg_c' and 'pressure:bar'

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.units.retrieve(['temperature:deg_c', 'pressure:bar'])

        """
        self._warning.warn()
        identifier = IdentifierSequence.load(external_ids=external_id)
        return self._retrieve_multiple(
            identifiers=identifier,
            list_cls=UnitList,
            resource_cls=Unit,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(self) -> UnitList:
        """`List all supported units <https://pr-50.units-api.preview.cogniteapp.com/#tag/Units/operation/List_units_api_v1_projects__project__units_get>`_

        Returns:
            UnitList: List of units

        Examples:
            List all supported unit in CDF:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.units.list()
        """
        self._warning.warn()
        return self._list(method="GET", list_cls=UnitList, resource_cls=Unit)


class UnitSystemAPI(APIClient):
    _RESOURCE_PATH = "/units/systems"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
        warning: FeaturePreviewWarning,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = warning
        self._api_subversion = "beta"

    def list(self) -> UnitSystemList:
        """`List all supported unit systems <https://pr-50.units-api.preview.cogniteapp.com/#tag/Units/operation/list_unit_systems_api_v1_projects__project__units_systems_get>`_

        Returns:
            UnitSystemList: List of unit systems

        Examples:
            List all supported unit systems in CDF:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.units.systems.list()

        """
        self._warning.warn()
        return self._list(method="GET", list_cls=UnitSystemList, resource_cls=UnitSystem)
