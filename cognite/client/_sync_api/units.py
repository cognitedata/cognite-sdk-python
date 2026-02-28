"""
===============================================================================
dbc2ed4a0d0352b8e5b0712b0e53cd61
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.unit_system import SyncUnitSystemAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.units import Unit, UnitList
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncUnitAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.systems = SyncUnitSystemAPI(async_client)

    @overload
    def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> None | Unit: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> UnitList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Unit | UnitList | None:
        """
        `Retrieve one or more unit <https://api-docs.cognite.com/20230101/tag/Units/operation/byIdsUnits>`_

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            Unit | UnitList | None: If a single external ID is specified: the requested unit, or None if it does not exist. If several external IDs are specified: the requested units.

        Examples:

            Retrive unit 'temperature:deg_c':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.units.retrieve('temperature:deg_c')

            Retrive units 'temperature:deg_c' and 'pressure:bar':

                >>> res = client.units.retrieve(['temperature:deg_c', 'pressure:bar'])
        """
        return run_sync(
            self.__async_client.units.retrieve(external_id=external_id, ignore_unknown_ids=ignore_unknown_ids)
        )

    @overload
    def from_alias(
        self,
        alias: str,
        quantity: str | None = None,
        *,
        return_ambiguous: Literal[False] = False,
        return_closest_matches: Literal[False] = False,
    ) -> Unit: ...

    @overload
    def from_alias(
        self,
        alias: str,
        quantity: str | None = None,
        *,
        return_ambiguous: bool = False,
        return_closest_matches: bool = False,
    ) -> UnitList: ...

    def from_alias(
        self,
        alias: str,
        quantity: str | None = None,
        *,
        return_ambiguous: bool = False,
        return_closest_matches: bool = False,
    ) -> Unit | UnitList:
        """
        Look up a unit by alias, optionally for a given quantity. Aliases and quantities are case-sensitive.

        Note:
            When just ``alias`` is given (i.e. ``quantity`` is not specified), some aliases are ambiguous as they are used
            by several quantities, e.g. 'F' which can be both Farad (Capacitance) and Fahrenheit (Temperature). These raise
            a ValueError by default unless also ``return_ambiguous=True`` is passed, in which case all matching units are returned.

        Tip:
            You can use ``return_closest_matches=True`` to get the closest matching units if the lookup fails. Note that there
            may not be any close matches, in which case an empty UnitList is returned.

        Args:
            alias (str): Alias of the unit, like 'cmol / L' or 'meter per second'.
            quantity (str | None): Quantity of the unit, like 'Temperature' or 'Pressure'.
            return_ambiguous (bool): If False (default), when the alias is ambiguous (i.e. no quantity was given), raise a ValueError. If True, return the list of all matching units.
            return_closest_matches (bool): If False (default), when the lookup fails, raise a ValueError (default). If True, return the closest matching units (even if empty).

        Returns:
            Unit | UnitList: The unit if found, else a ValueError is raised. If one or both of ``return_ambiguous`` and ``return_closest_matches`` is passed as True, a UnitList may be returned.

        Examples:

                Look up a unit by alias only:

                    >>> from cognite.client import CogniteClient, AsyncCogniteClient
                    >>> client = CogniteClient()
                    >>> # async_client = AsyncCogniteClient()  # another option
                    >>> unit = client.units.from_alias('cmol / L')

                Look up ambiguous alias 'F' by passing quantity 'Temperature':

                    >>> unit = client.units.from_alias('F', 'Temperature')

                Search for the closest matching unit of 'kilo watt' (should be 'kilowatt'):

                    >>> unit_matches = client.units.from_alias("kilo watt", return_closest_matches=True)
        """
        return run_sync(
            self.__async_client.units.from_alias(
                alias=alias,
                quantity=quantity,
                return_ambiguous=return_ambiguous,
                return_closest_matches=return_closest_matches,
            )
        )

    def list(self) -> UnitList:
        """
        `List all supported units <https://api-docs.cognite.com/20230101/tag/Units/operation/listUnits>`_

        Returns:
            UnitList: List of units

        Examples:

            List all supported units in CDF:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.units.list()
        """
        return run_sync(self.__async_client.units.list())
