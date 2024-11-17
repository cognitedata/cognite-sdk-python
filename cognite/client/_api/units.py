from __future__ import annotations

import difflib
from collections import defaultdict
from functools import cached_property
from itertools import chain
from typing import TYPE_CHECKING, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.units import (
    Unit,
    UnitList,
    UnitSystem,
    UnitSystemList,
)
from cognite.client.utils._auxiliary import remove_duplicates_keep_order
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

    @cached_property
    def _create_unit_lookups(self) -> tuple[dict[str, dict[str, Unit]], dict[str, list[Unit]]]:
        units = self.list()
        alias_by_quantity: defaultdict[str, dict[str, Unit]] = defaultdict(dict)
        for unit in units:
            dct = alias_by_quantity[unit.quantity]
            # fun fact, for some units, alias_names has duplicates:
            for alias in unit.alias_names:
                dct[alias] = unit

        alias_lookup = defaultdict(list)
        for dct in alias_by_quantity.values():
            for alias, unit in dct.items():
                alias_lookup[alias].append(unit)
        # we want failed lookups to raise, so we convert to dict:
        return dict(alias_by_quantity), dict(alias_lookup)

    @overload
    def from_alias(
        self,
        alias: str,
        quantity: str | None,
        return_ambiguous: Literal[False],
        return_closest_matches: Literal[False],
    ) -> Unit: ...

    @overload
    def from_alias(
        self,
        alias: str,
        quantity: str | None,
        return_ambiguous: bool,
        return_closest_matches: bool,
    ) -> Unit | UnitList: ...

    def from_alias(
        self,
        alias: str,
        quantity: str | None = None,
        return_ambiguous: bool = False,
        return_closest_matches: bool = False,
    ) -> Unit | UnitList:
        """Look up a unit by alias, optionally for a given quantity. Aliases and quantities are case-sensitive.

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

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> unit = client.units.from_alias('cmol / L')

                Look up ambiguous alias 'F' by passing quantity 'Temperature':

                    >>> unit = client.units.from_alias('F', 'Temperature')

                Search for the closest matching unit of 'kilo watt' (should be 'kilowatt'):

                    >>> unit_matches = client.units.from_alias("kilo watt", return_closest_matches=True):
        """
        alias_by_quantity, alias_lookup = self._create_unit_lookups
        if quantity is None:
            return self._lookup_unit_by_alias(alias, alias_lookup, return_ambiguous, return_closest_matches)
        else:
            return self._lookup_unit_by_alias_and_quantity(alias, alias_by_quantity, quantity, return_closest_matches)

    @staticmethod
    def _lookup_unit_by_alias(
        alias: str, alias_lookup: dict[str, list[Unit]], return_ambiguous: bool, return_closest_matches: bool
    ) -> Unit | UnitList:
        try:
            unit, *extra = alias_lookup[alias]
            if not extra:
                return unit
            elif return_ambiguous:
                return UnitList([unit, *extra])
            raise ValueError(f"Ambiguous alias, matches all of: {[u.external_id for u in (unit, *extra)]}") from None

        except KeyError:
            err_msg = f"Unknown {alias=}"
            close_matches = difflib.get_close_matches(alias, alias_lookup, n=10)
            if return_closest_matches:
                return UnitList(
                    remove_duplicates_keep_order(chain.from_iterable(alias_lookup[m] for m in close_matches))
                )
            if close_matches:
                err_msg += f", did you mean one of: {sorted(close_matches)}?"
            raise ValueError(err_msg) from None

    @staticmethod
    def _lookup_unit_by_alias_and_quantity(
        alias: str, alias_by_quantity: dict[str, dict[str, Unit]], quantity: str, return_closest_matches: bool
    ) -> Unit | UnitList:
        try:
            quantity_dct = alias_by_quantity[quantity]
        except KeyError:
            # All except one are title-cased (API Gravity - which stands for 'American Petroleum Institute' obviously...)
            if quantity.title() in alias_by_quantity:
                quantity_dct = alias_by_quantity[quantity.title()]
            else:
                err_msg = f"Unknown {quantity=}."
                if close_matches := difflib.get_close_matches(quantity, alias_by_quantity, n=3):
                    err_msg += f" Did you mean one of: {close_matches}?"
                raise ValueError(err_msg + f" All known quantities: {sorted(alias_by_quantity)}") from None
        try:
            return quantity_dct[alias]
        except KeyError:
            err_msg = f"Unknown {alias=} for known {quantity=}."
            if close_matches := difflib.get_close_matches(alias, quantity_dct, n=3):
                if return_closest_matches:
                    return UnitList(remove_duplicates_keep_order(quantity_dct[m] for m in close_matches))
                err_msg += f" Did you mean one of: {close_matches}?"
            raise ValueError(err_msg) from None

    def list(self) -> UnitList:
        """`List all supported units <https://developer.cognite.com/api#tag/Units/operation/listUnits>`_

        Returns:
            UnitList: List of units

        Examples:

            List all supported unit in CDF:

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

            List all supported unit systems in CDF:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.units.systems.list()

        """
        return self._list(method="GET", list_cls=UnitSystemList, resource_cls=UnitSystem)
