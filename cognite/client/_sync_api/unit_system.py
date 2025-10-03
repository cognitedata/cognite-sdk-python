"""
===============================================================================
ebf94ce24437dd81bab208403099ec24
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import Any, TypeVar

from cognite.client import AsyncCogniteClient
from cognite.client.data_classes.units import UnitSystemList
from cognite.client.utils._concurrency import ConcurrencySettings

_T = TypeVar("_T")


class SyncUnitSystemAPI:
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    def _run_sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        executor = ConcurrencySettings._get_event_loop_executor()
        return executor.run_coro(coro)

    def list(self) -> UnitSystemList:
        """
        `List all supported unit systems <https://developer.cognite.com/api#tag/Unit-Systems/operation/listUnitSystems>`_

        Returns:
            UnitSystemList: List of unit systems

        Examples:

            List all supported unit systems in CDF:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.units.systems.list()
        """
        return self._run_sync(self.__async_client.units.systems.list())
