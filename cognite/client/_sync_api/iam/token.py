"""
===============================================================================
5268479111509d912fc224eb231afa08
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client import AsyncCogniteClient
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.utils._concurrency import ConcurrencySettings

if TYPE_CHECKING:
    pass


_T = TypeVar("_T")


class SyncTokenAPI:
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    def _run_sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        executor = ConcurrencySettings._get_event_loop_executor()
        return executor.run_coro(coro)

    def inspect(self) -> TokenInspection:
        """
        Inspect a token.

        Get details about which projects it belongs to and which capabilities are granted to it.

        Returns:
            TokenInspection: The object with token inspection details.

        Example:

            Inspect token::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.token.inspect()
        """
        return self._run_sync(self.__async_client.iam.token.inspect())
