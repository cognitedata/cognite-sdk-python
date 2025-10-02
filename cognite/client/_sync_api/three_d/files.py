"""
===============================================================================
c23bdbc81be7b11616bd3dae81e7ef44
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client import AsyncCogniteClient
from cognite.client.utils._concurrency import ConcurrencySettings

if TYPE_CHECKING:
    pass


_T = TypeVar("_T")


class SyncThreeDFilesAPI:
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    def _run_sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        executor = ConcurrencySettings._get_event_loop_executor()
        return executor.run_coro(coro)

    def retrieve(self, id: int) -> bytes:
        """
        `Retrieve the contents of a 3d file by id. <https://developer.cognite.com/api#tag/3D-Files/operation/get3DFile>`_

        Args:
            id (int): The id of the file to retrieve.

        Returns:
            bytes: The contents of the file.

        Example:

            Retrieve the contents of a 3d file by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.three_d.files.retrieve(1)
        """
        return self._run_sync(self.__async_client.three_d.files.retrieve(id=id))
