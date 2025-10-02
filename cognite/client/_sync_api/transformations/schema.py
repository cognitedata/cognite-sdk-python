"""
===============================================================================
63827e363f4f15859ad49b1b4bae34e0
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import Any, TypeVar

from cognite.client import AsyncCogniteClient
from cognite.client.data_classes import (
    TransformationDestination,
    TransformationSchemaColumnList,
)
from cognite.client.utils._concurrency import ConcurrencySettings

_T = TypeVar("_T")


class SyncTransformationSchemaAPI:
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    def _run_sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        executor = ConcurrencySettings._get_event_loop_executor()
        return executor.run_coro(coro)

    def retrieve(
        self, destination: TransformationDestination, conflict_mode: str | None = None
    ) -> TransformationSchemaColumnList:
        """
        `Get expected schema for a transformation destination. <https://developer.cognite.com/api#tag/Schema/operation/getTransformationSchema>`_

        Args:
            destination (TransformationDestination): destination for which the schema is requested.
            conflict_mode (str | None): conflict mode for which the schema is requested.

        Returns:
            TransformationSchemaColumnList: List of column descriptions

        Example:

            Get the schema for a transformation producing assets::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TransformationDestination
                >>> client = CogniteClient()
                >>> columns = client.transformations.schema.retrieve(destination = TransformationDestination.assets())
        """
        return self._run_sync(
            self.__async_client.transformations.schema.retrieve(destination=destination, conflict_mode=conflict_mode)
        )
