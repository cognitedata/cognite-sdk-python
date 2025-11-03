"""
===============================================================================
63827e363f4f15859ad49b1b4bae34e0
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    TransformationDestination,
    TransformationSchemaColumnList,
)
from cognite.client.utils._async_helpers import run_sync


class SyncTransformationSchemaAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

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
        return run_sync(
            self.__async_client.transformations.schema.retrieve(destination=destination, conflict_mode=conflict_mode)
        )
