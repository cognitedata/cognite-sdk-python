"""
===============================================================================
b4f92e15db6249a70c4a32043aa5bebf
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.metering import MeteringData, MeteringDataList
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
from cognite.client.data_classes.filters import Prefix


class SyncMeteringAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def retrieve(
        self, id: str, start: int | None = None, end: int | None = None, number_of_datapoints: int | None = None
    ) -> MeteringData | None:
        """
        `Retrieve consumption data by its id <https://api-docs.cognite.com/20230101-alpha/tag/Metering/operation/fetchConsumptionDataById/>`_.

        Retrieves consumption data by its ``meterId``.

        Args:
            id (str): Meter ID to retrieve.
                Metering is identified by an id containing the service name and a service-scoped metering name.
                For instance ``atlas.monthly_ai_tokens`` is the id of the ``atlas`` service metering ``monthly_ai_tokens``.
                Service and metering names are always in ``lower_snake_case``.
            start (int | None): Start timestamp (inclusive) for historical data, in milliseconds since epoch.
                **Must be provided together with** ``number_of_datapoints`` to get time-series data.
                If omitted, only meter metadata is returned without time-series data.
            end (int | None): End timestamp (inclusive) for historical data, in milliseconds since epoch.
                Only relevant when ``start`` is provided. Defaults to the current time on the server if omitted.
            number_of_datapoints (int | None): Number of equal-width time buckets to return between ``start`` and ``end``.
                **Must be provided together with** ``start`` to get time-series data.
                Valid range: 0-600. API server default is 0 (metadata only).

        Returns:
            MeteringData | None: The requested consumption data, or ``None`` if not found.

        Examples:

            Retrieve a single meter by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.metering.retrieve(id="atlas.monthly_ai_tokens")

            Retrieve a single meter with historical data:

                >>> res = client.metering.retrieve(
                ...     id="atlas.monthly_ai_tokens",
                ...     start=1764547200000,
                ...     end=1767225599000,
                ...     number_of_datapoints=30,
                ... )
        """
        return run_sync(
            self.__async_client.metering.retrieve(
                id=id, start=start, end=end, number_of_datapoints=number_of_datapoints
            )
        )

    def retrieve_multiple(
        self, ids: list[str], start: int | None = None, end: int | None = None, number_of_datapoints: int | None = None
    ) -> MeteringDataList:
        """
        `Retrieve multiple consumption data by their ids <https://api-docs.cognite.com/20230101-alpha/tag/Metering/operation/fetchConsumptionDataByIds/>`_.

        Retrieves multiple consumption data items by their meter IDs.

        Args:
            ids (list[str]): List of meter IDs to retrieve.
            start (int | None): Start timestamp (inclusive) for historical data, in milliseconds since epoch.
                **Must be provided together with** ``number_of_datapoints`` to get time-series data.
                If omitted, only meter metadata is returned without time-series data.
            end (int | None): End timestamp (inclusive) for historical data, in milliseconds since epoch.
                Only relevant when ``start`` is provided. Defaults to the current time on the server if omitted.
            number_of_datapoints (int | None): Number of equal-width time buckets to return between ``start`` and ``end``.
                **Must be provided together with** ``start`` to get time-series data.
                Valid range: 0-600. API server default is 0 (metadata only).

        Returns:
            MeteringDataList: The requested consumption data items. Order matches the request.

        Examples:

            Retrieve multiple meters by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.metering.retrieve_multiple(
                ...     ids=["atlas.monthly_ai_tokens", "files.storage_bytes"]
                ... )
        """
        return run_sync(
            self.__async_client.metering.retrieve_multiple(
                ids=ids, start=start, end=end, number_of_datapoints=number_of_datapoints
            )
        )

    def list(
        self,
        filter: Prefix | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        start: int | None = None,
        end: int | None = None,
        number_of_datapoints: int | None = None,
    ) -> MeteringDataList:
        """
        `List all meters <https://api-docs.cognite.com/20230101-alpha/tag/Metering/operation/listConsumptionData/>`_.

        Lists all available meters for a specific project. Optionally filter by meter ID prefix using a ``Prefix`` filter.

        Args:
            filter (Prefix | None): Optional ``Prefix`` filter to apply on the ``meterId`` property (only ``Prefix`` filters are supported).
            limit (int | None): Maximum number of meters to return. Defaults to 1000. Set to ``None`` or ``-1`` to return all meters.
            start (int | None): Start timestamp (inclusive) for historical data, in milliseconds since epoch.
                **Must be provided together with** ``number_of_datapoints`` to get time-series data.
                If omitted, only meter metadata is returned without time-series data.
            end (int | None): End timestamp (inclusive) for historical data, in milliseconds since epoch.
                Only relevant when ``start`` is provided. Defaults to the current time on the server if omitted.
            number_of_datapoints (int | None): Number of equal-width time buckets to return between ``start`` and ``end``.
                **Must be provided together with** ``start`` to get time-series data.
                Valid range: 0-600. API server default is 0 (metadata only).

        Returns:
            MeteringDataList: List of all meters in the project.

        Examples:

            List all meters:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> meters = client.metering.list(limit=None)

            List meters filtered by prefix (e.g., all meters for the 'atlas' service):

                >>> from cognite.client.data_classes.filters import Prefix
                >>> prefix_filter = Prefix("meterId", "atlas.")
                >>> meters = client.metering.list(filter=prefix_filter)

            List meters with historical data:

                >>> meters = client.metering.list(
                ...     start=1764547200000,
                ...     end=1767225599000,
                ...     number_of_datapoints=30,
                ... )
        """
        return run_sync(
            self.__async_client.metering.list(
                filter=filter, limit=limit, start=start, end=end, number_of_datapoints=number_of_datapoints
            )
        )
