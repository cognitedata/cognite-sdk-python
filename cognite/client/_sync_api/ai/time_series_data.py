"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.ai import TimeSeriesForecast, TimeSeriesForecastList
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoint_aggregates import Aggregate
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class SyncAITimeSeriesDataAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def forecast(
        self,
        *,
        id: int,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast: ...

    @overload
    def forecast(
        self,
        *,
        id: Sequence[int],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    @overload
    def forecast(
        self,
        *,
        external_id: str,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast: ...

    @overload
    def forecast(
        self,
        *,
        external_id: SequenceNotStr[str],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    @overload
    def forecast(
        self,
        *,
        instance_id: NodeId,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast: ...

    @overload
    def forecast(
        self,
        *,
        instance_id: Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    @overload
    def forecast(
        self,
        *,
        id: int | Sequence[int] | None,
        external_id: str | SequenceNotStr[str] | None,
        instance_id: NodeId | Sequence[NodeId] | None,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    def forecast(
        self,
        *,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        instance_id: NodeId | Sequence[NodeId] | None = None,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast | TimeSeriesForecastList:
        """
        Forecast time series 512 steps past their last datapoint in `[start, end)`.

        The forecasting model needs evenly spaced history, and this method never resamples or truncates the data.
        With `granularity` and `aggregate`, the history is aligned using CDF aggregates, and empty buckets are sent as
        missing. Without them, raw datapoints are used, and they must already be evenly spaced; gaps are allowed.

        Args:
            id (int | Sequence[int] | None): Id(s) of the time series.
            external_id (str | SequenceNotStr[str] | None): External id(s) of the time series.
            instance_id (NodeId | Sequence[NodeId] | None): Instance id(s) of the time series.
            start (int | str | datetime.datetime): Start of the history window, for example "30d-ago".
            end (int | str | datetime.datetime | None): End of the history window. Defaults to now.
            granularity (str | None): Align the history using aggregates at this granularity, for example "1h".
                Requires `aggregate`.
            aggregate (Aggregate | str | None): The aggregate to forecast, for example "average" or "interpolation".
            cohort (str | None): Forecast all the requested time series jointly, aligned on one common grid. Omit it
                to forecast each time series independently.

        Returns:
            TimeSeriesForecast | TimeSeriesForecastList: A single forecast if a single identifier was passed, otherwise
            a list you can look up with `.get(id=..., external_id=..., instance_id=...)`.

        Examples:

            Forecast a time series from 30 days of hourly averages:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> fc = client.ai.time_series.data.forecast(
                ...     external_id="21-PT-1019", start="30d-ago", granularity="1h", aggregate="average"
                ... )

            Forecast the discharge pressures of three compressors on one compression train jointly:

                >>> res = client.ai.time_series.data.forecast(
                ...     external_id=["23-PT-1101", "23-PT-1201", "23-PT-1301"],
                ...     start="14d-ago",
                ...     granularity="10m",
                ...     aggregate="average",
                ...     cohort="compression-train-a",
                ... )
                >>> fc = res.get(external_id="23-PT-1201")
        """
        return run_sync(
            self.__async_client.ai.time_series.data.forecast(
                id=id,
                external_id=external_id,
                instance_id=instance_id,
                start=start,
                end=end,
                granularity=granularity,
                aggregate=aggregate,
                cohort=cohort,
            )
        )
