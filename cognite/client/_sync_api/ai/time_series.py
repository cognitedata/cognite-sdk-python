"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

import datetime
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.ai import (
    ForecastResultList,
    ImputeResultList,
    InputTimeSeries,
    TimeSeriesForecast,
    TimeSeriesForecastList,
    TimeSeriesImpute,
    TimeSeriesImputeList,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoint_aggregates import Aggregate
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    import pandas as pd


class SyncAITimeSeriesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def forecast(self, time_series: InputTimeSeries | Sequence[InputTimeSeries]) -> ForecastResultList:
        """
        Forecast one or more series 512 steps ahead, with quantile estimates for every step.

        Requests with more than 256 series are split automatically. A cohort is never split across requests.

        Args:
            time_series (InputTimeSeries | Sequence[InputTimeSeries]): The history to forecast from.

        Returns:
            ForecastResultList: One result per input series, in input order, with `quantile_levels` and `cohorts`.

        Examples:

            Forecast a single series:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.ai import InputTimeSeries
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.ai.time_series.forecast(
                ...     InputTimeSeries("21-PT-1019", [(1790812800000, 42.1), (1790812860000, 42.4)])
                ... )

            Forecast correlated series jointly by giving them the same cohort, for example compressors on the
            same compression train:

                >>> res = client.ai.time_series.forecast(
                ...     [
                ...         InputTimeSeries(
                ...             "23-PT-1101", [(0, 42.1), (60_000, 42.4)], cohort="compression-train-a"
                ...         ),
                ...         InputTimeSeries(
                ...             "23-PT-1201", [(0, 39.8), (60_000, 40.1)], cohort="compression-train-a"
                ...         ),
                ...     ]
                ... )
        """
        return run_sync(self.__async_client.ai.time_series.forecast(time_series=time_series))

    def impute(self, time_series: InputTimeSeries | Sequence[InputTimeSeries]) -> ImputeResultList:
        """
        Reconstruct the datapoints marked missing, with quantile estimates.

        Takes the same input as `forecast`. A point with `missing=True` is hidden from the model and reconstructed;
        a point with `value=None` and no `missing` flag is hidden but not returned.

        Args:
            time_series (InputTimeSeries | Sequence[InputTimeSeries]): The history, with the points to reconstruct
                marked `missing=True`.

        Returns:
            ImputeResultList: One result per input series, in input order, with `quantile_levels` and `cohorts`.

        Examples:

            Reconstruct one missing point:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.ai import InputDatapoint, InputTimeSeries
                >>> client = CogniteClient()
                >>> res = client.ai.time_series.impute(
                ...     InputTimeSeries(
                ...         "21-PT-1019",
                ...         [
                ...             InputDatapoint(0, 42.1),
                ...             InputDatapoint(60_000, None, missing=True),
                ...             InputDatapoint(120_000, 42.9),
                ...         ],
                ...     )
                ... )
        """
        return run_sync(self.__async_client.ai.time_series.impute(time_series=time_series))

    def forecast_dataframe(self, df: pd.DataFrame, cohort: str | Mapping[str, str] | None = None) -> pd.DataFrame:
        """
        Forecast every column of a DataFrame 512 steps ahead.

        Each column is one series, labelled by the column name. NaN values are sent as missing points.

        Args:
            df (pd.DataFrame): Evenly spaced history with a DatetimeIndex, one column per series.
            cohort (str | Mapping[str, str] | None): One cohort for every column, or a `{column: cohort}` mapping.
                Columns not in the mapping are forecast independently.

        Returns:
            pd.DataFrame: The forecasts, with a DatetimeIndex and `(column, quantile)` columns.

        Examples:

            Forecast two compressor pressures jointly, and a temperature independently:

                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> df = pd.DataFrame(
                ...     {
                ...         "23-PT-1101": [42.1, 42.4, 42.9],
                ...         "23-PT-1201": [39.8, 40.1, 40.0],
                ...         "24-TT-3001": [31.0, 31.2, 30.9],
                ...     },
                ...     index=pd.date_range("2026-10-01", periods=3, freq="1min"),
                ... )
                >>> forecast = client.ai.time_series.forecast_dataframe(
                ...     df,
                ...     cohort={
                ...         "23-PT-1101": "compression-train-a",
                ...         "23-PT-1201": "compression-train-a",
                ...     },
                ... )
        """
        return run_sync(self.__async_client.ai.time_series.forecast_dataframe(df=df, cohort=cohort))

    def impute_dataframe(self, df: pd.DataFrame, cohort: str | Mapping[str, str] | None = None) -> pd.DataFrame:
        """
        Reconstruct the NaN values in every column of a DataFrame.

        Each column is one series, labelled by the column name. NaN values are sent as missing points and
        reconstructed.

        Args:
            df (pd.DataFrame): Evenly spaced history with a DatetimeIndex, one column per series.
            cohort (str | Mapping[str, str] | None): One cohort for every column, or a `{column: cohort}` mapping.
                Columns not in the mapping are imputed independently.

        Returns:
            pd.DataFrame: The reconstructed points, with a DatetimeIndex and `(column, quantile)` columns.

        Examples:

            Reconstruct a gap in a pressure measurement:

                >>> import numpy as np
                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> df = pd.DataFrame(
                ...     {"21-PT-1019": [42.1, np.nan, 42.9]},
                ...     index=pd.date_range("2026-10-01", periods=3, freq="1min"),
                ... )
                >>> imputed = client.ai.time_series.impute_dataframe(df)
        """
        return run_sync(self.__async_client.ai.time_series.impute_dataframe(df=df, cohort=cohort))

    @overload
    def forecast_with_retrieval(
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
    def forecast_with_retrieval(
        self,
        *,
        instance_id: Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    def forecast_with_retrieval(
        self,
        *,
        instance_id: NodeId | Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast | TimeSeriesForecastList:
        """
        Retrieve the history of time series in `[start, end)` and forecast 512 steps past their last datapoint.

        The forecasting model needs evenly spaced history, and this method never resamples or truncates the data.
        With `granularity` and `aggregate`, the history is aligned using CDF aggregates, and empty buckets are sent as
        missing. Without them, raw datapoints are used, and they must already be evenly spaced; gaps are allowed.

        Args:
            instance_id (NodeId | Sequence[NodeId]): Instance id(s) of the time series.
            start (int | str | datetime.datetime): Start of the history window, for example "30d-ago".
            end (int | str | datetime.datetime | None): End of the history window. Defaults to now.
            granularity (str | None): Align the history using aggregates at this granularity, for example "1h".
                Requires `aggregate`.
            aggregate (Aggregate | str | None): The aggregate to forecast, for example "average" or "interpolation".
            cohort (str | None): Forecast all the requested time series jointly, aligned on one common grid. Omit it
                to forecast each time series independently.

        Returns:
            TimeSeriesForecast | TimeSeriesForecastList: A single forecast if a single instance id was passed,
            otherwise a list you can look up with `.get(instance_id=...)`.

        Examples:

            Forecast a time series from 30 days of hourly averages:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> fc = client.ai.time_series.forecast_with_retrieval(
                ...     instance_id=NodeId("my_space", "21-PT-1019"),
                ...     start="30d-ago",
                ...     granularity="1h",
                ...     aggregate="average",
                ... )

            Forecast the discharge pressures of three compressors on one compression train jointly:

                >>> ids = [NodeId("my_space", xid) for xid in ("23-PT-1101", "23-PT-1201", "23-PT-1301")]
                >>> res = client.ai.time_series.forecast_with_retrieval(
                ...     instance_id=ids,
                ...     start="14d-ago",
                ...     granularity="10m",
                ...     aggregate="average",
                ...     cohort="compression-train-a",
                ... )
                >>> fc = res.get(instance_id=ids[1])
        """
        return run_sync(
            self.__async_client.ai.time_series.forecast_with_retrieval(
                instance_id=instance_id,
                start=start,
                end=end,
                granularity=granularity,
                aggregate=aggregate,
                cohort=cohort,
            )
        )

    @overload
    def impute_with_retrieval(
        self,
        *,
        instance_id: NodeId,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
        fill_gaps: bool = False,
        mask: Sequence[
            int | str | datetime.datetime | tuple[int | str | datetime.datetime, int | str | datetime.datetime]
        ]
        | None = None,
    ) -> TimeSeriesImpute: ...

    @overload
    def impute_with_retrieval(
        self,
        *,
        instance_id: Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
        fill_gaps: bool = False,
        mask: Sequence[
            int | str | datetime.datetime | tuple[int | str | datetime.datetime, int | str | datetime.datetime]
        ]
        | None = None,
    ) -> TimeSeriesImputeList: ...

    def impute_with_retrieval(
        self,
        *,
        instance_id: NodeId | Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
        fill_gaps: bool = False,
        mask: Sequence[
            int | str | datetime.datetime | tuple[int | str | datetime.datetime, int | str | datetime.datetime]
        ]
        | None = None,
    ) -> TimeSeriesImpute | TimeSeriesImputeList:
        """
        Retrieve the history of time series in `[start, end)` and reconstruct gaps and chosen datapoints.

        You must say what to reconstruct: `fill_gaps=True`, `mask=[...]`, or both. The history is aligned the same way
        as in `forecast_with_retrieval`, and is never resampled or truncated.

        Args:
            instance_id (NodeId | Sequence[NodeId]): Instance id(s) of the time series.
            start (int | str | datetime.datetime): Start of the history window, for example "7d-ago".
            end (int | str | datetime.datetime | None): End of the history window. Defaults to now.
            granularity (str | None): Align the history using aggregates at this granularity, for example "10m".
                Requires `aggregate`.
            aggregate (Aggregate | str | None): The aggregate to reconstruct, for example "average".
            cohort (str | None): Reconstruct all the requested time series jointly, aligned on one common grid.
            fill_gaps (bool): Reconstruct every grid point without a datapoint (with aggregates, every empty bucket).
            mask (Sequence[int | str | datetime.datetime | tuple[int | str | datetime.datetime, int | str | datetime.datetime]] | None): Timestamps, or inclusive `(start, end)` ranges, to hide from the model
                and reconstruct even though values exist.

        Returns:
            TimeSeriesImpute | TimeSeriesImputeList: A single result if a single instance id was passed, otherwise a
            list you can look up with `.get(instance_id=...)`.

        Examples:

            Fill every empty 10-minute bucket in the last week:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.ai.time_series.impute_with_retrieval(
                ...     instance_id=NodeId("my_space", "21-PT-1019"),
                ...     start="7d-ago",
                ...     granularity="10m",
                ...     aggregate="average",
                ...     fill_gaps=True,
                ... )

            Reconstruct a period you know is bad, for example during a sensor fault:

                >>> from datetime import datetime, timezone
                >>> res = client.ai.time_series.impute_with_retrieval(
                ...     instance_id=NodeId("my_space", "21-PT-1019"),
                ...     start="7d-ago",
                ...     granularity="10m",
                ...     aggregate="average",
                ...     mask=[
                ...         (
                ...             datetime(2026, 9, 30, 8, tzinfo=timezone.utc),
                ...             datetime(2026, 9, 30, 12, tzinfo=timezone.utc),
                ...         )
                ...     ],
                ... )
        """
        return run_sync(
            self.__async_client.ai.time_series.impute_with_retrieval(
                instance_id=instance_id,
                start=start,
                end=end,
                granularity=granularity,
                aggregate=aggregate,
                cohort=cohort,
                fill_gaps=fill_gaps,
                mask=mask,
            )
        )
