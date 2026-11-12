"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.ai import ForecastInputTimeSeries, ForecastResultList, ImputeResultList
from cognite.client.utils._async_helpers import run_sync


class SyncAITimeSeriesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def forecast(self, time_series: Sequence[ForecastInputTimeSeries]) -> ForecastResultList:
        """
        Forecast the next 512 points of each time series from its history.

        No training or tuning on your data is needed. Each forecast point holds a set of quantiles, describing both
        the most likely value and the uncertainty around it. The points continue at the interval of the history,
        starting one interval after its last datapoint. Time series in the same cohort are forecast jointly, so the
        model can use the relationships between them. A time series without an underlying pattern, such as random
        noise, a flat line, or a setpoint changed by an operator, will not give a useful forecast.

        Note:
            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            time_series (Sequence[ForecastInputTimeSeries]): The time series to forecast. Each needs a unique label
                and between 2 and 8192 evenly spaced datapoints, at least one of which has a value and is not marked
                missing. Time series in a cohort must have identical timestamps.

        Returns:
            ForecastResultList: One result per input time series, in input order. `quantile_levels` lists the levels
                in every point's quantiles; use 0.5, the median, as a single predicted value.

        Raises:
            ValueError: If a label is repeated, a cohort has more than 32 time series, or a time series or cohort is
                over 5 MiB on its own. These are the API's limits for one request, and a cohort is always sent in one
                request.

        Examples:

            Forecast a time series, and get the median forecast as a DataFrame:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.ai import (
                ...     ForecastInputDatapoint,
                ...     ForecastInputTimeSeries,
                ... )
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> series = ForecastInputTimeSeries(
                ...     "21-PT-1019",
                ...     [
                ...         ForecastInputDatapoint(1790812800000, 42.1),
                ...         ForecastInputDatapoint(1790812860000, 42.4),
                ...     ],
                ... )
                >>> res = client.ai.time_series.forecast([series])
                >>> median = res.to_pandas()["21-PT-1019"]

            Forecast correlated time series jointly by giving them the same cohort, for example the compressors on
            one compression train:

                >>> res = client.ai.time_series.forecast(
                ...     [
                ...         ForecastInputTimeSeries(
                ...             "23-PT-1101",
                ...             [ForecastInputDatapoint(0, 42.1), ForecastInputDatapoint(60_000, 42.4)],
                ...             cohort="compression-train-a",
                ...         ),
                ...         ForecastInputTimeSeries(
                ...             "23-PT-1201",
                ...             [ForecastInputDatapoint(0, 39.8), ForecastInputDatapoint(60_000, 40.1)],
                ...             cohort="compression-train-a",
                ...         ),
                ...     ]
                ... )
        """
        return run_sync(self.__async_client.ai.time_series.forecast(time_series=time_series))

    def impute(self, time_series: Sequence[ForecastInputTimeSeries]) -> ImputeResultList:
        """
        Predict the value of every datapoint marked missing, from the datapoints on both sides of it.

        Use it to fill gaps: mark the datapoints in a gap as missing, and use the predicted median. Or use it to
        detect anomalies: mark observed datapoints as missing, and compare each observed value with its predicted
        quantiles. The API doesn't decide what is anomalous. Time series in the same cohort are imputed jointly, so
        the model can also use the other time series in the cohort.

        Note:
            A datapoint marked missing is hidden from the model, whatever its value, and returned. A datapoint with value
            None and not marked missing is hidden from the model but not returned.

            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            time_series (Sequence[ForecastInputTimeSeries]): The time series, with the datapoints to predict marked
                missing. They have the same requirements as in `forecast`.

        Returns:
            ImputeResultList: One result per input time series, in input order, with one point per datapoint marked
                missing. `quantile_levels` lists the levels in every point's quantiles; use 0.5, the median, as a
                single predicted value.

        Raises:
            ValueError: If a label is repeated, a cohort has more than 32 time series, or a time series or cohort is
                over 5 MiB on its own. These are the API's limits for one request, and a cohort is always sent in one
                request.

        Examples:

            Fill a one-point gap, and get the predicted median:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.ai import (
                ...     ForecastInputDatapoint,
                ...     ForecastInputTimeSeries,
                ... )
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> series = ForecastInputTimeSeries(
                ...     "21-PT-1019",
                ...     [
                ...         ForecastInputDatapoint(0, 42.1),
                ...         ForecastInputDatapoint(60_000, None, missing=True),
                ...         ForecastInputDatapoint(120_000, 42.9),
                ...     ],
                ... )
                >>> res = client.ai.time_series.impute([series])
                >>> median = res.to_pandas()["21-PT-1019"]
        """
        return run_sync(self.__async_client.ai.time_series.impute(time_series=time_series))
