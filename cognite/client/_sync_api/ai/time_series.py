"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Hashable, Mapping, Sequence
from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.ai import (
    ForecastInputTimeSeries,
    ForecastResultList,
    ImputeResultList,
)
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    import pandas as pd


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

        The time series are sent in as many requests as needed to stay within the API's limits of 32 time series and
        5 MiB per request. A cohort is always sent in one request.

        Note:
            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            time_series (Sequence[ForecastInputTimeSeries]): The time series to forecast. Each needs a unique label
                and 2 to 8192 evenly spaced datapoints, at least one of which has a value and is not marked missing.
                Time series in a cohort must have identical timestamps.

        Returns:
            ForecastResultList: One result per input time series, in input order. `quantile_levels` lists the levels
                in every point's quantiles; use 0.5, the median, as a single predicted value.

        Raises:
            ValueError: If no time series are given, a label is repeated, or a cohort doesn't fit in one request.

        Examples:

            Forecast a time series, and get the forecast as a DataFrame with one column per quantile level:

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
                >>> median = res.to_pandas()["21-PT-1019"][0.5]

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

        A datapoint marked missing is hidden from the model, whatever its value, and returned. A datapoint with value
        None and not marked missing is hidden from the model but not returned.

        The time series are sent in as many requests as needed to stay within the API's limits of 32 time series and
        5 MiB per request. A cohort is always sent in one request.

        Note:
            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            time_series (Sequence[ForecastInputTimeSeries]): The time series, with the datapoints to predict marked
                missing. Each needs a unique label and 2 to 8192 evenly spaced datapoints, at least one of which has
                a value and is not marked missing. Time series in a cohort must have identical timestamps.

        Returns:
            ImputeResultList: One result per input time series, in input order, with one point per datapoint marked
                missing. `quantile_levels` lists the levels in every point's quantiles; use 0.5, the median, as a
                single predicted value.

        Raises:
            ValueError: If no time series are given, a label is repeated, or a cohort doesn't fit in one request.

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
                >>> median = res.to_pandas()["21-PT-1019"][0.5]
        """
        return run_sync(self.__async_client.ai.time_series.impute(time_series=time_series))

    def forecast_dataframe(
        self,
        df: pd.DataFrame,
        cohort: str | Mapping[Hashable, str] | None = None,
        quantile_levels: Sequence[float] | None = (0.5,),
    ) -> pd.DataFrame:
        """
        Forecast the next 512 points of every column of a DataFrame.

        Each column is one time series, labelled by its column name. NaN values are hidden from the model. See
        `forecast` for how the forecast is made.

        Note:
            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            df (pd.DataFrame): The history, one numeric column per time series, with an evenly spaced DatetimeIndex
                of 2 to 8192 timestamps. A timezone-naive index is taken to be in UTC.
            cohort (str | Mapping[Hashable, str] | None): A cohort for every column, or a mapping from column name to
                cohort. Columns not in the mapping are forecast independently.
            quantile_levels (Sequence[float] | None): The quantile levels to include. Defaults to the median, 0.5. Pass
                None to include every level the API returns.

        Returns:
            pd.DataFrame: The forecast, with a DatetimeIndex in the timezone of `df`, and a `(column, quantile level)`
                column for every column of `df` and quantile level included.

        Raises:
            ValueError: If the index isn't a DatetimeIndex, a column is repeated or not numeric, a value is infinite,
                or the cohort mapping names a column that isn't in the DataFrame. Also for the reasons `forecast`
                raises.

        Examples:

            Forecast two compressor pressures jointly and a temperature independently, and get the median forecast
            of one of them:

                >>> import pandas as pd
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
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
                >>> median = forecast["23-PT-1101"][0.5]
        """
        return run_sync(
            self.__async_client.ai.time_series.forecast_dataframe(df=df, cohort=cohort, quantile_levels=quantile_levels)
        )

    def impute_dataframe(
        self,
        df: pd.DataFrame,
        cohort: str | Mapping[Hashable, str] | None = None,
        quantile_levels: Sequence[float] | None = (0.5,),
    ) -> pd.DataFrame:
        """
        Predict every NaN value of a DataFrame.

        Each column is one time series, labelled by its column name. Every NaN value is marked missing, so it's
        hidden from the model and predicted. See `impute` for how the prediction is made.

        Note:
            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            df (pd.DataFrame): The time series, one numeric column per time series, with an evenly spaced
                DatetimeIndex of 2 to 8192 timestamps. A timezone-naive index is taken to be in UTC.
            cohort (str | Mapping[Hashable, str] | None): A cohort for every column, or a mapping from column name to
                cohort. Columns not in the mapping are imputed independently.
            quantile_levels (Sequence[float] | None): The quantile levels to include. Defaults to the median, 0.5. Pass
                None to include every level the API returns.

        Returns:
            pd.DataFrame: The predictions, with one row per timestamp that has a NaN value in any column, a
                DatetimeIndex in the timezone of `df`, and a `(column, quantile level)` column for every column of
                `df` and quantile level included.

        Raises:
            ValueError: If the index isn't a DatetimeIndex, a column is repeated or not numeric, a value is infinite,
                or the cohort mapping names a column that isn't in the DataFrame. Also for the reasons `impute`
                raises.

        Examples:

            Fill the gaps in a pressure measurement with the predicted median:

                >>> import numpy as np
                >>> import pandas as pd
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> df = pd.DataFrame(
                ...     {"21-PT-1019": [42.1, np.nan, 42.9]},
                ...     index=pd.date_range("2026-10-01", periods=3, freq="1min"),
                ... )
                >>> imputed = client.ai.time_series.impute_dataframe(df)
                >>> filled = df.fillna(imputed.xs(0.5, axis=1, level=1))  # doctest: +SKIP
        """
        return run_sync(
            self.__async_client.ai.time_series.impute_dataframe(df=df, cohort=cohort, quantile_levels=quantile_levels)
        )
