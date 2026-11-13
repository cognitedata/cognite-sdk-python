"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.ai import (
    ForecastInputTimeSeries,
    ForecastResult,
    ForecastResultList,
    ImputeResult,
    ImputeResultList,
)
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    import pandas as pd


class SyncAITimeSeriesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def forecast(self, time_series: ForecastInputTimeSeries) -> ForecastResult: ...

    @overload
    def forecast(self, time_series: Sequence[ForecastInputTimeSeries]) -> ForecastResultList: ...

    def forecast(
        self, time_series: ForecastInputTimeSeries | Sequence[ForecastInputTimeSeries]
    ) -> ForecastResult | ForecastResultList:
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
            time_series (ForecastInputTimeSeries | Sequence[ForecastInputTimeSeries]): The time series to forecast.
                Each needs a unique label and between 2 and 8192 evenly spaced datapoints, at least one of which has a
                value and is not marked missing. Time series in a cohort must have identical timestamps.

        Returns:
            ForecastResult | ForecastResultList: The result for a single time series, or one result per time series, in
                input order. Use the 0.5 quantile, the median, as a single predicted value.

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
                >>> res = client.ai.time_series.forecast(series)
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

    @overload
    def impute(self, time_series: ForecastInputTimeSeries) -> ImputeResult: ...

    @overload
    def impute(self, time_series: Sequence[ForecastInputTimeSeries]) -> ImputeResultList: ...

    def impute(
        self, time_series: ForecastInputTimeSeries | Sequence[ForecastInputTimeSeries]
    ) -> ImputeResult | ImputeResultList:
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
            time_series (ForecastInputTimeSeries | Sequence[ForecastInputTimeSeries]): The time series, with the
                datapoints to predict marked missing. They have the same requirements as in `forecast`.

        Returns:
            ImputeResult | ImputeResultList: The result for a single time series, or one result per time series, in
                input order, with one point per datapoint marked missing. Use the 0.5 quantile, the median, as a single
                predicted value.

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
                >>> res = client.ai.time_series.impute(series)
                >>> median = res.to_pandas()["21-PT-1019"]
        """
        return run_sync(self.__async_client.ai.time_series.impute(time_series=time_series))

    def forecast_dataframe(
        self,
        df: pd.DataFrame,
        cohort: str | Mapping[str, str] | None = None,
        missing: pd.DataFrame | None = None,
        include_all_quantiles: bool = False,
    ) -> pd.DataFrame:
        """
        Forecast the next 512 points of every column of a DataFrame.

        Each column is one time series, labelled by its column name. A NaN value is a gap with no reading, sent as a
        datapoint without a value, so the model doesn't see it. A datapoint marked in `missing` is hidden from the
        model, whatever its value, for example to exclude a segment of the history. See `forecast` for how the
        forecast is made.

        Note:
            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            df (pd.DataFrame): The history, one numeric column per time series, named by its label, with an evenly
                spaced DatetimeIndex of 2 to 8192 timestamps. A timezone-naive index is taken to be in UTC.
            cohort (str | Mapping[str, str] | None): A cohort for every column, or a mapping from column name to
                cohort. Columns not in the mapping are forecast independently.
            missing (pd.DataFrame | None): Booleans with the same index and columns as `df`, True for each datapoint
                to hide from the model. By default, none is marked.
            include_all_quantiles (bool): Include every quantile level instead of only the median, as
                `(column, quantile level)` columns.

        Returns:
            pd.DataFrame: The forecast, like `to_pandas` of the result: a timezone-naive DatetimeIndex in UTC, and the
                median in one column for every column of `df`, or every quantile level with `include_all_quantiles`.

        Raises:
            ValueError: If the index isn't a DatetimeIndex, a column name isn't a string, a column is repeated or not
                numeric, a value is infinite,
                the cohort mapping names a column that isn't in the DataFrame, or `missing` isn't booleans with the same
                index and columns as `df`. Also for the reasons `forecast` raises.

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
                >>> median = forecast["23-PT-1101"]
        """
        return run_sync(
            self.__async_client.ai.time_series.forecast_dataframe(
                df=df, cohort=cohort, missing=missing, include_all_quantiles=include_all_quantiles
            )
        )

    def impute_dataframe(
        self,
        df: pd.DataFrame,
        cohort: str | Mapping[str, str] | None = None,
        missing: pd.DataFrame | None = None,
        include_all_quantiles: bool = False,
    ) -> pd.DataFrame:
        """
        Predict the datapoints of a DataFrame that are marked missing.

        Each column is one time series, labelled by its column name. A datapoint marked in `missing` is hidden from
        the model, whatever its value, and predicted: mark the gaps to fill them, or observed values to compare them
        with their predictions. A NaN value that isn't marked is a gap with no reading: the model doesn't see it, and
        it isn't predicted. See `impute` for how the prediction is made.

        Note:
            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            df (pd.DataFrame): The time series, one numeric column per time series, named by its label, with an
                evenly spaced DatetimeIndex of 2 to 8192 timestamps. A timezone-naive index is taken to be in UTC.
            cohort (str | Mapping[str, str] | None): A cohort for every column, or a mapping from column name to
                cohort. Columns not in the mapping are imputed independently.
            missing (pd.DataFrame | None): Booleans with the same index and columns as `df`, True for each datapoint
                to predict. By default, none is marked, so nothing is predicted.
            include_all_quantiles (bool): Include every quantile level instead of only the median, as
                `(column, quantile level)` columns.

        Returns:
            pd.DataFrame: The predictions, like `to_pandas` of the result: one row per timestamp marked missing in
                any column, a timezone-naive DatetimeIndex in UTC, and the median in one column for every column of
                `df`, or every quantile level with `include_all_quantiles`.

        Raises:
            ValueError: If the index isn't a DatetimeIndex, a column name isn't a string, a column is repeated or not
                numeric, a value is infinite,
                the cohort mapping names a column that isn't in the DataFrame, or `missing` isn't booleans with the same
                index and columns as `df`. Also for the reasons `impute` raises.

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
                >>> imputed = client.ai.time_series.impute_dataframe(df, missing=df.isna())
                >>> filled = df.fillna(imputed)  # doctest: +SKIP
        """
        return run_sync(
            self.__async_client.ai.time_series.impute_dataframe(
                df=df, cohort=cohort, missing=missing, include_all_quantiles=include_all_quantiles
            )
        )
