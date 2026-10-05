"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.ai.time_series_data import SyncAITimeSeriesDataAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.ai import ForecastResultList, ImputeResultList, InputTimeSeries
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    import pandas as pd


class SyncAITimeSeriesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.data = SyncAITimeSeriesDataAPI(async_client)

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
