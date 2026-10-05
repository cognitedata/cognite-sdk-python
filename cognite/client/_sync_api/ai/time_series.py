"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.ai import ForecastResultList, ImputeResultList, InputTimeSeries
from cognite.client.utils._async_helpers import run_sync


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
