from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, TypeVar

from cognite.client._api_client import APIClient
from cognite.client.data_classes.ai import (
    ForecastInputTimeSeries,
    ForecastResultList,
    ImputeResultList,
)
from cognite.client.utils._auxiliary import find_duplicates
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient, ClientConfig

T_ResultList = TypeVar("T_ResultList", ForecastResultList, ImputeResultList)


class AITimeSeriesAPI(APIClient):
    _RESOURCE_PATH = "/ai/timeseries"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Time series forecasting and imputation"
        )

    async def forecast(self, time_series: Sequence[ForecastInputTimeSeries]) -> ForecastResultList:
        """Forecast the next 512 points of each time series from its history.

        No training or tuning on your data is needed. Each forecast point holds a set of quantiles, describing both
        the most likely value and the uncertainty around it. The points continue at the interval of the history,
        starting one interval after its last datapoint. Time series in the same cohort are forecast jointly, so the
        model can use the relationships between them. A time series without an underlying pattern, such as random
        noise, a flat line, or a setpoint changed by an operator, will not give a useful forecast.

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
            ValueError: If no time series are given, or a label is repeated.

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
        return await self._forecast_or_impute("/forecast", time_series, ForecastResultList)

    async def impute(self, time_series: Sequence[ForecastInputTimeSeries]) -> ImputeResultList:
        """Predict the value of every datapoint marked missing, from the datapoints on both sides of it.

        Use it to fill gaps: mark the datapoints in a gap as missing, and use the predicted median. Or use it to
        detect anomalies: mark observed datapoints as missing, and compare each observed value with its predicted
        quantiles. The API doesn't decide what is anomalous. Time series in the same cohort are imputed jointly, so
        the model can also use the other time series in the cohort.

        A datapoint marked missing is hidden from the model, whatever its value, and returned. A datapoint with value
        None and not marked missing is hidden from the model but not returned.

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
            ValueError: If no time series are given, or a label is repeated.

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
        return await self._forecast_or_impute("/impute", time_series, ImputeResultList)

    async def _forecast_or_impute(
        self, path: str, time_series: Sequence[ForecastInputTimeSeries], list_cls: type[T_ResultList]
    ) -> T_ResultList:
        self._warning.warn()
        series = list(time_series)
        if not series:
            raise ValueError("Pass at least one time series.")
        if duplicates := find_duplicates(s.label for s in series):
            raise ValueError(f"Labels must be unique within a call. Duplicated labels: {duplicates}.")

        response = await self._post(
            self._RESOURCE_PATH + path,
            json={"items": [s.dump(camel_case=True) for s in series]},
            headers=self._beta_version_header(),
            semaphore=self._get_semaphore("write"),
        )
        return list_cls._load_raw_api_response([response.json()])
