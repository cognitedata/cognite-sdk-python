from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, TypeVar, overload

from cognite.client._api.ai.utils import (
    group_series_by_cohort,
    pack_series_requests,
    request_body,
    validate_series_groups,
)
from cognite.client._api_client import APIClient
from cognite.client.data_classes.ai import (
    ForecastInputTimeSeries,
    ForecastResult,
    ForecastResultList,
    ImputeResult,
    ImputeResultList,
)
from cognite.client.utils._auxiliary import find_duplicates
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
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

    @overload
    async def forecast(self, time_series: ForecastInputTimeSeries) -> ForecastResult: ...

    @overload
    async def forecast(self, time_series: Sequence[ForecastInputTimeSeries]) -> ForecastResultList: ...

    async def forecast(
        self, time_series: ForecastInputTimeSeries | Sequence[ForecastInputTimeSeries]
    ) -> ForecastResult | ForecastResultList:
        """Forecast the next 512 points of each time series from its history.

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
        if isinstance(time_series, ForecastInputTimeSeries):
            return (await self._forecast_or_impute("/forecast", [time_series], ForecastResultList))[0]
        return await self._forecast_or_impute("/forecast", time_series, ForecastResultList)

    @overload
    async def impute(self, time_series: ForecastInputTimeSeries) -> ImputeResult: ...

    @overload
    async def impute(self, time_series: Sequence[ForecastInputTimeSeries]) -> ImputeResultList: ...

    async def impute(
        self, time_series: ForecastInputTimeSeries | Sequence[ForecastInputTimeSeries]
    ) -> ImputeResult | ImputeResultList:
        """Predict the value of every datapoint marked missing, from the datapoints on both sides of it.

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
        if isinstance(time_series, ForecastInputTimeSeries):
            return (await self._forecast_or_impute("/impute", [time_series], ImputeResultList))[0]
        return await self._forecast_or_impute("/impute", time_series, ImputeResultList)

    async def _forecast_or_impute(
        self, path: str, time_series: Sequence[ForecastInputTimeSeries], list_cls: type[T_ResultList]
    ) -> T_ResultList:
        self._warning.warn()
        series = list(time_series)
        if not series:
            return list_cls([])
        if duplicates := find_duplicates(s.label for s in series):
            raise ValueError(f"Labels must be unique within a call. Duplicated labels: {duplicates}.")

        groups = group_series_by_cohort(series)
        validate_series_groups(series, groups)
        requests = pack_series_requests(series, groups)
        tasks = [AsyncSDKTask(self._post_chunk, path, request) for request in requests]
        # Results are only returned all together, so stop sending the others once one request fails.
        summary = await execute_async_tasks(tasks, fail_fast=True)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task[1], task_list_element_unwrap_fn=lambda s: s.label
        )
        # Like _retrieve_multiple, load the responses of all requests together, which merges their metadata.
        loaded = list_cls._load_raw_api_response(summary.results)
        by_label = {item.label: item for item in loaded}
        return list_cls([by_label[s.label] for s in series], loaded.metadata)

    async def _post_chunk(self, path: str, series: Sequence[ForecastInputTimeSeries]) -> dict[str, Any]:
        response = await self._post(
            self._RESOURCE_PATH + path,
            json=request_body(series),
            headers=self._beta_version_header(),
            semaphore=self._get_semaphore("write"),
        )
        return response.json()
