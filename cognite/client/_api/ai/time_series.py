from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client._api_client import APIClient
from cognite.client.data_classes.ai import (
    ForecastResultList,
    ImputeResultList,
    InputTimeSeries,
)
from cognite.client.utils._auxiliary import find_duplicates
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._forecasting import split_into_requests

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

    async def forecast(self, time_series: InputTimeSeries | Sequence[InputTimeSeries]) -> ForecastResultList:
        """Forecast one or more series 512 steps ahead, with quantile estimates for every step.

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
        return await self._call("/forecast", time_series, ForecastResultList)

    async def impute(self, time_series: InputTimeSeries | Sequence[InputTimeSeries]) -> ImputeResultList:
        """Reconstruct the datapoints marked missing, with quantile estimates.

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
        return await self._call("/impute", time_series, ImputeResultList)

    async def _call(
        self,
        path: str,
        time_series: InputTimeSeries | Sequence[InputTimeSeries],
        list_cls: type[T_ResultList],
    ) -> T_ResultList:
        self._warning.warn()
        series = [time_series] if isinstance(time_series, InputTimeSeries) else list(time_series)
        if not series:
            raise ValueError("Pass at least one series.")
        if duplicates := find_duplicates(s.label for s in series):
            raise ValueError(f"Labels must be unique within a call. Duplicated labels: {duplicates}.")

        tasks = [AsyncSDKTask(self._post_request, path, request) for request in split_into_requests(series)]
        summary = await execute_async_tasks(tasks)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task[1], task_list_element_unwrap_fn=lambda s: s.label
        )
        results = [list_cls._load_response(response) for response in summary.results]
        by_label = {item.label: item for res in results for item in res}
        return list_cls(
            [by_label[s.label] for s in series],
            quantile_levels=results[0].quantile_levels,
            cohorts=sorted({c for res in results for c in res.cohorts}),
        )

    async def _post_request(self, path: str, series: list[InputTimeSeries]) -> dict[str, Any]:
        response = await self._post(
            self._RESOURCE_PATH + path,
            json={"timeSeries": [s.dump(camel_case=True) for s in series]},
            api_subversion="beta",
            semaphore=self._get_semaphore("write"),
        )
        return response.json()
