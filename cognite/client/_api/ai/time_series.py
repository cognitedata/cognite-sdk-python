from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client._api.ai.time_series_data import AITimeSeriesDataAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes.ai import (
    ForecastResultList,
    ImputeResultList,
    InputDatapoint,
    InputTimeSeries,
)
from cognite.client.utils._auxiliary import find_duplicates
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._forecasting import split_into_requests
from cognite.client.utils._importing import local_import

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client import AsyncCogniteClient, ClientConfig

T_ResultList = TypeVar("T_ResultList", ForecastResultList, ImputeResultList)


class AITimeSeriesAPI(APIClient):
    _RESOURCE_PATH = "/ai/timeseries"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Time series forecasting and imputation"
        )
        self.data = AITimeSeriesDataAPI(config, api_version, cognite_client)

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

    async def forecast_dataframe(self, df: pd.DataFrame, cohort: str | Mapping[str, str] | None = None) -> pd.DataFrame:
        """Forecast every column of a DataFrame 512 steps ahead.

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
        res = await self.forecast(_dataframe_to_input(df, cohort))
        return res.to_pandas()

    async def impute_dataframe(self, df: pd.DataFrame, cohort: str | Mapping[str, str] | None = None) -> pd.DataFrame:
        """Reconstruct the NaN values in every column of a DataFrame.

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
        res = await self.impute(_dataframe_to_input(df, cohort))
        return res.to_pandas()

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


def _dataframe_to_input(df: pd.DataFrame, cohort: str | Mapping[str, str] | None) -> list[InputTimeSeries]:
    np = local_import("numpy")
    if df.columns.has_duplicates:
        raise ValueError(f"DataFrame columns must be unique. Duplicated cols: {find_duplicates(df.columns)}.")
    if np.isinf(df.select_dtypes(include="number")).any(axis=None):
        raise ValueError("DataFrame contains one or more (+/-) Infinity.")

    idx = df.index.to_numpy("datetime64[ms]").astype(np.int64).tolist()
    series = []
    for column, col in df.items():
        datapoints = [
            InputDatapoint(ts, None, missing=True) if isna else InputDatapoint(ts, float(value))
            for ts, value, isna in zip(idx, col.tolist(), col.isna().tolist())
        ]
        column_cohort = cohort if cohort is None or isinstance(cohort, str) else cohort.get(str(column))
        series.append(InputTimeSeries(str(column), datapoints, column_cohort))
    return series
