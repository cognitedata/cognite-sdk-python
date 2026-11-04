from __future__ import annotations

from collections.abc import Hashable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from cognite.client._api_client import APIClient
from cognite.client.data_classes.ai import (
    ForecastInputDatapoint,
    ForecastInputTimeSeries,
    ForecastResultList,
    ImputeResultList,
)
from cognite.client.exceptions import CogniteException
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

    async def forecast(self, time_series: Sequence[ForecastInputTimeSeries]) -> ForecastResultList:
        """Forecast the next 512 points of each time series from its history.

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
        return await self._forecast_or_impute("/forecast", time_series, ForecastResultList)

    async def impute(self, time_series: Sequence[ForecastInputTimeSeries]) -> ImputeResultList:
        """Predict the value of every datapoint marked missing, from the datapoints on both sides of it.

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
        return await self._forecast_or_impute("/impute", time_series, ImputeResultList)

    async def forecast_dataframe(
        self, df: pd.DataFrame, cohort: str | Mapping[Hashable, str] | None = None
    ) -> pd.DataFrame:
        """Forecast the next 512 points of every column of a DataFrame.

        Each column is one time series, labelled by its column name. NaN values are hidden from the model. See
        `forecast` for how the forecast is made.

        Note:
            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            df (pd.DataFrame): The history, one numeric column per time series, with an evenly spaced DatetimeIndex
                of 2 to 8192 timestamps. A timezone-naive index is taken to be in UTC.
            cohort (str | Mapping[Hashable, str] | None): A cohort for every column, or a mapping from column name to
                cohort. Columns not in the mapping are forecast independently.

        Returns:
            pd.DataFrame: The forecast, with a DatetimeIndex in the timezone of `df`, and a `(column, quantile level)`
                column for every column of `df`, which holds the median (level 0.5).

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
        res = await self.forecast(_dataframe_to_input(df, cohort))
        return _like_input(res.to_pandas(), df)

    async def impute_dataframe(
        self, df: pd.DataFrame, cohort: str | Mapping[Hashable, str] | None = None
    ) -> pd.DataFrame:
        """Predict every NaN value of a DataFrame.

        Each column is one time series, labelled by its column name. Every NaN value is marked missing, so it's
        hidden from the model and predicted. See `impute` for how the prediction is made.

        Note:
            This endpoint is in beta, and is called with the beta ``cdf-version`` header.

        Args:
            df (pd.DataFrame): The time series, one numeric column per time series, with an evenly spaced
                DatetimeIndex of 2 to 8192 timestamps. A timezone-naive index is taken to be in UTC.
            cohort (str | Mapping[Hashable, str] | None): A cohort for every column, or a mapping from column name to
                cohort. Columns not in the mapping are imputed independently.

        Returns:
            pd.DataFrame: The predictions, with one row per timestamp that has a NaN value in any column, a
                DatetimeIndex in the timezone of `df`, and a `(column, quantile level)` column for every column of
                `df`, which holds the median (level 0.5).

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
        res = await self.impute(_dataframe_to_input(df, cohort))
        return _like_input(res.to_pandas(), df)

    async def _forecast_or_impute(
        self, path: str, time_series: Sequence[ForecastInputTimeSeries], list_cls: type[T_ResultList]
    ) -> T_ResultList:
        self._warning.warn()
        series = list(time_series)
        if not series:
            raise ValueError("Pass at least one time series.")
        if duplicates := find_duplicates(s.label for s in series):
            raise ValueError(f"Labels must be unique within a call. Duplicated labels: {duplicates}.")

        requests = split_into_requests(series, self._size_as_sent)
        tasks = [AsyncSDKTask(self._post_chunk, path, request, list_cls) for request in requests]
        # Results are only returned all together, so stop sending the others once one request fails.
        summary = await execute_async_tasks(tasks, fail_fast=True)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task[1], task_list_element_unwrap_fn=lambda s: s.label
        )
        results: list[T_ResultList] = summary.results
        if len(levels := {tuple(res.quantile_levels) for res in results}) > 1:
            raise CogniteException(
                f"The requests for this call returned different quantile levels: {sorted(levels)}. This can happen "
                "while the model is being updated; retry the call."
            )
        by_label = {item.label: item for res in results for item in res}
        return list_cls(
            [by_label[s.label] for s in series],
            quantile_levels=results[0].quantile_levels,
            # Each response lists its cohorts in input order, and the requests follow the input, so this is the
            # order one request would return them in.
            cohorts=list(dict.fromkeys(c for res in results for c in res.cohorts)),
        )

    @staticmethod
    def _request_body(series: Sequence[ForecastInputTimeSeries]) -> dict[str, Any]:
        return {"items": [s.dump(camel_case=True) for s in series]}

    def _size_as_sent(self, series: Sequence[ForecastInputTimeSeries]) -> int:
        # Serialized like _post does it, so this is the size of the body it sends: gzip'd unless that's disabled.
        content = self._handle_json_dump(self._request_body(series), {}) or b""
        return len(content.encode() if isinstance(content, str) else content)

    async def _post_chunk(
        self, path: str, series: Sequence[ForecastInputTimeSeries], list_cls: type[T_ResultList]
    ) -> T_ResultList:
        response = await self._post(
            self._RESOURCE_PATH + path,
            json=self._request_body(series),
            headers=self._beta_version_header(),
            semaphore=self._get_semaphore("write"),
        )
        return list_cls._load_response(response.json())


def _dataframe_to_input(df: pd.DataFrame, cohort: str | Mapping[Hashable, str] | None) -> list[ForecastInputTimeSeries]:
    np, pd = local_import("numpy", "pandas")
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(f"DataFrame index must be a DatetimeIndex, got {type(df.index).__name__}.")
    if df.columns.has_duplicates:
        raise ValueError(f"DataFrame columns must be unique. Duplicated cols: {find_duplicates(df.columns)}.")
    if non_numeric := [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]:
        raise ValueError(f"DataFrame columns must be numeric. Non-numeric cols: {non_numeric}.")
    if np.isinf(df.to_numpy(dtype=float)).any():
        raise ValueError("DataFrame contains one or more (+/-) Infinity.")
    if isinstance(cohort, Mapping) and (unknown := [col for col in cohort if col not in df.columns]):
        raise ValueError(f"The cohort mapping names columns that aren't in the DataFrame: {unknown}.")

    timestamps = df.index.to_numpy("datetime64[ms]").astype(np.int64).tolist()
    series = []
    for column, values in df.items():
        datapoints = [
            ForecastInputDatapoint(ts, None, missing=True) if isna else ForecastInputDatapoint(ts, float(value))
            for ts, value, isna in zip(timestamps, values.tolist(), values.isna().tolist())
        ]
        column_cohort = cohort.get(column) if isinstance(cohort, Mapping) else cohort
        series.append(ForecastInputTimeSeries(str(column), datapoints, column_cohort))
    return series


def _like_input(out: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    # Labels are the column names as strings, and results are in UTC: give them back the input's names and timezone.
    out = out.rename(columns={str(col): col for col in df.columns}, level=0)
    if df.index.tz is not None:
        out.index = out.index.tz_localize("UTC").tz_convert(df.index.tz)
    return out
