from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload

from cognite.client._api.ai.utils import (
    group_series_by_cohort,
    pack_series_requests,
    request_body,
    validate_series_groups,
)
from cognite.client._api_client import APIClient
from cognite.client.data_classes.ai import (
    ForecastInputDatapoint,
    ForecastInputTimeSeries,
    ForecastResult,
    ForecastResultList,
    ImputeResult,
    ImputeResultList,
)
from cognite.client.utils._auxiliary import find_duplicates
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
from cognite.client.utils._experimental import FeaturePreviewWarning
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

    def _get_semaphore(self, operation: Literal["read", "write", "delete"]) -> asyncio.BoundedSemaphore:
        from cognite.client import global_config

        return global_config.concurrency_settings.ai_time_series._semaphore_factory(
            operation, project=self._cognite_client.config.project
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

    async def forecast_dataframe(
        self,
        df: pd.DataFrame,
        cohort: str | Mapping[str, str] | None = None,
        missing: pd.DataFrame | None = None,
        include_all_quantiles: bool = False,
    ) -> pd.DataFrame:
        """Forecast the next 512 points of every column of a DataFrame.

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
        res = await self.forecast(_dataframe_to_input(df, cohort, missing))
        return res.to_pandas(include_all_quantiles=include_all_quantiles)

    async def impute_dataframe(
        self,
        df: pd.DataFrame,
        cohort: str | Mapping[str, str] | None = None,
        missing: pd.DataFrame | None = None,
        include_all_quantiles: bool = False,
    ) -> pd.DataFrame:
        """Predict the datapoints of a DataFrame that are marked missing.

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
        res = await self.impute(_dataframe_to_input(df, cohort, missing))
        return res.to_pandas(include_all_quantiles=include_all_quantiles)

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


def _dataframe_to_input(
    df: pd.DataFrame, cohort: str | Mapping[str, str] | None, missing: pd.DataFrame | None = None
) -> list[ForecastInputTimeSeries]:
    np, pd = local_import("numpy", "pandas")
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(f"DataFrame index must be `pd.DatetimeIndex`, got: {type(df.index)}")
    if df.columns.has_duplicates:
        raise ValueError(f"DataFrame columns must be unique. Duplicated cols: {find_duplicates(df.columns)}.")
    if non_string := [col for col in df.columns if not isinstance(col, str)]:
        raise ValueError(f"DataFrame column names must be strings, the labels of the time series. Not: {non_string}.")
    if non_numeric := [
        col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_bool_dtype(df[col])
    ]:
        raise ValueError(f"DataFrame columns must be numeric. Non-numeric cols: {non_numeric}.")
    if np.isinf(df.select_dtypes(include="number")).any(axis=None):
        raise ValueError("DataFrame contains one or more (+/-) Infinity.")
    if isinstance(cohort, Mapping) and (unknown := [col for col in cohort if col not in df.columns]):
        raise ValueError(f"The cohort mapping names columns that aren't in the DataFrame: {unknown}.")
    if missing is None:
        missing = pd.DataFrame(False, index=df.index, columns=df.columns)
    elif not (
        missing.index.equals(df.index)
        and missing.columns.equals(df.columns)
        and all(pd.api.types.is_bool_dtype(missing[col]) for col in missing.columns)
    ):
        raise ValueError("missing must be booleans with the same index and columns as the DataFrame.")

    idx = df.index.to_numpy("datetime64[ms]").astype(np.int64).tolist()
    series = []
    for column, col in df.items():
        values = [None if isna else float(value) for value, isna in zip(col.tolist(), col.isna().tolist())]
        datapoints = list(map(ForecastInputDatapoint, idx, values, missing[column].tolist()))
        column_cohort = cohort.get(column) if isinstance(cohort, Mapping) else cohort
        series.append(ForecastInputTimeSeries(column, datapoints, column_cohort))
    return series
