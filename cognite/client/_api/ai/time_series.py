from __future__ import annotations

import datetime
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypeVar, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Datapoints
from cognite.client.data_classes.ai import (
    ForecastResultList,
    ImputeResultList,
    InputDatapoint,
    InputTimeSeries,
    TimeSeriesForecast,
    TimeSeriesForecastList,
    TimeSeriesImpute,
    TimeSeriesImputeList,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.datapoint_aggregates import Aggregate
from cognite.client.utils._auxiliary import find_duplicates
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._forecasting import AlignmentProblem, SourceSeries, build_inputs, split_into_requests
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import to_snake_case
from cognite.client.utils._time import granularity_to_ms, timestamp_to_ms

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

    @overload
    async def forecast_with_retrieval(
        self,
        *,
        instance_id: NodeId,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast: ...

    @overload
    async def forecast_with_retrieval(
        self,
        *,
        instance_id: Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecastList: ...

    async def forecast_with_retrieval(
        self,
        *,
        instance_id: NodeId | Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
    ) -> TimeSeriesForecast | TimeSeriesForecastList:
        """Retrieve the history of time series in `[start, end)` and forecast 512 steps past their last datapoint.

        The forecasting model needs evenly spaced history, and this method never resamples or truncates the data.
        With `granularity` and `aggregate`, the history is aligned using CDF aggregates, and empty buckets are sent as
        missing. Without them, raw datapoints are used, and they must already be evenly spaced; gaps are allowed.

        Args:
            instance_id (NodeId | Sequence[NodeId]): Instance id(s) of the time series.
            start (int | str | datetime.datetime): Start of the history window, for example "30d-ago".
            end (int | str | datetime.datetime | None): End of the history window. Defaults to now.
            granularity (str | None): Align the history using aggregates at this granularity, for example "1h".
                Requires `aggregate`.
            aggregate (Aggregate | str | None): The aggregate to forecast, for example "average" or "interpolation".
            cohort (str | None): Forecast all the requested time series jointly, aligned on one common grid. Omit it
                to forecast each time series independently.

        Returns:
            TimeSeriesForecast | TimeSeriesForecastList: A single forecast if a single instance id was passed,
            otherwise a list you can look up with `.get(instance_id=...)`.

        Examples:

            Forecast a time series from 30 days of hourly averages:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> fc = client.ai.time_series.forecast_with_retrieval(
                ...     instance_id=NodeId("my_space", "21-PT-1019"),
                ...     start="30d-ago",
                ...     granularity="1h",
                ...     aggregate="average",
                ... )

            Forecast the discharge pressures of three compressors on one compression train jointly:

                >>> ids = [NodeId("my_space", xid) for xid in ("23-PT-1101", "23-PT-1201", "23-PT-1301")]
                >>> res = client.ai.time_series.forecast_with_retrieval(
                ...     instance_id=ids,
                ...     start="14d-ago",
                ...     granularity="10m",
                ...     aggregate="average",
                ...     cohort="compression-train-a",
                ... )
                >>> fc = res.get(instance_id=ids[1])
        """
        dps_list, is_single = await self._retrieve_history(instance_id, start, end, granularity, aggregate)
        inputs = build_inputs(
            _to_source_series(dps_list, aggregate),
            step_ms=None if granularity is None else granularity_to_ms(granularity),
            cohort=cohort,
            hints=_ALIGNMENT_HINTS,
        )
        results = await self.forecast(inputs)
        forecasts = [
            TimeSeriesForecast(res.forecast, _instance_id(dps), res.cohort) for dps, res in zip(dps_list, results)
        ]
        return forecasts[0] if is_single else TimeSeriesForecastList(forecasts, results.quantile_levels)

    @overload
    async def impute_with_retrieval(
        self,
        *,
        instance_id: NodeId,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
        fill_gaps: bool = False,
        mask: Sequence[
            int | str | datetime.datetime | tuple[int | str | datetime.datetime, int | str | datetime.datetime]
        ]
        | None = None,
    ) -> TimeSeriesImpute: ...

    @overload
    async def impute_with_retrieval(
        self,
        *,
        instance_id: Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
        fill_gaps: bool = False,
        mask: Sequence[
            int | str | datetime.datetime | tuple[int | str | datetime.datetime, int | str | datetime.datetime]
        ]
        | None = None,
    ) -> TimeSeriesImputeList: ...

    async def impute_with_retrieval(
        self,
        *,
        instance_id: NodeId | Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None = None,
        granularity: str | None = None,
        aggregate: Aggregate | str | None = None,
        cohort: str | None = None,
        fill_gaps: bool = False,
        mask: Sequence[
            int | str | datetime.datetime | tuple[int | str | datetime.datetime, int | str | datetime.datetime]
        ]
        | None = None,
    ) -> TimeSeriesImpute | TimeSeriesImputeList:
        """Retrieve the history of time series in `[start, end)` and reconstruct gaps and chosen datapoints.

        You must say what to reconstruct: `fill_gaps=True`, `mask=[...]`, or both. The history is aligned the same way
        as in `forecast_with_retrieval`, and is never resampled or truncated.

        Args:
            instance_id (NodeId | Sequence[NodeId]): Instance id(s) of the time series.
            start (int | str | datetime.datetime): Start of the history window, for example "7d-ago".
            end (int | str | datetime.datetime | None): End of the history window. Defaults to now.
            granularity (str | None): Align the history using aggregates at this granularity, for example "10m".
                Requires `aggregate`.
            aggregate (Aggregate | str | None): The aggregate to reconstruct, for example "average".
            cohort (str | None): Reconstruct all the requested time series jointly, aligned on one common grid.
            fill_gaps (bool): Reconstruct every grid point without a datapoint (with aggregates, every empty bucket).
            mask (Sequence[int | str | datetime.datetime | tuple[int | str | datetime.datetime, int | str | datetime.datetime]] | None): Timestamps, or inclusive `(start, end)` ranges, to hide from the model
                and reconstruct even though values exist.

        Returns:
            TimeSeriesImpute | TimeSeriesImputeList: A single result if a single instance id was passed, otherwise a
            list you can look up with `.get(instance_id=...)`.

        Examples:

            Fill every empty 10-minute bucket in the last week:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.ai.time_series.impute_with_retrieval(
                ...     instance_id=NodeId("my_space", "21-PT-1019"),
                ...     start="7d-ago",
                ...     granularity="10m",
                ...     aggregate="average",
                ...     fill_gaps=True,
                ... )

            Reconstruct a period you know is bad, for example during a sensor fault:

                >>> from datetime import datetime, timezone
                >>> res = client.ai.time_series.impute_with_retrieval(
                ...     instance_id=NodeId("my_space", "21-PT-1019"),
                ...     start="7d-ago",
                ...     granularity="10m",
                ...     aggregate="average",
                ...     mask=[
                ...         (
                ...             datetime(2026, 9, 30, 8, tzinfo=timezone.utc),
                ...             datetime(2026, 9, 30, 12, tzinfo=timezone.utc),
                ...         )
                ...     ],
                ... )
        """
        if not fill_gaps and not mask:
            raise ValueError("Say what to reconstruct: pass `fill_gaps=True`, `mask=[...]`, or both.")
        dps_list, is_single = await self._retrieve_history(instance_id, start, end, granularity, aggregate)
        inputs = build_inputs(
            _to_source_series(dps_list, aggregate),
            step_ms=None if granularity is None else granularity_to_ms(granularity),
            cohort=cohort,
            mask=[_mask_range(entry) for entry in mask or []],
            fill_gaps=fill_gaps,
            hints=_ALIGNMENT_HINTS,
        )
        results = await self.impute(inputs)
        imputed = [TimeSeriesImpute(res.imputed, _instance_id(dps), res.cohort) for dps, res in zip(dps_list, results)]
        return imputed[0] if is_single else TimeSeriesImputeList(imputed, results.quantile_levels)

    async def _retrieve_history(
        self,
        instance_id: NodeId | Sequence[NodeId],
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime | None,
        granularity: str | None,
        aggregate: Aggregate | str | None,
    ) -> tuple[list[Datapoints], bool]:
        if (granularity is None) != (aggregate is None):
            raise ValueError("Pass `granularity` and `aggregate` together, or neither to use raw datapoints.")
        res = await self._cognite_client.time_series.data.retrieve(
            instance_id=instance_id, start=start, end=end, aggregates=aggregate, granularity=granularity
        )
        if isinstance(res, Datapoints):
            return [res], True
        if not res:
            raise ValueError("Pass at least one `instance_id`.")
        return list(res), False

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


_ALIGNMENT_HINTS: dict[AlignmentProblem, str] = {
    "irregular": "Pass `granularity=` and `aggregate=` to align it with CDF aggregates, "
    "e.g. `granularity='1h', aggregate='average'`.",
    "mixed_spacing": "Pass `granularity=` and `aggregate=` so all time series share one grid.",
    "off_grid": "Pass `granularity=` and `aggregate=` so all time series share one grid.",
    "too_long": "Narrow `start`/`end` or use a coarser `granularity`.",
    "too_short": "Widen `start`/`end`.",
}


def _to_source_series(dps_list: Sequence[Datapoints], aggregate: Aggregate | str | None) -> list[SourceSeries]:
    attribute = "value" if aggregate is None else to_snake_case(aggregate)
    sources = []
    for dps in dps_list:
        node = _instance_id(dps)
        name = f"{node.space}:{node.external_id}"
        if dps.is_string:
            raise ValueError(f"Time series '{name}' is a string time series; only numeric time series can be forecast.")
        sources.append(SourceSeries(name, dps.timestamp, getattr(dps, attribute) or []))
    return sources


def _instance_id(dps: Datapoints) -> NodeId:
    if dps.instance_id is None:
        raise ValueError(f"Datapoints for time series with id {dps.id} have no instance id")
    return dps.instance_id


def _mask_range(
    entry: int | str | datetime.datetime | tuple[int | str | datetime.datetime, int | str | datetime.datetime],
) -> tuple[int, int]:
    start, end = entry if isinstance(entry, tuple) else (entry, entry)
    return timestamp_to_ms(start), timestamp_to_ms(end)
