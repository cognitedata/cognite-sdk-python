import copy
import math
import re as regexp
import threading
from collections import defaultdict
from datetime import datetime
from typing import *

import cognite.client.utils._time
from cognite.client import utils
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Datapoints, DatapointsList, DatapointsQuery
from cognite.client.exceptions import CogniteAPIError


class DatapointsAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/data"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._DPS_LIMIT_AGG = 10000
        self._DPS_LIMIT = 100000
        self._POST_DPS_OBJECTS_LIMIT = 10000
        self._RETRIEVE_LATEST_LIMIT = 100
        self.synthetic = SyntheticDatapointsAPI(
            self._config, api_version=self._api_version, cognite_client=self._cognite_client
        )

    def retrieve(
        self,
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        id: Union[int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]] = None,
        external_id: Union[
            str, List[str], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]
        ] = None,
        aggregates: List[str] = None,
        granularity: str = None,
        include_outside_points: bool = None,
        limit: int = None,
        ignore_unknown_ids: bool = False,
    ) -> Union[None, Datapoints, DatapointsList]:
        """`Get datapoints for one or more time series. <https://docs.cognite.com/api/v1/#operation/getMultiTimeSeriesDatapoints>`_

        Note that you cannot specify the same ids/external_ids multiple times.

        Args:
            start (Union[int, str, datetime]): Inclusive start.
            end (Union[int, str, datetime]): Exclusive end.
            id (Union[int, List[int], Dict[str, Any], List[Dict[str, Any]]]): Id or list of ids. Can also be object
                specifying aggregates. See example below.
            external_id (Union[str, List[str], Dict[str, Any], List[Dict[str, Any]]]): External id or list of external
                ids. Can also be object specifying aggregates. See example below.
            aggregates (List[str]): List of aggregate functions to apply.
            granularity (str): The granularity to fetch aggregates at. e.g. '1s', '2h', '10d'.
            include_outside_points (bool): Whether or not to include outside points.
            limit (int): Maximum number of datapoints to return for each time series.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            Union[None, Datapoints, DatapointsList]: A Datapoints object containing the requested data, or a list of such objects. If `ignore_unknown_id` is True, single id is requested and it is not found, the function will return `None`.

        Examples:

            You can get specify the ids of the datapoints you wish to retrieve in a number of ways. In this example
            we are using the time-ago format to get raw data for the time series with id 1::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.retrieve(id=1, start="2w-ago", end="now")

            We can also get aggregated values, such as average. Here we are getting daily averages for all of 2018 for
            two different time series. Note that we are fetching them using their external ids::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.retrieve(external_id=["abc", "def"],
                ...                         start=datetime(2018,1,1),
                ...                         end=datetime(2019,1,1),
                ...                         aggregates=["average"],
                ...                         granularity="1d")

            If you want different aggregates for different time series specify your ids like this::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.retrieve(id=[{"id": 1, "aggregates": ["average"]},
                ...                             {"id": 1, "aggregates": ["min"]}],
                ...                         external_id={"externalId": "1", "aggregates": ["max"]},
                ...                         start="1d-ago", end="now", granularity="1h")
        """
        fetcher = DatapointsFetcher(client=self)

        _, is_single_id = fetcher._process_ts_identifiers(id, external_id)

        query = DatapointsQuery(
            start=start,
            end=end,
            id=id,
            external_id=external_id,
            aggregates=aggregates,
            granularity=granularity,
            include_outside_points=include_outside_points,
            limit=limit,
            ignore_unknown_ids=ignore_unknown_ids,
        )
        dps_list = fetcher.fetch(query)
        if is_single_id:
            if len(dps_list) == 0 and ignore_unknown_ids is True:
                return None
            return dps_list[0]
        return dps_list

    def retrieve_latest(
        self,
        id: Union[int, List[int]] = None,
        external_id: Union[str, List[str]] = None,
        before: Union[int, str, datetime] = None,
        ignore_unknown_ids: bool = False,
    ) -> Union[Datapoints, DatapointsList]:
        """`Get the latest datapoint for one or more time series <https://docs.cognite.com/api/v1/#operation/getLatest>`_

        Args:
            id (Union[int, List[int]]: Id or list of ids.
            external_id (Union[str, List[str]): External id or list of external ids.
            before: Union[int, str, datetime]: Get latest datapoint before this time.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            Union[Datapoints, DatapointsList]: A Datapoints object containing the requested data, or a list of such objects.

        Examples:

            Getting the latest datapoint in a time series. This method returns a Datapoints object, so the datapoint will
            be the first element::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datapoints.retrieve_latest(id=1)[0]

            You can also get the first datapoint before a specific time::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datapoints.retrieve_latest(id=1, before="2d-ago")[0]

            If you need the latest datapoint for multiple time series simply give a list of ids. Note that we are
            using external ids here, but either will work::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datapoints.retrieve_latest(external_id=["abc", "def"])
                >>> latest_abc = res[0][0]
                >>> latest_def = res[1][0]
        """
        before = cognite.client.utils._time.timestamp_to_ms(before) if before else None
        all_ids = self._process_ids(id, external_id, wrap_ids=True)
        is_single_id = self._is_single_identifier(id, external_id)
        if before:
            for id in all_ids:
                id.update({"before": before})

        tasks = [
            {
                "url_path": self._RESOURCE_PATH + "/latest",
                "json": {"items": chunk, "ignoreUnknownIds": ignore_unknown_ids},
            }
            for chunk in utils._auxiliary.split_into_chunks(all_ids, self._RETRIEVE_LATEST_LIMIT)
        ]
        tasks_summary = utils._concurrency.execute_tasks_concurrently(
            self._post, tasks, max_workers=self._config.max_workers
        )
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]
        res = tasks_summary.joined_results(lambda res: res.json()["items"])
        if is_single_id:
            return Datapoints._load(res[0], cognite_client=self._cognite_client)
        return DatapointsList._load(res, cognite_client=self._cognite_client)

    def query(
        self, query: Union[DatapointsQuery, List[DatapointsQuery]]
    ) -> Union[DatapointsList, List[DatapointsList]]:
        """Get datapoints for one or more time series

        This method is different from get() in that you can specify different start times, end times, and granularities
        for each requested time series.

        Args:
            query (Union[DatapointsQuery, List[DatapointsQuery]): List of datapoint queries.

        Returns:
            Union[DatapointsList, List[DatapointsList]]: The requested DatapointsList(s).

        Examples:

            This method is useful if you want to get multiple time series, but you want to specify different starts,
            ends, or granularities for each. e.g.::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> c = CogniteClient()
                >>> queries = [DatapointsQuery(id=1, start="2d-ago", end="now"),
                ...             DatapointsQuery(external_id="abc",
                ...                             start="10d-ago",
                ...                             end="now",
                ...                             aggregates=["average"],
                ...                             granularity="1m")]
                >>> res = c.datapoints.query(queries)
        """
        fetcher = DatapointsFetcher(self)
        if isinstance(query, DatapointsQuery):
            return fetcher.fetch(query)
        return fetcher.fetch_multiple(query)

    def insert(
        self,
        datapoints: Union[
            List[Dict[Union[int, float, datetime], Union[int, float, str]]],
            List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
        ],
        id: int = None,
        external_id: str = None,
    ) -> None:
        """Insert datapoints into a time series

        Timestamps can be represented as milliseconds since epoch or datetime objects.

        Args:
            datapoints(Union[List[Dict], List[Tuple],Datapoints]): The datapoints you wish to insert. Can either be a list of tuples,
                a list of dictionaries, or a Datapoints object. See examples below.
            id (int): Id of time series to insert datapoints into.
            external_id (str): External id of time series to insert datapoint into.

        Returns:
            None

        Examples:

            Your datapoints can be a list of tuples where the first element is the timestamp and the second element is
            the value::


                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> # with datetime objects
                >>> datapoints = [(datetime(2018,1,1), 1000), (datetime(2018,1,2), 2000)]
                >>> c.datapoints.insert(datapoints, id=1)
                >>> # with ms since epoch
                >>> datapoints = [(150000000000, 1000), (160000000000, 2000)]
                >>> c.datapoints.insert(datapoints, id=2)

            Or they can be a list of dictionaries::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> # with datetime objects
                >>> datapoints = [{"timestamp": datetime(2018,1,1), "value": 1000},
                ...    {"timestamp": datetime(2018,1,2), "value": 2000}]
                >>> c.datapoints.insert(datapoints, external_id="abc")
                >>> # with ms since epoch
                >>> datapoints = [{"timestamp": 150000000000, "value": 1000},
                ...    {"timestamp": 160000000000, "value": 2000}]
                >>> c.datapoints.insert(datapoints, external_id="def")

            Or they can be a Datapoints object::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> data = c.datapoints.retrieve(external_id="abc",start=datetime(2018,1,1),end=datetime(2018,2,2))
                >>> c.datapoints.insert(data, external_id="def")
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        post_dps_object = self._process_ids(id, external_id, wrap_ids=True)[0]
        if isinstance(datapoints, Datapoints):
            datapoints = [(t, v) for t, v in zip(datapoints.timestamp, datapoints.value)]
        post_dps_object.update({"datapoints": datapoints})
        dps_poster = DatapointsPoster(self)
        dps_poster.insert([post_dps_object])

    def insert_multiple(self, datapoints: List[Dict[str, Union[str, int, List]]]) -> None:
        """`Insert datapoints into multiple time series <https://docs.cognite.com/api/v1/#operation/postMultiTimeSeriesDatapoints>`_

        Args:
            datapoints (List[Dict]): The datapoints you wish to insert along with the ids of the time series.
                See examples below.

        Returns:
            None

        Examples:

            Your datapoints can be a list of tuples where the first element is the timestamp and the second element is
            the value::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()

                >>> datapoints = []
                >>> # with datetime objects and id
                >>> datapoints.append({"id": 1, "datapoints": [(datetime(2018,1,1), 1000), (datetime(2018,1,2), 2000)]})
                >>> # with ms since epoch and externalId
                >>> datapoints.append({"externalId": 1, "datapoints": [(150000000000, 1000), (160000000000, 2000)]})

                >>> c.datapoints.insert_multiple(datapoints)

            Or they can be a list of dictionaries::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()

                >>> datapoints = []
                >>> # with datetime objects and external id
                >>> datapoints.append({"externalId": "1", "datapoints": [{"timestamp": datetime(2018,1,1), "value": 1000},
                ...                     {"timestamp": datetime(2018,1,2), "value": 2000}]})
                >>> # with ms since epoch and id
                >>> datapoints.append({"id": 1, "datapoints": [{"timestamp": 150000000000, "value": 1000},
                ...                     {"timestamp": 160000000000, "value": 2000}]})

                >>> c.datapoints.insert_multiple(datapoints)
        """
        dps_poster = DatapointsPoster(self)
        dps_poster.insert(datapoints)

    def delete_range(
        self, start: Union[int, str, datetime], end: Union[int, str, datetime], id: int = None, external_id: str = None
    ) -> None:
        """Delete a range of datapoints from a time series.

        Args:
            start (Union[int, str, datetime]): Inclusive start of delete range
            end (Union[int, str, datetime]): Exclusvie end of delete range
            id (int): Id of time series to delete data from
            external_id (str): External id of time series to delete data from

        Returns:
            None

        Examples:

            Deleting the last week of data from a time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.datapoints.delete_range(start="1w-ago", end="now", id=1)
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        start = utils._time.timestamp_to_ms(start)
        end = utils._time.timestamp_to_ms(end)
        assert end > start, "end must be larger than start"

        delete_dps_object = self._process_ids(id, external_id, wrap_ids=True)[0]
        delete_dps_object.update({"inclusiveBegin": start, "exclusiveEnd": end})
        self._delete_datapoints_ranges([delete_dps_object])

    def delete_ranges(self, ranges: List[Dict[str, Any]]) -> None:
        """`Delete a range of datapoints from multiple time series. <https://docs.cognite.com/api/v1/#operation/deleteDatapoints>`_

        Args:
            ranges (List[Dict[str, Any]]): The list of datapoint ids along with time range to delete. See examples below.

        Returns:
            None

        Examples:

            Each element in the list ranges must be specify either id or externalId, and a range::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> ranges = [{"id": 1, "start": "2d-ago", "end": "now"},
                ...             {"externalId": "abc", "start": "2d-ago", "end": "now"}]
                >>> c.datapoints.delete_ranges(ranges)
        """
        valid_ranges = []
        for range in ranges:
            for key in range:
                if key not in ("id", "externalId", "start", "end"):
                    raise AssertionError(
                        "Invalid key '{}' in range. Must contain 'start', 'end', and 'id' or 'externalId".format(key)
                    )
            id = range.get("id")
            external_id = range.get("externalId")
            utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
            valid_range = self._process_ids(id, external_id, wrap_ids=True)[0]
            start = utils._time.timestamp_to_ms(range["start"])
            end = utils._time.timestamp_to_ms(range["end"])
            valid_range.update({"inclusiveBegin": start, "exclusiveEnd": end})
            valid_ranges.append(valid_range)
        self._delete_datapoints_ranges(valid_ranges)

    def _delete_datapoints_ranges(self, delete_range_objects):
        self._post(url_path=self._RESOURCE_PATH + "/delete", json={"items": delete_range_objects})

    def retrieve_dataframe(
        self,
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        aggregates: List[str],
        granularity: str,
        id: Union[int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]] = None,
        external_id: Union[
            str, List[str], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]
        ] = None,
        limit: int = None,
        include_aggregate_name=True,
        complete: str = None,
        ignore_unknown_ids: bool = False,
    ) -> "pandas.DataFrame":
        """Get a pandas dataframe describing the requested data.

        Note that you cannot specify the same ids/external_ids multiple times.

        Args:
            start (Union[int, str, datetime]): Inclusive start.
            end (Union[int, str, datetime]): Exclusive end.
            aggregates (List[str]): List of aggregate functions to apply.
            granularity (str): The granularity to fetch aggregates at. e.g. '1s', '2h', '10d'.
            id (Union[int, List[int], Dict[str, Any], List[Dict[str, Any]]]): Id or list of ids. Can also be object
                specifying aggregates. See example below.
            external_id (Union[str, List[str], Dict[str, Any], List[Dict[str, Any]]]): External id or list of external
                ids. Can also be object specifying aggregates. See example below.
            limit (int): Maximum number of datapoints to return for each time series.
            include_aggregate_name (bool): Include 'aggregate' in the column name. Defaults to True and should only be set to False when only a single aggregate is requested per id/external id.
            complete (str): Post-processing of the dataframe.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

                Pass 'fill' to insert missing entries into the index, and complete data where possible (supports interpolation, stepInterpolation, count, sum, totalVariation).

                Pass 'fill,dropna' to additionally drop rows in which any aggregate for any time series has missing values (typically rows at the start and end for interpolation aggregates).
                This option guarantees that all returned dataframes have the exact same shape and no missing values anywhere, and is only supported for aggregates sum, count, totalVariation, interpolation and stepInterpolation.

        Returns:
            pandas.DataFrame: The requested dataframe

        Examples:

            Get a pandas dataframe::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> df = c.datapoints.retrieve_dataframe(id=[1,2,3], start="2w-ago", end="now",
                ...         aggregates=["average","sum"], granularity="1h")

            Get a pandas dataframe with the index regularly spaced at 1 minute intervals, missing values completed and without the aggregate name in the columns::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> df = c.datapoints.retrieve_dataframe(id=[1,2,3], start="2w-ago", end="now",
                ...         aggregates=["interpolation"], granularity="1m", include_aggregate_name=False, complete="fill,dropna")
        """
        pd = utils._auxiliary.local_import("pandas")

        if id is not None:
            id_dpl = self.retrieve(
                id=id,
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                limit=limit,
                ignore_unknown_ids=ignore_unknown_ids,
            )
            if id_dpl is None:
                id_dpl = DatapointsList([])
            id_df = id_dpl.to_pandas(column_names="id")
        else:
            id_df = pd.DataFrame()
            id_dpl = DatapointsList([])

        if external_id is not None:
            external_id_dpl = self.retrieve(
                external_id=external_id,
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                limit=limit,
                ignore_unknown_ids=ignore_unknown_ids,
            )
            if external_id_dpl is None:
                external_id_dpl = DatapointsList([])
            external_id_df = external_id_dpl.to_pandas()
        else:
            external_id_df = pd.DataFrame()
            external_id_dpl = DatapointsList([])

        df = pd.concat([id_df, external_id_df], axis="columns")

        complete = [s.strip() for s in (complete or "").split(",")]
        if set(complete) - {"fill", "dropna", ""}:
            raise ValueError("complete should be 'fill', 'fill,dropna' or Falsy")

        if "fill" in complete and df.shape[0] > 1:
            ag_used_by_id = {
                dp.id: [attr for attr, _ in dp._get_non_empty_data_fields(get_empty_lists=True)]
                for dpl in [id_dpl, external_id_dpl]
                for dp in (dpl.data if isinstance(dpl, DatapointsList) else [dpl])
            }
            is_step_dict = {
                str(field): bool(dp.is_step)
                for dpl in [id_dpl, external_id_dpl]
                for dp in (dpl.data if isinstance(dpl, DatapointsList) else [dpl])
                for field in [dp.id, dp.external_id]
                if field
            }
            df = self._dataframe_fill(df, granularity, is_step_dict)

            if "dropna" in complete:
                self._dataframe_safe_dropna(df, set([ag for id, ags in ag_used_by_id.items() for ag in ags]))

        if not include_aggregate_name:
            Datapoints._strip_aggregate_names(df)

        return df

    def _dataframe_fill(self, df, granularity, is_step_dict):
        np, pd = utils._auxiliary.local_import("numpy", "pandas")
        df = df.reindex(
            np.arange(
                df.index[0],
                df.index[-1] + pd.Timedelta(microseconds=1),
                pd.Timedelta(microseconds=cognite.client.utils._time.granularity_to_ms(granularity) * 1000),
            ),
            copy=False,
        )
        df.fillna({c: 0 for c in df.columns if regexp.search(c, r"\|(sum|totalVariation|count)$")}, inplace=True)
        int_cols = [c for c in df.columns if regexp.search(c, r"\|interpolation$")]
        lin_int_cols = [c for c in int_cols if not is_step_dict[regexp.match(r"(.*)\|\w+$", c).group(1)]]
        step_int_cols = [c for c in df.columns if regexp.search(c, r"\|stepInterpolation$")] + list(
            set(int_cols) - set(lin_int_cols)
        )
        if lin_int_cols:
            df[lin_int_cols] = df[lin_int_cols].interpolate(limit_area="inside")
        df[step_int_cols] = df[step_int_cols].ffill()
        return df

    def _dataframe_safe_dropna(self, df, aggregates_used):
        supported_aggregates = ["sum", "count", "total_variation", "interpolation", "step_interpolation"]
        not_supported = set(aggregates_used) - set(supported_aggregates + ["timestamp"])
        if not_supported:
            raise ValueError(
                "The aggregate(s) {} is not supported for dataframe completion with dropna, only {} are".format(
                    [utils._auxiliary.to_camel_case(a) for a in not_supported],
                    [utils._auxiliary.to_camel_case(a) for a in supported_aggregates],
                )
            )
        df.dropna(inplace=True)

    def retrieve_dataframe_dict(
        self,
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        aggregates: List[str],
        granularity: str,
        id: Union[int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]] = None,
        external_id: Union[
            str, List[str], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]
        ] = None,
        limit: int = None,
        ignore_unknown_ids: bool = False,
        complete: bool = None,
    ) -> Dict[str, "pandas.DataFrame"]:
        """Get a dictionary of aggregate: pandas dataframe describing the requested data.

        Args:
            start (Union[int, str, datetime]): Inclusive start.
            end (Union[int, str, datetime]): Exclusive end.
            aggregates (List[str]): List of aggregate functions to apply.
            granularity (str): The granularity to fetch aggregates at. e.g. '1s', '2h', '10d'.
            id (Union[int, List[int], Dict[str, Any], List[Dict[str, Any]]]: Id or list of ids. Can also be object specifying aggregates.
            external_id (Union[str, List[str], Dict[str, Any], List[Dict[str, Any]]]): External id or list of external ids. Can also be object specifying aggregates.
            limit (int): Maximum number of datapoints to return for each time series.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.
            complete (str): Post-processing of the dataframe.

                Pass 'fill' to insert missing entries into the index, and complete data where possible (supports interpolation, stepInterpolation, count, sum, totalVariation).

                Pass 'fill,dropna' to additionally drop rows in which any aggregate for any time series has missing values (typically rows at the start and end for interpolation aggregates).
                This option guarantees that all returned dataframes have the exact same shape and no missing values anywhere, and is only supported for aggregates sum, count, totalVariation, interpolation and stepInterpolation.

        Returns:
           Dict[str,pandas.DataFrame]: A dictionary of aggregate: dataframe.

        Examples:

            Get a dictionary of pandas dataframes, with the index evenly spaced at 1h intervals, missing values completed in the middle and incomplete entries dropped at the start and end::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dfs = c.datapoints.retrieve_dataframe_dict(id=[1,2,3], start="2w-ago", end="now",
                ...          aggregates=["interpolation","count"], granularity="1h", complete="fill,dropna")
        """
        all_aggregates = aggregates
        for queries in [id, external_id]:
            if isinstance(queries, list) and queries and isinstance(queries[0], dict):
                for it in queries:
                    for ag in it.get("aggregates", []):
                        if ag not in all_aggregates:
                            all_aggregates.append(ag)

        df = self.retrieve_dataframe(
            start,
            end,
            aggregates,
            granularity,
            id,
            external_id,
            limit,
            include_aggregate_name=True,
            complete=complete,
            ignore_unknown_ids=ignore_unknown_ids,
        )
        return {ag: df.filter(like="|" + ag).rename(columns=lambda s: s[: -len(ag) - 1]) for ag in all_aggregates}

    def insert_dataframe(self, dataframe, external_id_headers: bool = False, dropna: bool = False):
        """Insert a dataframe.

        The index of the dataframe must contain the timestamps. The names of the remaining columns specify the ids or external ids of
        the time series to which column contents will be written.

        Said time series must already exist.

        Args:
            dataframe (pandas.DataFrame):  Pandas DataFrame Object containing the time series.
            external_id_headers (bool): Set to True if the column headers are external ids rather than internal ids.
                Defaults to False.
            dropna (bool): Set to True to skip NaNs in the given DataFrame, applied per column.

        Returns:
            None

        Examples:
            Post a dataframe with white noise::

                >>> import numpy as np
                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>> from datetime import datetime, timedelta
                >>>
                >>> c = CogniteClient()
                >>> ts_id = 123
                >>> start = datetime(2018, 1, 1)
                >>> x = pd.DatetimeIndex([start + timedelta(days=d) for d in range(100)])
                >>> y = np.random.normal(0, 1, 100)
                >>> df = pd.DataFrame({ts_id: y}, index=x)
                >>> c.datapoints.insert_dataframe(df)
        """
        np = utils._auxiliary.local_import("numpy")
        assert not np.isinf(dataframe.select_dtypes(include=[np.number])).any(
            axis=None
        ), "Dataframe contains Infinity. Remove them in order to insert the data."
        if not dropna:
            assert not dataframe.isnull().any(
                axis=None
            ), "Dataframe contains NaNs. Remove them or pass `dropna=True` in order to insert the data."
        dps = []
        idx = dataframe.index.values.astype("datetime64[ms]").astype(np.int64)
        for column_id, col in dataframe.iteritems():
            mask = col.notna()
            datapoints = list(zip(idx[mask], col[mask]))
            if not datapoints:
                continue
            if external_id_headers:
                dps.append({"datapoints": datapoints, "externalId": column_id})
            else:
                dps.append({"datapoints": datapoints, "id": int(column_id)})
        self.insert_multiple(dps)


class DatapointsBin:
    def __init__(self, dps_objects_limit: int, dps_limit: int):
        self.dps_objects_limit = dps_objects_limit
        self.dps_limit = dps_limit
        self.current_num_datapoints = 0
        self.dps_object_list = []

    def add(self, dps_object):
        self.current_num_datapoints += len(dps_object["datapoints"])
        self.dps_object_list.append(dps_object)

    def will_fit(self, number_of_dps: int):
        will_fit_dps = (self.current_num_datapoints + number_of_dps) <= self.dps_limit
        will_fit_dps_objects = (len(self.dps_object_list) + 1) <= self.dps_objects_limit
        return will_fit_dps and will_fit_dps_objects


class DatapointsPoster:
    def __init__(self, client: DatapointsAPI):
        self.client = client
        self.bins = []

    def insert(self, dps_object_list: List[Dict[str, Any]]):
        valid_dps_object_list = self._validate_dps_objects(dps_object_list)
        binned_dps_object_lists = self._bin_datapoints(valid_dps_object_list)
        self._insert_datapoints_concurrently(binned_dps_object_lists)

    @staticmethod
    def _validate_dps_objects(dps_object_list):
        valid_dps_objects = []
        for dps_object in dps_object_list:
            for key in dps_object:
                if key not in ("id", "externalId", "datapoints"):
                    raise ValueError(
                        "Invalid key '{}' in datapoints. Must contain 'datapoints', and 'id' or 'externalId".format(key)
                    )
            valid_dps_object = {k: dps_object[k] for k in ["id", "externalId"] if k in dps_object}
            valid_dps_object["datapoints"] = DatapointsPoster._validate_and_format_datapoints(dps_object["datapoints"])
            valid_dps_objects.append(valid_dps_object)
        return valid_dps_objects

    @staticmethod
    def _validate_and_format_datapoints(
        datapoints: Union[
            List[Dict[Union[int, float, datetime], Union[int, float, str]]],
            List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
        ],
    ) -> List[Tuple[int, Any]]:
        utils._auxiliary.assert_type(datapoints, "datapoints", [list])
        assert len(datapoints) > 0, "No datapoints provided"
        utils._auxiliary.assert_type(datapoints[0], "datapoints element", [tuple, dict])

        valid_datapoints = []
        if isinstance(datapoints[0], tuple):
            valid_datapoints = [(cognite.client.utils._time.timestamp_to_ms(t), v) for t, v in datapoints]
        elif isinstance(datapoints[0], dict):
            for dp in datapoints:
                assert "timestamp" in dp, "A datapoint is missing the 'timestamp' key"
                assert "value" in dp, "A datapoint is missing the 'value' key"
                valid_datapoints.append((cognite.client.utils._time.timestamp_to_ms(dp["timestamp"]), dp["value"]))
        return valid_datapoints

    def _bin_datapoints(self, dps_object_list: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        for dps_object in dps_object_list:
            for i in range(0, len(dps_object["datapoints"]), self.client._DPS_LIMIT):
                dps_object_chunk = {k: dps_object[k] for k in ["id", "externalId"] if k in dps_object}
                dps_object_chunk["datapoints"] = dps_object["datapoints"][i : i + self.client._DPS_LIMIT]
                for bin in self.bins:
                    if bin.will_fit(len(dps_object_chunk["datapoints"])):
                        bin.add(dps_object_chunk)
                        break
                else:
                    bin = DatapointsBin(self.client._DPS_LIMIT, self.client._POST_DPS_OBJECTS_LIMIT)
                    bin.add(dps_object_chunk)
                    self.bins.append(bin)
        binned_dps_object_list = []
        for bin in self.bins:
            binned_dps_object_list.append(bin.dps_object_list)
        return binned_dps_object_list

    def _insert_datapoints_concurrently(self, dps_object_lists: List[List[Dict[str, Any]]]):
        tasks = []
        for dps_object_list in dps_object_lists:
            tasks.append((dps_object_list,))
        summary = utils._concurrency.execute_tasks_concurrently(
            self._insert_datapoints, tasks, max_workers=self.client._config.max_workers
        )
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda x: x[0],
            task_list_element_unwrap_fn=lambda x: {k: x[k] for k in ["id", "externalId"] if k in x},
        )

    def _insert_datapoints(self, post_dps_objects: List[Dict[str, Any]]):
        # convert to memory intensive format as late as possible and clean up after
        for it in post_dps_objects:
            it["datapoints"] = [{"timestamp": t, "value": v} for t, v in it["datapoints"]]
        self.client._post(url_path=self.client._RESOURCE_PATH, json={"items": post_dps_objects})
        for it in post_dps_objects:
            del it["datapoints"]


class _DPWindow:
    def __init__(self, start, end, limit=float("inf")):
        self.start = start
        self.end = end
        self.limit = limit

    def __eq__(self, other):
        return [self.start, self.end, self.limit] == [other.start, other.end, other.limit]


class _DPTask:
    def __init__(
        self, client, start, end, ts_item, aggregates, granularity, include_outside_points, limit, ignore_unknown_ids
    ):
        self.start = cognite.client.utils._time.timestamp_to_ms(start)
        self.end = cognite.client.utils._time.timestamp_to_ms(end)
        self.aggregates = ts_item.get("aggregates") or aggregates
        self.ts_item = {k: v for k, v in ts_item.items() if k in ["id", "externalId"]}
        self.granularity = granularity
        self.include_outside_points = include_outside_points
        self.limit = limit or float("inf")
        self.ignore_unknown_ids = ignore_unknown_ids

        self.client = client
        self.request_limit = client._DPS_LIMIT_AGG if self.aggregates else client._DPS_LIMIT
        self.missing = False
        self.results = []
        self.point_before = Datapoints()
        self.point_after = Datapoints()

    def next_start_offset(self):
        return cognite.client.utils._time.granularity_to_ms(self.granularity) if self.granularity else 1

    def store_partial_result(self, raw_data, start, end):
        expected_fields = self.aggregates or ["value"]

        if self.include_outside_points and raw_data["datapoints"]:
            # assumes first query has full start/end range
            copy_data = copy.copy(raw_data)  # shallow copy
            if raw_data["datapoints"][0]["timestamp"] < start:
                if not self.point_before:
                    copy_data["datapoints"] = raw_data["datapoints"][:1]
                    self.point_before = Datapoints._load(
                        copy_data, expected_fields, cognite_client=self.client._cognite_client
                    )
                raw_data["datapoints"] = raw_data["datapoints"][1:]
            if raw_data["datapoints"] and raw_data["datapoints"][-1]["timestamp"] >= end:
                if not self.point_after:
                    copy_data["datapoints"] = raw_data["datapoints"][-1:]
                    self.point_after = Datapoints._load(
                        copy_data, expected_fields, cognite_client=self.client._cognite_client
                    )
                raw_data["datapoints"] = raw_data["datapoints"][:-1]

        self.results.append(Datapoints._load(raw_data, expected_fields, cognite_client=self.client._cognite_client))
        last_timestamp = raw_data["datapoints"] and raw_data["datapoints"][-1]["timestamp"]
        return len(raw_data["datapoints"]), last_timestamp

    def mark_missing(self):  # for ignore unknown ids
        self.missing = True
        return 0, None  # as in store partial result

    def result(self):
        def custom_sort_key(x):
            if x.timestamp:
                return x.timestamp[0]
            return 0

        dps = self.point_before
        for res in sorted(self.results, key=custom_sort_key):
            dps._extend(res)
        dps._extend(self.point_after)
        if len(dps) > self.limit:
            dps = dps[: self.limit]
        return dps

    def as_tuple(self):
        return (
            self.start,
            self.end,
            self.ts_item,
            self.aggregates,
            self.granularity,
            self.include_outside_points,
            self.limit,
        )


class DatapointsFetcher:
    def __init__(self, client: DatapointsAPI):
        self.client = client

    def fetch(self, query: DatapointsQuery) -> DatapointsList:
        return self.fetch_multiple([query])[0]

    def fetch_multiple(self, queries: List[DatapointsQuery]) -> List[DatapointsList]:
        task_lists = [self._create_tasks(q) for q in queries]
        self._fetch_datapoints(sum(task_lists, []))
        return self._get_dps_results(task_lists)

    def _create_tasks(self, query: DatapointsQuery) -> List[_DPTask]:
        ts_items, _ = self._process_ts_identifiers(query.id, query.external_id)
        tasks = [
            _DPTask(
                self.client,
                query.start,
                query.end,
                ts_item,
                query.aggregates,
                query.granularity,
                query.include_outside_points,
                query.limit,
                query.ignore_unknown_ids,
            )
            for ts_item in ts_items
        ]
        self._validate_tasks(tasks)
        self._preprocess_tasks(tasks)
        return tasks

    def _validate_tasks(self, tasks: List[_DPTask]):
        identifiers_seen = set()
        for t in tasks:
            identifier = utils._auxiliary.unwrap_identifer(t.ts_item)
            if identifier in identifiers_seen:
                raise ValueError("Time series identifier '{}' is duplicated in query".format(identifier))
            identifiers_seen.add(identifier)
            if t.aggregates is not None and t.granularity is None:
                raise ValueError("When specifying aggregates, granularity must also be provided.")
            if t.granularity is not None and not t.aggregates:
                raise ValueError("When specifying granularity, aggregates must also be provided.")

    def _preprocess_tasks(self, tasks: List[_DPTask]):
        for t in tasks:
            new_start = cognite.client.utils._time.timestamp_to_ms(t.start)
            new_end = cognite.client.utils._time.timestamp_to_ms(t.end)
            if t.aggregates:
                new_start = self._align_with_granularity_unit(new_start, t.granularity)
                new_end = self._align_with_granularity_unit(new_end, t.granularity)
            t.start = new_start
            t.end = new_end

    def _get_dps_results(self, task_lists: List[List[_DPTask]]) -> List[DatapointsList]:
        return [
            DatapointsList([t.result() for t in tl if not t.missing], cognite_client=self.client._cognite_client)
            for tl in task_lists
        ]

    def _fetch_datapoints(self, tasks: List[_DPTask]):
        tasks_summary = utils._concurrency.execute_tasks_concurrently(
            self._fetch_dps_initial_and_return_remaining_tasks,
            [(t,) for t in tasks],
            max_workers=self.client._config.max_workers,
        )
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]

        remaining_tasks_with_windows = tasks_summary.joined_results()
        if len(remaining_tasks_with_windows) > 0:
            self._fetch_datapoints_for_remaining_queries(remaining_tasks_with_windows)

    def _fetch_dps_initial_and_return_remaining_tasks(self, task: _DPTask) -> List[Tuple[_DPTask, _DPWindow]]:
        ndp_in_first_task, last_timestamp = self._get_datapoints(task, None, True)
        if ndp_in_first_task < task.request_limit:
            return []
        remaining_user_limit = task.limit - ndp_in_first_task
        task.start = last_timestamp + task.next_start_offset()
        queries = self._split_task_into_windows(task.results[0].id, task, remaining_user_limit)
        return queries

    def _fetch_datapoints_for_remaining_queries(self, tasks_with_windows: List[Tuple[_DPTask, _DPWindow]]):
        tasks_summary = utils._concurrency.execute_tasks_concurrently(
            self._get_datapoints_with_paging, tasks_with_windows, max_workers=self.client._config.max_workers
        )
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]

    @staticmethod
    def _align_with_granularity_unit(ts: int, granularity: str):
        gms = cognite.client.utils._time.granularity_unit_to_ms(granularity)
        if ts % gms == 0:
            return ts
        return ts - (ts % gms) + gms

    def _split_task_into_windows(self, id, task, remaining_user_limit):
        windows = self._get_windows(id, task, remaining_user_limit)
        return [(task, w) for w in windows]

    def _get_windows(self, id, task, remaining_user_limit):
        if task.start >= task.end:
            return []
        count_granularity = "1d"
        if task.granularity and cognite.client.utils._time.granularity_to_ms(
            "1d"
        ) < cognite.client.utils._time.granularity_to_ms(task.granularity):
            count_granularity = task.granularity
        try:
            count_task = _DPTask(
                self.client, task.start, task.end, {"id": id}, ["count"], count_granularity, False, None, False
            )
            self._get_datapoints_with_paging(count_task, _DPWindow(task.start, task.end))
            res = count_task.result()
        except CogniteAPIError:
            res = []
        if len(res) == 0:  # string based series or aggregates not yet calculated
            return [_DPWindow(task.start, task.end, remaining_user_limit)]
        counts = list(zip(res.timestamp, res.count))
        windows = []
        total_count = 0
        current_window_count = 0
        window_start = task.start
        granularity_ms = cognite.client.utils._time.granularity_to_ms(task.granularity) if task.granularity else None
        agg_count = lambda count: int(
            min(math.ceil(cognite.client.utils._time.granularity_to_ms(count_granularity) / granularity_ms), count)
        )
        for i, (ts, count) in enumerate(counts):
            if ts < task.start:  # API rounds time stamps down, so some of the first day may have been retrieved already
                count = 0

            if i < len(counts) - 1:
                next_timestamp = counts[i + 1][0]
                next_raw_count = counts[i + 1][1]
                next_count = next_raw_count if task.granularity is None else agg_count(next_raw_count)
            else:
                next_timestamp = task.end
                next_count = 0
            current_count = count if task.granularity is None else agg_count(count)
            total_count += current_count
            current_window_count += current_count
            if current_window_count + next_count > task.request_limit or i == len(counts) - 1:
                window_end = next_timestamp
                if task.granularity:
                    window_end = self._align_window_end(task.start, next_timestamp, task.granularity)
                windows.append(_DPWindow(window_start, window_end, remaining_user_limit))
                window_start = window_end
                current_window_count = 0
                if total_count >= remaining_user_limit:
                    break
        return windows

    @staticmethod
    def _align_window_end(start: int, end: int, granularity: str):
        gms = cognite.client.utils._time.granularity_to_ms(granularity)
        diff = end - start
        end -= diff % gms
        return end

    def _get_datapoints_with_paging(self, task, window):
        ndp_retrieved_total = 0
        while window.end > window.start and ndp_retrieved_total < window.limit:
            ndp_retrieved, last_time = self._get_datapoints(task, window)
            if ndp_retrieved < min(window.limit, task.request_limit):
                break
            window.limit -= ndp_retrieved
            window.start = last_time + task.next_start_offset()

    def _get_datapoints(
        self, task: _DPTask, window: _DPWindow = None, first_page: bool = False
    ) -> Tuple[int, Union[None, int]]:
        window = window or _DPWindow(task.start, task.end, task.limit)
        payload = {
            "items": [task.ts_item],
            "start": window.start,
            "end": window.end,
            "aggregates": task.aggregates,
            "granularity": task.granularity,
            "includeOutsidePoints": task.include_outside_points and first_page,
            "ignoreUnknownIds": task.ignore_unknown_ids,
            "limit": min(window.limit, task.request_limit),
        }
        res = self.client._post(self.client._RESOURCE_PATH + "/list", json=payload).json()["items"]
        if not res and task.ignore_unknown_ids:
            return task.mark_missing()
        else:
            return task.store_partial_result(res[0], window.start, window.end)

    @staticmethod
    def _process_ts_identifiers(ids, external_ids) -> Tuple[List[Dict], bool]:
        is_list = False
        items = []

        if isinstance(ids, List):
            is_list = True
            for item in ids:
                items.append(DatapointsFetcher._process_single_ts_item(item, False))
        elif ids is not None:
            items.append(DatapointsFetcher._process_single_ts_item(ids, False))

        if isinstance(external_ids, List):
            is_list = True
            for item in external_ids:
                items.append(DatapointsFetcher._process_single_ts_item(item, True))
        elif external_ids is not None:
            items.append(DatapointsFetcher._process_single_ts_item(external_ids, True))

        return items, not is_list and len(items) == 1

    @staticmethod
    def _process_single_ts_item(item, external: bool):
        item_type = "externalId" if external else "id"
        id_type = str if external else int
        if isinstance(item, id_type):
            return {item_type: item}
        elif isinstance(item, Dict):
            for key in item:
                if key not in [item_type, "aggregates"]:
                    raise ValueError("Unknown key '{}' in {} dict argument".format(key, item_type))
            if item_type not in item:
                raise ValueError(
                    "When passing a dict to the {} argument, '{}' must be specified.".format(item_type, item_type)
                )
            return item
        raise TypeError("Invalid type '{}' for argument '{}'".format(type(item), item_type))
