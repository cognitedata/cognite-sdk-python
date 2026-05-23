"""
===============================================================================
0d4c4f70863a81af2b14d434fa684352
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.datapoints import (
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    DatapointsQuery,
    LatestDatapoint,
    LatestDatapointList,
)
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    import pandas as pd


class SyncDataModelingDatapointsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def retrieve(self, query: DatapointsQuery | Sequence[DatapointsQuery]) -> Datapoints | DatapointsList | None:
        """
        `Retrieve datapoints for one or more time series. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        Each :class:`~cognite.client.data_classes.datapoints.DatapointsQuery` must be created with
        an ``instance_id`` — passing ``id`` or ``external_id`` will raise a ``ValueError``.
        All query options (start, end, aggregates, granularity, limit, …) are set on the query object.

        Args:
            query (DatapointsQuery | Sequence[DatapointsQuery]): Query or list of queries. Each must have ``instance_id`` set.

        Returns:
            Datapoints | DatapointsList | None: A ``Datapoints`` object for a single query, or ``DatapointsList`` for multiple. Returns ``None`` if the query has ``ignore_unknown_ids=True`` and the time series was not found.

        Examples:

            Retrieve raw datapoints:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> dps = client.data_modeling.time_series.data.retrieve(
                ...     DatapointsQuery(instance_id=NodeId("my-space", "my-ts"), start="2w-ago")
                ... )

            Retrieve hourly averages for multiple time series:

                >>> dps = client.data_modeling.time_series.data.retrieve(
                ...     [
                ...         DatapointsQuery(
                ...             instance_id=NodeId("my-space", "ts-1"),
                ...             aggregates="average",
                ...             granularity="1h",
                ...         ),
                ...         DatapointsQuery(
                ...             instance_id=NodeId("my-space", "ts-2"),
                ...             aggregates="average",
                ...             granularity="1h",
                ...         ),
                ...     ]
                ... )
        """
        return run_sync(self.__async_client.data_modeling.time_series.data.retrieve(query=query))

    def retrieve_arrays(
        self, query: DatapointsQuery | Sequence[DatapointsQuery]
    ) -> DatapointsArray | DatapointsArrayList | None:
        """
        `Retrieve datapoints as numpy arrays. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getMultiTimeSeriesDatapoints>`_

        Each :class:`~cognite.client.data_classes.datapoints.DatapointsQuery` must be created with
        an ``instance_id`` — passing ``id`` or ``external_id`` will raise a ``ValueError``.

        Note:
            This method requires ``numpy`` to be installed.

        Args:
            query (DatapointsQuery | Sequence[DatapointsQuery]): Query or list of queries. Each must have ``instance_id`` set.

        Returns:
            DatapointsArray | DatapointsArrayList | None: A ``DatapointsArray`` for a single query, or ``DatapointsArrayList`` for multiple.

        Examples:

            Retrieve raw datapoints as numpy arrays:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> arr = client.data_modeling.time_series.data.retrieve_arrays(
                ...     DatapointsQuery(instance_id=NodeId("my-space", "my-ts"), start="1d-ago")
                ... )
        """
        return run_sync(self.__async_client.data_modeling.time_series.data.retrieve_arrays(query=query))

    def retrieve_dataframe(self, query: DatapointsQuery | Sequence[DatapointsQuery]) -> pd.DataFrame:
        """
        Get datapoints as a pandas DataFrame.

        Each :class:`~cognite.client.data_classes.datapoints.DatapointsQuery` must be created with
        an ``instance_id`` — passing ``id`` or ``external_id`` will raise a ``ValueError``.

        Note:
            This method requires ``pandas`` to be installed.

        Args:
            query (DatapointsQuery | Sequence[DatapointsQuery]): Query or list of queries. Each must have ``instance_id`` set.

        Returns:
            pd.DataFrame: DataFrame with a DatetimeIndex and one column per (instance_id, aggregate) combination.

        Examples:

            Retrieve daily averages as a DataFrame:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> df = client.data_modeling.time_series.data.retrieve_dataframe(
                ...     DatapointsQuery(
                ...         instance_id=NodeId("my-space", "my-ts"),
                ...         aggregates="average",
                ...         granularity="1d",
                ...         start="30d-ago",
                ...     )
                ... )
        """
        return run_sync(self.__async_client.data_modeling.time_series.data.retrieve_dataframe(query=query))

    def retrieve_latest(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        *,
        before: None | int | str | datetime.datetime = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        ignore_unknown_ids: bool = False,
    ) -> LatestDatapoint | LatestDatapointList | None:
        """
        `Get the latest datapoint for one or more time series by instance ID. <https://api-docs.cognite.com/20230101/tag/Time-series/operation/getLatest>`_

        Args:
            instance_id (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Instance ID or list of instance IDs.
            before (None | int | str | datetime.datetime): Get latest datapoint before this time. Default: None ("now").
            target_unit (str | None): Unit to convert returned datapoints to. Cannot be used with target_unit_system.
            target_unit_system (str | None): Unit system to convert returned datapoints to. Cannot be used with target_unit.
            include_status (bool): Include status code for the datapoint. Default: False.
            ignore_bad_datapoints (bool): Ignore datapoints with bad status codes. Default: True.
            treat_uncertain_as_bad (bool): Treat uncertain status codes as bad. Default: True.
            ignore_unknown_ids (bool): Ignore unknown instance IDs instead of raising. Default: False.

        Returns:
            LatestDatapoint | LatestDatapointList | None: The latest datapoint, or a list if multiple IDs were given.

        Examples:

            Get the latest datapoint:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.time_series.data.retrieve_latest(
                ...     NodeId("my-space", "my-ts")
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.time_series.data.retrieve_latest(
                instance_id=instance_id,
                before=before,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                include_status=include_status,
                ignore_bad_datapoints=ignore_bad_datapoints,
                treat_uncertain_as_bad=treat_uncertain_as_bad,
                ignore_unknown_ids=ignore_unknown_ids,
            )
        )

    def insert(
        self,
        datapoints: Datapoints
        | DatapointsArray
        | Sequence[dict[str, Any]]
        | Sequence[tuple[int | float | datetime.datetime, int | float | str]],
        instance_id: NodeId | tuple[str, str],
    ) -> None:
        """
        Insert datapoints into a time series by instance ID.

        Args:
            datapoints (Datapoints | DatapointsArray | Sequence[dict[str, Any]] | Sequence[tuple[int | float | datetime.datetime, int | float | str]]): Datapoints to insert. Each element is either a ``(timestamp, value)`` tuple, a ``(timestamp, value, status_code)`` tuple, or a dict with ``"timestamp"`` and ``"value"`` keys.
            instance_id (NodeId | tuple[str, str]): Instance ID of the target time series.

        Examples:

            Insert datapoints using tuples:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> from datetime import datetime, timezone
                >>> client = CogniteClient()
                >>> datapoints = [
                ...     (datetime(2024, 1, 1, tzinfo=timezone.utc), 1.0),
                ...     (datetime(2024, 1, 2, tzinfo=timezone.utc), 2.0),
                ... ]
                >>> client.data_modeling.time_series.data.insert(datapoints, NodeId("my-space", "my-ts"))
        """
        return run_sync(
            self.__async_client.data_modeling.time_series.data.insert(datapoints=datapoints, instance_id=instance_id)
        )

    def delete_range(
        self,
        start: int | str | datetime.datetime,
        end: int | str | datetime.datetime,
        instance_id: NodeId | tuple[str, str],
    ) -> None:
        """
        Delete a range of datapoints from a time series by instance ID.

        Args:
            start (int | str | datetime.datetime): Inclusive start of the range to delete.
            end (int | str | datetime.datetime): Exclusive end of the range to delete.
            instance_id (NodeId | tuple[str, str]): Instance ID of the time series.

        Examples:

            Delete the last week of datapoints:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> client.data_modeling.time_series.data.delete_range(
                ...     start="1w-ago", end="now", instance_id=NodeId("my-space", "my-ts")
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.time_series.data.delete_range(
                start=start, end=end, instance_id=instance_id
            )
        )

    def insert_dataframe(self, df: pd.DataFrame, dropna: bool = True) -> None:
        """
        Insert datapoints from a pandas DataFrame by instance ID.

        The DataFrame index must be a ``pd.DatetimeIndex``. Each column identifier must be a
        :class:`~cognite.client.data_classes.data_modeling.NodeId` or a ``(space, external_id)`` tuple —
        passing ``int`` (id) or ``str`` (external_id) columns will raise a ``ValueError``.

        Note:
            Status codes cannot be inserted via this method. Use :meth:`insert` instead.

        Args:
            df (pd.DataFrame): DataFrame with a DatetimeIndex and one column per time series (identified by NodeId or tuple).
            dropna (bool): Ignore NaN values per column. Default: True.

        Examples:

            Insert a DataFrame with two time series columns:

                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> df = pd.DataFrame(
                ...     {
                ...         NodeId("my-space", "ts-1"): [1.0, 2.0],
                ...         NodeId("my-space", "ts-2"): [3.0, 4.0],
                ...     },
                ...     index=pd.date_range("2024-01-01", periods=2, freq="1d"),
                ... )
                >>> client.data_modeling.time_series.data.insert_dataframe(df)
        """
        return run_sync(self.__async_client.data_modeling.time_series.data.insert_dataframe(df=df, dropna=dropna))
