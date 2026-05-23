"""
===============================================================================
72f1fcfbbaf6d174dae9fe2def8749ce
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.data_modeling.datapoints import SyncDataModelingDatapointsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.data_modeling.instances import InstanceSort, Node, NodeList
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
from cognite.client._api.data_modeling.instances import Source


class SyncDataModelingTimeSeriesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.data = SyncDataModelingDatapointsAPI(async_client)

    @overload
    def retrieve(self, instance_id: NodeId | tuple[str, str], *, source: Source | None = None) -> Node | None: ...

    @overload
    def retrieve(
        self, instance_id: Sequence[NodeId | tuple[str, str]], *, source: Source | None = None
    ) -> NodeList[Node]: ...

    def retrieve(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        *,
        source: Source | None = None,
    ) -> Node | NodeList[Node] | None:
        """
        `Retrieve one or more time series nodes by instance ID. <https://api-docs.cognite.com/20230101/tag/Instances/operation/byExternalIdsInstances>`_

        Properties are fetched from the given source view. The source must implement CogniteTimeSeries
        (``ViewId("cdf_cdm", "CogniteTimeSeries", "v1")``). Defaults to CogniteTimeSeries itself.

        Args:
            instance_id (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Single instance ID or a list of instance IDs.
            source (Source | None): View to retrieve properties from. Must implement CogniteTimeSeries. Defaults to ``ViewId("cdf_cdm", "CogniteTimeSeries", "v1")``.

        Returns:
            Node | NodeList[Node] | None: A single ``Node`` (or ``None`` if not found) when given a single identifier, or a ``NodeList`` when given a sequence.

        Examples:

            Get a single time series by instance ID (uses CogniteTimeSeries source by default):

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.time_series.retrieve(NodeId("my-space", "my-ts"))

            Using a tuple shorthand:

                >>> res = client.data_modeling.time_series.retrieve(("my-space", "my-ts"))

            Get multiple time series:

                >>> res = client.data_modeling.time_series.retrieve(
                ...     [NodeId("my-space", "ts-1"), ("my-space", "ts-2")]
                ... )

            Get with a custom source that implements CogniteTimeSeries:

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> res = client.data_modeling.time_series.retrieve(
                ...     NodeId("my-space", "my-ts"),
                ...     source=ViewId("my-space", "MyTimeSeries", "v1"),
                ... )
        """
        return run_sync(self.__async_client.data_modeling.time_series.retrieve(instance_id=instance_id, source=source))

    def list(
        self,
        *,
        source: Source | None = None,
        space: str | SequenceNotStr[str] | None = None,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> NodeList[Node]:
        """
        `List time series nodes from Data Modeling. <https://api-docs.cognite.com/20230101/tag/Instances/operation/listInstances>`_

        Args:
            source (Source | None): View to retrieve properties from. Must implement CogniteTimeSeries. Defaults to ``ViewId("cdf_cdm", "CogniteTimeSeries", "v1")``.
            space (str | SequenceNotStr[str] | None): Restrict results to this space (or list of spaces).
            sort (Sequence[InstanceSort | dict] | InstanceSort | dict | None): Sort order for the results.
            filter (Filter | dict[str, Any] | None): Advanced filter to apply. See :class:`~cognite.client.data_classes.filters`.
            limit (int | None): Maximum number of nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            NodeList[Node]: The matching time series nodes.

        Examples:

            List all CogniteTimeSeries nodes:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.time_series.list()

            List nodes in a specific space:

                >>> res = client.data_modeling.time_series.list(space="my-space")

            List with a custom source and filter:

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes import filters
                >>> res = client.data_modeling.time_series.list(
                ...     source=ViewId("my-space", "MyTimeSeries", "v1"),
                ...     filter=filters.Equals(["cdf_cdm", "CogniteTimeSeries", "v1", "isStep"], True),
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.time_series.list(
                source=source, space=space, sort=sort, filter=filter, limit=limit
            )
        )
