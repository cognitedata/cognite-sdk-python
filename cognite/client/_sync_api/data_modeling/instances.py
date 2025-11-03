"""
===============================================================================
a8ae2f24eb039b7171759e027b913883
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._api.data_modeling.instances import Source
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.aggregations import (
    AggregatedNumberedValue,
    Histogram,
    HistogramValue,
    MetricAggregation,
)
from cognite.client.data_classes.data_modeling.ids import EdgeId, NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import (
    Edge,
    EdgeApply,
    EdgeList,
    InstanceAggregationResultList,
    InstanceInspectResults,
    InstancesApplyResult,
    InstancesDeleteResult,
    InstanceSort,
    InstancesResult,
    InvolvedContainers,
    InvolvedViews,
    Node,
    NodeApply,
    NodeList,
    T_Edge,
    T_Node,
    TargetUnit,
)
from cognite.client.data_classes.data_modeling.query import Query, QueryResult, QuerySync
from cognite.client.data_classes.data_modeling.sync import SubscriptionContext
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
from cognite.client.data_classes.data_modeling.debug import DebugParameters


class SyncInstancesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        instance_type: Literal["node"] = "node",
        limit: int | None = None,
        include_typing: bool = False,
        sources: Source | Sequence[Source] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> Iterator[Node]: ...

    @overload
    def __call__(
        self,
        chunk_size: None,
        instance_type: Literal["edge"],
        limit: int | None = None,
        include_typing: bool = False,
        sources: Source | Sequence[Source] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> Iterator[Edge]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        instance_type: Literal["node"] = "node",
        limit: int | None = None,
        include_typing: bool = False,
        sources: Source | Sequence[Source] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> Iterator[NodeList]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        instance_type: Literal["edge"],
        limit: int | None = None,
        include_typing: bool = False,
        sources: Source | Sequence[Source] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> Iterator[EdgeList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        instance_type: Literal["node", "edge"] = "node",
        limit: int | None = None,
        include_typing: bool = False,
        sources: Source | Sequence[Source] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> Iterator[Edge | EdgeList | Node | NodeList]:
        """
        Iterate over nodes or edges.
        Fetches instances as they are iterated over, so you keep a limited number of instances in memory.

        Args:
            chunk_size (int | None): Number of data_models to return in each chunk. Defaults to yielding one instance at a time.
            instance_type (Literal['node', 'edge']): Whether to query for nodes or edges.
            limit (int | None): Maximum number of instances to return. Defaults to returning all items.
            include_typing (bool): Whether to return property type information as part of the result.
            sources (Source | Sequence[Source] | None): Views to retrieve properties from.
            space (str | SequenceNotStr[str] | None): Only return instances in the given space (or list of spaces).
            sort (list[InstanceSort | dict] | InstanceSort | dict | None): Sort(s) to apply to the returned instances. For nontrivial amounts of data, you need to have a backing, cursorable index.
            filter (Filter | dict[str, Any] | None): Advanced filtering of instances.
            debug (DebugParameters | None): Debug settings for profiling and troubleshooting.

        Yields:
            Edge | EdgeList | Node | NodeList: yields Instance one by one if chunk_size is not specified, else NodeList/EdgeList objects.
        """
        yield from SyncIterator(
            self.__async_client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type=instance_type,
                limit=limit,
                include_typing=include_typing,
                sources=sources,
                space=space,
                sort=sort,
                filter=filter,
                debug=debug,
            )
        )  # type: ignore [misc]

    @overload
    def retrieve_edges(self, edges: EdgeId | tuple[str, str], *, edge_cls: type[T_Edge]) -> T_Edge | None: ...

    @overload
    def retrieve_edges(
        self,
        edges: EdgeId | tuple[str, str],
        *,
        sources: Source | Sequence[Source] | None = None,
        include_typing: bool = False,
    ) -> Edge | None: ...

    @overload
    def retrieve_edges(
        self, edges: Sequence[EdgeId] | Sequence[tuple[str, str]], *, edge_cls: type[T_Edge]
    ) -> EdgeList[T_Edge]: ...

    @overload
    def retrieve_edges(
        self,
        edges: Sequence[EdgeId] | Sequence[tuple[str, str]],
        *,
        sources: Source | Sequence[Source] | None = None,
        include_typing: bool = False,
    ) -> EdgeList[Edge]: ...

    def retrieve_edges(
        self,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]],
        edge_cls: type[T_Edge] = Edge,  # type: ignore [assignment]
        sources: Source | Sequence[Source] | None = None,
        include_typing: bool = False,
    ) -> EdgeList[T_Edge] | T_Edge | Edge | None:
        """
        `Retrieve one or more edges by id(s). <https://developer.cognite.com/api#tag/Instances/operation/byExternalIdsInstances>`_

        Note:
            This method should be used for retrieving edges with a custom edge class. You can use it
            without providing a custom edge class, but in that case, the retrieved edges will be of the
            built-in Edge class.


        Args:
            edges (EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]]): Edge id(s) to retrieve.
            edge_cls (type[T_Edge]): The custom edge class to use, the retrieved edges will automatically be serialized into this class.
            sources (Source | Sequence[Source] | None): Retrieve properties from the listed - by reference - views. This only applies if you do not provide a custom edge class.
            include_typing (bool): Whether to include typing information

        Returns:
            EdgeList[T_Edge] | T_Edge | Edge | None: The requested edges.

        Examples:

            Retrieve edges using a custom typed class "Flow". Any property that you want to look up by a different attribute name,
            e.g. you want `my_edge.flow_rate` to return the data for property `flowRate`, must use the PropertyOptions as shown below.
            We strongly suggest you use snake_cased attribute names, as is done here:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import EdgeId, TypedEdge, PropertyOptions, DirectRelationReference, ViewId
                >>> class Flow(TypedEdge):
                ...    flow_rate = PropertyOptions(identifier="flowRate")
                ...
                ...    def __init__(
                ...        self,
                ...        space: str,
                ...        external_id: str,
                ...        version: int,
                ...        type: DirectRelationReference,
                ...        last_updated_time: int,
                ...        created_time: int,
                ...        flow_rate: float,
                ...        start_node: DirectRelationReference,
                ...        end_node: DirectRelationReference,
                ...        deleted_time: int | None = None,
                ...    ) -> None:
                ...        super().__init__(
                ...            space, external_id, version, type, last_updated_time, created_time, start_node, end_node, deleted_time
                ...        )
                ...        self.flow_rate = flow_rate
                ...
                ...    @classmethod
                ...    def get_source(cls) -> ViewId:
                ...        return ViewId("sp_model_space", "flow", "1")
                ...
                >>> client = CogniteClient()
                >>> res = client.data_modeling.instances.retrieve_edges(
                ...     EdgeId("mySpace", "theFlow"), edge_cls=Flow
                ... )
                >>> isinstance(res, Flow)
        """
        return run_sync(
            self.__async_client.data_modeling.instances.retrieve_edges(
                edges=edges, edge_cls=edge_cls, sources=sources, include_typing=include_typing
            )  # type: ignore [call-overload]
        )

    @overload
    def retrieve_nodes(self, nodes: NodeId | tuple[str, str], *, node_cls: type[T_Node]) -> T_Node | None: ...

    @overload
    def retrieve_nodes(
        self,
        nodes: NodeId | tuple[str, str],
        *,
        sources: Source | Sequence[Source] | None = None,
        include_typing: bool = False,
    ) -> Node | None: ...

    @overload
    def retrieve_nodes(
        self, nodes: Sequence[NodeId] | Sequence[tuple[str, str]], *, node_cls: type[T_Node]
    ) -> NodeList[T_Node]: ...

    @overload
    def retrieve_nodes(
        self,
        nodes: Sequence[NodeId] | Sequence[tuple[str, str]],
        *,
        sources: Source | Sequence[Source] | None = None,
        include_typing: bool = False,
    ) -> NodeList[Node]: ...

    def retrieve_nodes(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]],
        node_cls: type[T_Node] = Node,  # type: ignore [assignment]
        sources: Source | Sequence[Source] | None = None,
        include_typing: bool = False,
    ) -> NodeList[T_Node] | T_Node | Node | None:
        """
        `Retrieve one or more nodes by id(s). <https://developer.cognite.com/api#tag/Instances/operation/byExternalIdsInstances>`_

        Note:
            This method should be used for retrieving nodes with a custom node class. You can use it
            without providing a custom node class, but in that case, the retrieved nodes will be of the
            built-in Node class.

        Args:
            nodes (NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]]): Node id(s) to retrieve.
            node_cls (type[T_Node]): The custom node class to use, the retrieved nodes will automatically be serialized to this class.
            sources (Source | Sequence[Source] | None): Retrieve properties from the listed - by reference - views. This only applies if you do not provide a custom node class.
            include_typing (bool): Whether to include typing information

        Returns:
            NodeList[T_Node] | T_Node | Node | None: The requested edges.

        Examples:

            Retrieve nodes using a custom typed node class "Person". Any property that you want to look up by a different attribute name,
            e.g. you want `my_node.birth_year` to return the data for property `birthYear`, must use the PropertyOptions as shown below.
            We strongly suggest you use snake_cased attribute names, as is done here:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId, TypedNode, PropertyOptions, DirectRelationReference, ViewId
                >>> class Person(TypedNode):
                ...    birth_year = PropertyOptions(identifier="birthYear")
                ...
                ...    def __init__(
                ...        self,
                ...        space: str,
                ...        external_id: str,
                ...        version: int,
                ...        last_updated_time: int,
                ...        created_time: int,
                ...        name: str,
                ...        birth_year: int | None = None,
                ...        type: DirectRelationReference | None = None,
                ...        deleted_time: int | None = None,
                ...    ):
                ...        super().__init__(
                ...            space=space,
                ...            external_id=external_id,
                ...            version=version,
                ...            last_updated_time=last_updated_time,
                ...            created_time=created_time,
                ...            type=type,
                ...            deleted_time=deleted_time
                ...        )
                ...        self.name = name
                ...        self.birth_year = birth_year
                ...
                ...    @classmethod
                ...    def get_source(cls) -> ViewId:
                ...        return ViewId("myModelSpace", "Person", "1")
                ...
                >>> client = CogniteClient()
                >>> res = client.data_modeling.instances.retrieve_nodes(
                ...     NodeId("myDataSpace", "myPerson"), node_cls=Person
                ... )
                >>> isinstance(res, Person)
        """
        return run_sync(
            self.__async_client.data_modeling.instances.retrieve_nodes(
                nodes=nodes, node_cls=node_cls, sources=sources, include_typing=include_typing
            )  # type: ignore [call-overload]
        )

    def retrieve(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        sources: Source | Sequence[Source] | None = None,
        include_typing: bool = False,
    ) -> InstancesResult[Node, Edge]:
        """
        `Retrieve one or more instance by id(s). <https://developer.cognite.com/api#tag/Instances/operation/byExternalIdsInstances>`_

        Args:
            nodes (NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Node ids
            edges (EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Edge ids
            sources (Source | Sequence[Source] | None): Retrieve properties from the listed - by reference - views.
            include_typing (bool): Whether to return property type information as part of the result.

        Returns:
            InstancesResult[Node, Edge]: Requested instances.

        Examples:

            Retrieve instances by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.instances.retrieve(
                ...     nodes=("mySpace", "myNodeExternalId"),
                ...     edges=("mySpace", "myEdgeExternalId"),
                ...     sources=("mySpace", "myViewExternalId", "myViewVersion"))

            Retrieve nodes an edges using the built in data class

                >>> from cognite.client.data_classes.data_modeling import NodeId, EdgeId, ViewId
                >>> res = client.data_modeling.instances.retrieve(
                ...     NodeId("mySpace", "myNode"),
                ...     EdgeId("mySpace", "myEdge"),
                ...     ViewId("mySpace", "myViewExternalId", "myViewVersion"))

            Retrieve nodes an edges using the the view object as source

                >>> from cognite.client.data_classes.data_modeling import NodeId, EdgeId
                >>> res = client.data_modeling.instances.retrieve(
                ...     NodeId("mySpace", "myNode"),
                ...     EdgeId("mySpace", "myEdge"),
                ...     sources=("myspace", "myView"))
        """
        return run_sync(
            self.__async_client.data_modeling.instances.retrieve(
                nodes=nodes, edges=edges, sources=sources, include_typing=include_typing
            )
        )

    def delete(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
    ) -> InstancesDeleteResult:
        """
        `Delete one or more instances <https://developer.cognite.com/api#tag/Instances/operation/deleteBulk>`_

        Args:
            nodes (NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Node ids
            edges (EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Edge ids

        Returns:
            InstancesDeleteResult: The instance ID(s) that was deleted. Empty list if nothing was deleted.

        Examples:

            Delete instances by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.instances.delete(nodes=("mySpace", "myNode"))

            Delete nodes and edges using the built in data class

                >>> from cognite.client.data_classes.data_modeling import NodeId, EdgeId
                >>> client.data_modeling.instances.delete(NodeId('mySpace', 'myNode'), EdgeId('mySpace', 'myEdge'))

            Delete all nodes from a NodeList

                >>> from cognite.client.data_classes.data_modeling import NodeId, EdgeId
                >>> my_view = client.data_modeling.views.retrieve(('mySpace', 'myView'))
                >>> my_nodes = client.data_modeling.instances.list(instance_type='node', sources=my_view, limit=None)
                >>> client.data_modeling.instances.delete(nodes=my_nodes.as_ids())
        """
        return run_sync(self.__async_client.data_modeling.instances.delete(nodes=nodes, edges=edges))

    def inspect(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        *,
        involved_views: InvolvedViews | None = None,
        involved_containers: InvolvedContainers | None = None,
    ) -> InstanceInspectResults:
        """
        `Reverse lookup for instances. <https://developer.cognite.com/api/v1/#tag/Instances/operation/instanceInspect>`_

        This method will return the involved views and containers for the given nodes and edges.

        Args:
            nodes (NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Node IDs.
            edges (EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Edge IDs.
            involved_views (InvolvedViews | None): Whether to include involved views. Must pass at least one of involved_views or involved_containers.
            involved_containers (InvolvedContainers | None): Whether to include involved containers. Must pass at least one of involved_views or involved_containers.

        Returns:
            InstanceInspectResults: List of instance inspection results.

        Examples:

            Look up the involved views for a given node and edge:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId, EdgeId, InvolvedViews
                >>> client = CogniteClient()
                >>> res = client.data_modeling.instances.inspect(
                ...     nodes=NodeId("my-space", "foo1"),
                ...     edges=EdgeId("my-space", "bar2"),
                ...     involved_views=InvolvedViews(all_versions=False),
                ... )

            Look up the involved containers:

                >>> from cognite.client.data_classes.data_modeling import InvolvedContainers
                >>> res = client.data_modeling.instances.inspect(
                ...     nodes=[("my-space", "foo1"), ("my-space", "foo2")],
                ...     involved_containers=InvolvedContainers(),
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.instances.inspect(
                nodes=nodes, edges=edges, involved_views=involved_views, involved_containers=involved_containers
            )
        )

    def subscribe(
        self,
        query: QuerySync,
        callback: Callable[[QueryResult], None | Awaitable[None]],
        poll_delay_seconds: float = 30,
        throttle_seconds: float = 1,
    ) -> SubscriptionContext:
        """
        Subscribe to a query and get updates when the result set changes. This runs the sync() method in a background task.
        We do not support chaining result sets when subscribing to a query.

        Tip:
            For a practical guide on using this method to create a live local replica of your data,
            see :ref:`this example of syncing instances to a local SQLite database <dm_instances_subscribe_example>`.

        Args:
            query (QuerySync): The query to subscribe to.
            callback (Callable[[QueryResult], None | Awaitable[None]]): The callback function to call when the result set changes. Can be a regular or async function.
            poll_delay_seconds (float): The time to wait between polls when no data is present. Defaults to 30 seconds.
            throttle_seconds (float): The time to wait between polls despite data being present.

        Returns:
            SubscriptionContext: An object that can be used to inspect and cancel the subscription.

        Examples:

            Subscribe to a given query and process the results in your own callback function
            (here we just print the result for illustration):

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.query import (
                ...     QuerySync, QueryResult, NodeResultSetExpressionSync, SelectSync, SourceSelector
                ... )
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes.filters import Equals
                >>>
                >>> client = CogniteClient()
                >>> def just_print_the_result(result: QueryResult) -> None:
                ...     print(result)
                >>>
                >>> view_id = ViewId("someSpace", "someView", "v1")
                >>> filter = Equals(view_id.as_property_ref("myAsset"), "Il-Tempo-Gigante")
                >>> query = QuerySync(
                ...     with_={"work_orders": NodeResultSetExpressionSync(filter=filter)},
                ...     select={"work_orders": SelectSync([SourceSelector(view_id, ["*"])])}
                ... )
                >>> subscription_context = client.data_modeling.instances.subscribe(
                ...     query, callback=just_print_the_result
                ... )
                >>> # Use the returned subscription_context to manage the subscription, e.g. to cancel it:
                >>> subscription_context.cancel()
        """
        return run_sync(
            self.__async_client.data_modeling.instances.subscribe(
                query=query, callback=callback, poll_delay_seconds=poll_delay_seconds, throttle_seconds=throttle_seconds
            )
        )

    def apply(
        self,
        nodes: NodeApply | Sequence[NodeApply] | None = None,
        edges: EdgeApply | Sequence[EdgeApply] | None = None,
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        auto_create_direct_relations: bool = True,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> InstancesApplyResult:
        """
        `Add or update (upsert) instances. <https://developer.cognite.com/api#tag/Instances/operation/applyNodeAndEdges>`_

        Args:
            nodes (NodeApply | Sequence[NodeApply] | None): Nodes to apply
            edges (EdgeApply | Sequence[EdgeApply] | None): Edges to apply
            auto_create_start_nodes (bool): Whether to create missing start nodes for edges when ingesting. By default, the start node of an edge must exist before it can be ingested.
            auto_create_end_nodes (bool): Whether to create missing end nodes for edges when ingesting. By default, the end node of an edge must exist before it can be ingested.
            auto_create_direct_relations (bool): Whether to create missing direct relation targets when ingesting.
            skip_on_version_conflict (bool): If existingVersion is specified on any of the nodes/edges in the input, the default behaviour is that the entire ingestion will fail when version conflicts occur. If skipOnVersionConflict is set to true, items with version conflicts will be skipped instead. If no version is specified for nodes/edges, it will do the writing directly.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)? Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            InstancesApplyResult: Created instance(s)

        Examples:

            Create new node without data:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import EdgeApply, NodeOrEdgeData, NodeApply
                >>> client = CogniteClient()
                >>> node = NodeApply("mySpace", "myNodeId")
                >>> res = client.data_modeling.instances.apply(node)

            Create two nodes with data with a one-to-many edge

                >>> from cognite.client.data_classes.data_modeling import ContainerId, EdgeApply, NodeOrEdgeData, NodeApply, ViewId
                >>> work_order = NodeApply(
                ...     space="industrial",
                ...     external_id="work_order:123",
                ...     sources=[
                ...         # Insert data through a view
                ...         NodeOrEdgeData(
                ...             ViewId("mySpace", "WorkOrderView", "v1"),
                ...             {"title": "Repair pump", "createdYear": 2023}
                ...         )
                ...     ]
                ... )
                >>> pump = NodeApply(
                ...     space="industrial",
                ...     external_id="pump:456",
                ...     sources=[
                ...         # Insert data directly to the container
                ...         NodeOrEdgeData(
                ...             ContainerId("mySpace", "PumpContainer"),
                ...             {"name": "Pump 456", "location": "Subsea"}
                ...         )
                ...     ]
                ... )
                ... # This is one-to-many edge, in this case from a work order to a pump
                >>> work_order_to_pump = EdgeApply(
                ...     space="industrial",
                ...     external_id="relation:work_order:123:pump:456",
                ...     type=("industrial", "relates-to"),
                ...     start_node=("industrial", "work_order:123"),
                ...     end_node=("industrial", "pump:456"),
                ... )
                >>> res = client.data_modeling.instances.apply([work_order, pump], [work_order_to_pump])

            Create new edge and automatically create end nodes.

                >>> from cognite.client.data_classes.data_modeling import EdgeApply
                >>> work_order_to_pump = EdgeApply(
                ...     space="industrial",
                ...     external_id="relation:work_order:123:pump:456",
                ...     type=("industrial", "relates-to"),
                ...     start_node=("industrial", "work_order:123"),
                ...     end_node=("industrial", "pump:456"),
                ... )
                >>> res = client.data_modeling.instances.apply(
                ...     edges=work_order_to_pump,
                ...     auto_create_start_nodes=True,
                ...     auto_create_end_nodes=True
                ... )

            Using helper function to create valid graphql timestamp for a datetime object:

                >>> from cognite.client.utils import datetime_to_ms_iso_timestamp
                >>> from datetime import datetime, timezone
                >>> my_date = datetime(2020, 3, 14, 15, 9, 26, 535000, tzinfo=timezone.utc)
                >>> data_model_timestamp = datetime_to_ms_iso_timestamp(my_date)  # "2020-03-14T15:09:26.535+00:00"

            Create a typed node apply. Any property that you want to look up by a different attribute name, e.g. you want
            `my_node.birth_year` to return the data for property `birthYear`, must use the PropertyOptions as shown below.
            We strongly suggest you use snake_cased attribute names, as is done here:

                >>> from cognite.client.data_classes.data_modeling import TypedNodeApply, PropertyOptions
                >>> class PersonApply(TypedNodeApply):
                ...     birth_year = PropertyOptions(identifier="birthYear")
                ...
                ...     def __init__(self, space: str, external_id, name: str, birth_year: int):
                ...         super().__init__(space, external_id, type=("sp_model_space", "Person"))
                ...         self.name = name
                ...         self.birth_year = birth_year
                ...     def get_source(self):
                ...         return ViewId("sp_model_space", "Person", "v1")
                ...
                >>> person = PersonApply("sp_date_space", "my_person", "John Doe", 1980)
                >>> res = client.data_modeling.instances.apply(nodes=person)
        """
        return run_sync(
            self.__async_client.data_modeling.instances.apply(
                nodes=nodes,
                edges=edges,
                auto_create_start_nodes=auto_create_start_nodes,
                auto_create_end_nodes=auto_create_end_nodes,
                auto_create_direct_relations=auto_create_direct_relations,
                skip_on_version_conflict=skip_on_version_conflict,
                replace=replace,
            )
        )

    @overload
    def search(
        self,
        view: ViewId,
        query: str | None,
        instance_type: Literal["node"] = "node",
        properties: list[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        include_typing: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        operator: Literal["AND", "OR"] = "OR",
    ) -> NodeList[Node]: ...

    @overload
    def search(
        self,
        view: ViewId,
        query: str | None,
        instance_type: Literal["edge"],
        properties: list[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        include_typing: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        operator: Literal["AND", "OR"] = "OR",
    ) -> EdgeList[Edge]: ...

    @overload
    def search(
        self,
        view: ViewId,
        query: str | None,
        instance_type: type[T_Node],
        properties: list[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        include_typing: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        operator: Literal["AND", "OR"] = "OR",
    ) -> NodeList[T_Node]: ...

    @overload
    def search(
        self,
        view: ViewId,
        query: str | None,
        instance_type: type[T_Edge],
        properties: list[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        include_typing: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        operator: Literal["AND", "OR"] = "OR",
    ) -> EdgeList[T_Edge]: ...

    def search(
        self,
        view: ViewId,
        query: str | None = None,
        instance_type: Literal["node", "edge"] | type[T_Node] | type[T_Edge] = "node",
        properties: list[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        include_typing: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        operator: Literal["AND", "OR"] = "OR",
    ) -> NodeList[T_Node] | EdgeList[T_Edge]:
        """
        `Search instances <https://developer.cognite.com/api/v1/#tag/Instances/operation/searchInstances>`_

        Args:
            view (ViewId): View to search in.
            query (str | None): Query string that will be parsed and used for search.
            instance_type (Literal['node', 'edge'] | type[T_Node] | type[T_Edge]): Whether to search for nodes or edges. You can also pass a custom typed node (or edge class) inheriting from TypedNode (or TypedEdge). See apply, retrieve_nodes or retrieve_edges for an example.
            properties (list[str] | None): Optional array of properties you want to search through. If you do not specify one or more properties, the service will search all text fields within the view.
            target_units (list[TargetUnit] | None): Properties to convert to another unit. The API can only convert to another unit if a unit has been defined as part of the type on the underlying container being queried.
            space (str | SequenceNotStr[str] | None): Restrict instance search to the given space (or list of spaces).
            filter (Filter | dict[str, Any] | None): Advanced filtering of instances.
            include_typing (bool): Whether to include typing information.
            limit (int | None): Maximum number of instances to return. Defaults to 25. Will return the maximum number
                of results (1000) if set to None, -1, or math.inf.
            sort (Sequence[InstanceSort | dict] | InstanceSort | dict | None): How you want the listed instances information ordered.
            operator (Literal['AND', 'OR']): Controls how multiple search terms are combined when matching documents. OR (default): A document matches if it contains any of the query terms in the searchable fields. This typically returns more results but with lower precision. AND: A document matches only if it contains all of the query terms across the searchable fields. This typically returns fewer results but with higher relevance.

        Returns:
            NodeList[T_Node] | EdgeList[T_Edge]: Search result with matching nodes or edges.

        Examples:

            Search for Arnold in the person view in the name property:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.instances.search(
                ...     ViewId("mySpace", "PersonView", "v1"),
                ...     query="Arnold",
                ...     properties=["name"],
                ... )

            Search for Tarantino, Ritchie or Scorsese in the person view in the name property, but only born after 1942:

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes import filters
                >>> born_after_1942 = filters.Range(["mySpace", "PersonView/v1", "birthYear"], gt=1942)
                >>> res = client.data_modeling.instances.search(
                ...     ViewId("mySpace", "PersonView", "v1"),
                ...     query="Tarantino Ritchie Scorsese",
                ...     properties=["name"],
                ...     filter=born_after_1942,
                ...     operator="OR"
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.instances.search(
                view=view,
                query=query,
                instance_type=instance_type,
                properties=properties,
                target_units=target_units,
                space=space,
                filter=filter,
                include_typing=include_typing,
                limit=limit,
                sort=sort,
                operator=operator,
            )
        )

    @overload
    def aggregate(
        self,
        view: ViewId,
        aggregates: MetricAggregation | dict,
        group_by: None = None,
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: str | SequenceNotStr[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        view: ViewId,
        aggregates: Sequence[MetricAggregation | dict],
        group_by: None = None,
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: str | SequenceNotStr[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> list[AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        view: ViewId,
        aggregates: MetricAggregation | dict | Sequence[MetricAggregation | dict],
        group_by: str | SequenceNotStr[str],
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: str | SequenceNotStr[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        view: ViewId,
        aggregates: MetricAggregation | dict | Sequence[MetricAggregation | dict],
        group_by: str | SequenceNotStr[str] | None = None,
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: str | SequenceNotStr[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> AggregatedNumberedValue | list[AggregatedNumberedValue] | InstanceAggregationResultList:
        """
        `Aggregate data across nodes/edges <https://developer.cognite.com/api/v1/#tag/Instances/operation/aggregateInstances>`_

        Args:
            view (ViewId): View to aggregate over.
            aggregates (MetricAggregation | dict | Sequence[MetricAggregation | dict]): The properties to aggregate over.
            group_by (str | SequenceNotStr[str] | None): The selection of fields to group the results by when doing aggregations. You can specify up to 5 items to group by.
            instance_type (Literal['node', 'edge']): The type of instance.
            query (str | None): Optional query string. The API will parse the query string, and use it to match the text properties on elements to use for the aggregate(s).
            properties (str | SequenceNotStr[str] | None): Optional list of properties you want to apply the query to. If you do not list any properties, you search through text fields by default.
            target_units (list[TargetUnit] | None): Properties to convert to another unit. The API can only convert to another unit if a unit has been defined as part of the type on the underlying container being queried.
            space (str | SequenceNotStr[str] | None): Restrict instance aggregate query to the given space (or list of spaces).
            filter (Filter | dict[str, Any] | None): Advanced filtering of instances.
            limit (int | None): Maximum number of instances to return. Defaults to 25. Will return the maximum number
                of results (1000) if set to None, -1, or math.inf.

        Returns:
            AggregatedNumberedValue | list[AggregatedNumberedValue] | InstanceAggregationResultList: Node or edge aggregation results.

        Examples:

            Get the average run time in minutes for pumps grouped by release year:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewId, aggregations as aggs
                >>> client = CogniteClient()
                >>> avg_run_time = aggs.Avg("runTimeMinutes")
                >>> view_id = ViewId("mySpace", "PumpView", "v1")
                >>> res = client.data_modeling.instances.aggregate(view_id, avg_run_time, group_by="releaseYear")
        """
        return run_sync(
            self.__async_client.data_modeling.instances.aggregate(
                view=view,
                aggregates=aggregates,
                group_by=group_by,
                instance_type=instance_type,
                query=query,
                properties=properties,
                target_units=target_units,
                space=space,
                filter=filter,
                limit=limit,
            )
        )

    @overload
    def histogram(
        self,
        view: ViewId,
        histograms: Histogram,
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: SequenceNotStr[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> HistogramValue: ...

    @overload
    def histogram(
        self,
        view: ViewId,
        histograms: Sequence[Histogram],
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: SequenceNotStr[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> list[HistogramValue]: ...

    def histogram(
        self,
        view: ViewId,
        histograms: Histogram | Sequence[Histogram],
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: SequenceNotStr[str] | None = None,
        target_units: list[TargetUnit] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> HistogramValue | list[HistogramValue]:
        """
        `Produces histograms for nodes/edges <https://developer.cognite.com/api/v1/#tag/Instances/operation/aggregateInstances>`_

        Args:
            view (ViewId): View to to aggregate over.
            histograms (Histogram | Sequence[Histogram]):  The properties to aggregate over.
            instance_type (Literal['node', 'edge']): Whether to search for nodes or edges.
            query (str | None): Query string that will be parsed and used for search.
            properties (SequenceNotStr[str] | None): Optional array of properties you want to search through. If you do not specify one or more properties, the service will search all text fields within the view.
            target_units (list[TargetUnit] | None): Properties to convert to another unit. The API can only convert to another unit if a unit has been defined as part of the type on the underlying container being queried.
            space (str | SequenceNotStr[str] | None): Restrict histogram query to instances in the given space (or list of spaces).
            filter (Filter | dict[str, Any] | None): Advanced filtering of instances.
            limit (int): Maximum number of instances to return. Defaults to 25.

        Returns:
            HistogramValue | list[HistogramValue]: Node or edge aggregation results.

        Examples:

            Find the number of people born per decade:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import aggregations as aggs, ViewId
                >>> client = CogniteClient()
                >>> birth_by_decade = aggs.Histogram("birthYear", interval=10.0)
                >>> view_id = ViewId("mySpace", "PersonView", "v1")
                >>> res = client.data_modeling.instances.histogram(view_id, birth_by_decade)
        """
        return run_sync(
            self.__async_client.data_modeling.instances.histogram(
                view=view,
                histograms=histograms,
                instance_type=instance_type,
                query=query,
                properties=properties,
                target_units=target_units,
                space=space,
                filter=filter,
                limit=limit,
            )
        )

    def query(self, query: Query, include_typing: bool = False, debug: DebugParameters | None = None) -> QueryResult:
        """
        `Advanced query interface for nodes/edges. <https://developer.cognite.com/api/v1/#tag/Instances/operation/queryContent>`_

        The Data Modelling API exposes an advanced query interface. The query interface supports parameterization,
        recursive edge traversal, chaining of result sets, and granular property selection.

        Args:
            query (Query): Query.
            include_typing (bool): Should we return property type information as part of the result?
            debug (DebugParameters | None): Debug settings for profiling and troubleshooting.

        Returns:
            QueryResult: The resulting nodes and/or edges from the query.

        Examples:

            Find work orders created before 2023 sorted by title:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.query import Query, Select, NodeResultSetExpression, EdgeResultSetExpression, SourceSelector
                >>> from cognite.client.data_classes.filters import Range, Equals
                >>> from cognite.client.data_classes.data_modeling.ids import ViewId
                >>> client = CogniteClient()
                >>> work_order_id = ViewId("mySpace", "WorkOrderView", "v1")
                >>> pump_id = ViewId("mySpace", "PumpView", "v1")
                >>> query = Query(
                ...     with_ = {
                ...         "work_orders": NodeResultSetExpression(filter=Range(work_order_id.as_property_ref("createdYear"), lt=2023)),
                ...         "work_orders_to_pumps": EdgeResultSetExpression(from_="work_orders", filter=Equals(["edge", "type"], {"space": work_order_id.space, "externalId": "WorkOrder.asset"})),
                ...         "pumps": NodeResultSetExpression(from_="work_orders_to_pumps"),
                ...     },
                ...     select = {
                ...         "pumps": Select(
                ...             [SourceSelector(pump_id, ["name"])], sort=[InstanceSort(pump_id.as_property_ref("name"))]),
                ...     },
                ... )
                >>> res = client.data_modeling.instances.query(query)

            To convert units, specify what your target units are in the SourceSelector. You can either use
            a UnitReference or a UnitSystemReference. Note that in order for a property to be converted, they
            need to have a unit defined in the underlying container.

                >>> from cognite.client.data_classes.data_modeling.data_types import UnitReference, UnitSystemReference
                >>> selected_source = SourceSelector(
                ...     source=ViewId("my-space", "my-xid", "v1"),
                ...     properties=["f32_prop1", "f32_prop2", "f64_prop1", "f64_prop2"],
                ...     target_units=[
                ...         TargetUnit("f32_prop1", UnitReference("pressure:kilopa")),
                ...         TargetUnit("f32_prop2", UnitReference("pressure:barg")),
                ...         TargetUnit("f64_prop1", UnitSystemReference("SI")),
                ...         TargetUnit("f64_prop2", UnitSystemReference("Imperial")),
                ...     ],
                ... )

            To select all properties, use '[*]' in your SourceSelector:

                >>> SourceSelector(source=ViewId("my-space", "my-xid", "v1"), properties=["*"])

            To debug and/or profile your query, you can use the debug parameter:

                >>> from cognite.client.data_classes.data_modeling.debug import DebugParameters
                >>> debug_params = DebugParameters(
                ...     emit_results=False,
                ...     include_plan=True,  # Include the postgres execution plan
                ...     include_translated_query=True,  # Include the internal representation of the query.
                ...     profile=True,
                ... )
                >>> res = client.data_modeling.instances.query(query, debug=debug_params)
                >>> print(res.debug)
        """
        return run_sync(
            self.__async_client.data_modeling.instances.query(query=query, include_typing=include_typing, debug=debug)
        )

    def sync(self, query: QuerySync, include_typing: bool = False, debug: DebugParameters | None = None) -> QueryResult:
        """
        `Subscription to changes for nodes/edges. <https://developer.cognite.com/api/v1/#tag/Instances/operation/syncContent>`_

        Subscribe to changes for nodes and edges in a project, matching a supplied filter.

        Args:
            query (QuerySync): Query.
            include_typing (bool): Should we return property type information as part of the result?
            debug (DebugParameters | None): Debug settings for profiling and troubleshooting.

        Returns:
            QueryResult: The resulting nodes and/or edges from the query.

        Examples:

            Query all pumps connected to work orders created before 2023, sorted by name:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.instances import InstanceSort
                >>> from cognite.client.data_classes.data_modeling.query import Query, Select, NodeResultSetExpression, EdgeResultSetExpression, SourceSelector
                >>> from cognite.client.data_classes.filters import Range, Equals
                >>> from cognite.client.data_classes.data_modeling.ids import ViewId
                >>> client = CogniteClient()
                >>> work_order_id = ViewId("mySpace", "WorkOrderView", "v1")
                >>> pump_id = ViewId("mySpace", "PumpView", "v1")
                >>> query = Query(
                ...     with_ = {
                ...         "work_orders": NodeResultSetExpression(filter=Range(work_order_id.as_property_ref("createdYear"), lt=2023)),
                ...         "work_orders_to_pumps": EdgeResultSetExpression(from_="work_orders", filter=Equals(["edge", "type"], {"space": work_order_id.space, "externalId": "WorkOrder.asset"})),
                ...         "pumps": NodeResultSetExpression(from_="work_orders_to_pumps"),
                ...     },
                ...     select = {
                ...         "pumps": Select(
                ...             [SourceSelector(pump_id, ["name"])], sort=[InstanceSort(pump_id.as_property_ref("name"))]),
                ...     },
                ... )
                >>> res = client.data_modeling.instances.sync(query)
                >>> # Added a new work order with pumps created before 2023
                >>> query.cursors = res.cursors
                >>> res_new = client.data_modeling.instances.sync(query)

            In the last example, the res_new will only contain the pumps that have been added with the new work order.

            To debug and/or profile your query, you can use the debug parameter:

                >>> from cognite.client.data_classes.data_modeling.debug import DebugParameters
                >>> debug_params = DebugParameters(
                ...     emit_results=False,
                ...     include_plan=True,  # Include the postgres execution plan
                ...     include_translated_query=True,  # Include the internal representation of the query.
                ...     profile=True,
                ... )
                >>> res = client.data_modeling.instances.sync(query, debug=debug_params)
                >>> print(res.debug)
        """
        return run_sync(
            self.__async_client.data_modeling.instances.sync(query=query, include_typing=include_typing, debug=debug)
        )

    @overload
    def list(
        self,
        instance_type: Literal["node"] = "node",
        include_typing: bool = False,
        sources: Source | Sequence[Source] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> NodeList[Node]: ...

    @overload
    def list(
        self,
        instance_type: Literal["edge"],
        include_typing: bool = False,
        sources: Source | Sequence[Source] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> EdgeList[Edge]: ...

    @overload
    def list(
        self,
        instance_type: type[T_Node],
        *,
        space: str | SequenceNotStr[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> NodeList[T_Node]: ...

    @overload
    def list(
        self,
        instance_type: type[T_Edge],
        *,
        space: str | SequenceNotStr[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> EdgeList[T_Edge]: ...

    def list(
        self,
        instance_type: Literal["node", "edge"] | type[T_Node] | type[T_Edge] = "node",
        include_typing: bool = False,
        sources: Source | Sequence[Source] | None = None,
        space: str | SequenceNotStr[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        debug: DebugParameters | None = None,
    ) -> NodeList[T_Node] | EdgeList[T_Edge]:
        """
        `List instances <https://developer.cognite.com/api#tag/Instances/operation/advancedListInstance>`_

        Args:
            instance_type (Literal['node', 'edge'] | type[T_Node] | type[T_Edge]): Whether to query for nodes or edges. You can also pass a custom typed node (or edge class) inheriting from TypedNode (or TypedEdge). See apply, retrieve_nodes or retrieve_edges for an example.
            include_typing (bool): Whether to return property type information as part of the result.
            sources (Source | Sequence[Source] | None): Views to retrieve properties from.
            space (str | SequenceNotStr[str] | None): Only return instances in the given space (or list of spaces).
            limit (int | None): Maximum number of instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            sort (Sequence[InstanceSort | dict] | InstanceSort | dict | None): How you want the listed instances information ordered.
            filter (Filter | dict[str, Any] | None): Advanced filtering of instances.
            debug (DebugParameters | None): Debug settings for profiling and troubleshooting.

        Returns:
            NodeList[T_Node] | EdgeList[T_Edge]: List of requested instances

        Examples:

            List instances and limit to 5:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> instance_list = client.data_modeling.instances.list(limit=5)

            List some instances in the space 'my-space':

                >>> instance_list = client.data_modeling.instances.list(space="my-space")

            List instances and sort by some property:

                >>> from cognite.client.data_classes.data_modeling import InstanceSort
                >>> property_sort = InstanceSort(
                ...     property=('space', 'view_xid/view_version', 'some_property'),
                ...     direction="descending",
                ...     nulls_first=True)
                >>> instance_list = client.data_modeling.instances.list(sort=property_sort)

            Iterate over instances (nodes by default), one-by-one:

                >>> for node in client.data_modeling.instances():
                ...     node
                >>> for edge in client.data_modeling.instances(instance_type="edge"):
                ...     edge

            Iterate over chunks of instances to reduce memory load:

                >>> for instance_list in client.data_modeling.instances(chunk_size=100):
                ...     instance_list # do something with the instances

            List instances with a view as source:

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> my_view = ViewId("mySpace", "myView", "v1")
                >>> instance_list = client.data_modeling.instances.list(sources=my_view)

            Convert instances to pandas DataFrame with expanded properties (``expand_properties=True``).
            This will add the properties directly as dataframe columns. Specifying ``camel_case=True``
            will convert the basic columns to camel case (e.g. externalId), but leave the property names as-is.

                >>> df = instance_list.to_pandas(
                ...     expand_properties=True,
                ...     camel_case=True,
                ... )

            To debug and/or profile your query, you can use the debug parameter:

                >>> from cognite.client.data_classes.data_modeling.debug import DebugParameters
                >>> debug_params = DebugParameters(
                ...     emit_results=False,
                ...     include_plan=True,  # Include the postgres execution plan
                ...     include_translated_query=True,  # Include the internal representation of the query.
                ...     profile=True,
                ... )
                >>> res = client.data_modeling.instances.list(
                ...     debug=debug_params, sources=my_view
                ... )
                >>> print(res.debug)
        """
        return run_sync(
            self.__async_client.data_modeling.instances.list(
                instance_type=instance_type,  # type: ignore [arg-type]
                include_typing=include_typing,
                sources=sources,
                space=space,
                limit=limit,
                sort=sort,
                filter=filter,
                debug=debug,
            )  # type: ignore [misc]
        )
