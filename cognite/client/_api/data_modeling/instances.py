from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Literal, Optional, Sequence, Type, Union, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT
from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.data_modeling.aggregations import (
    Aggregation,
    Histogram,
    HistogramValue,
    MetricAggregation,
)
from cognite.client.data_classes.data_modeling.filters import Filter
from cognite.client.data_classes.data_modeling.ids import (
    EdgeId,
    NodeId,
    ViewId,
    ViewIdentifier,
    _load_identifier,
)
from cognite.client.data_classes.data_modeling.instances import (
    Edge,
    EdgeApply,
    EdgeApplyResult,
    EdgeApplyResultList,
    EdgeList,
    InstanceAggregationResultList,
    InstancesApplyResult,
    InstancesDeleteResult,
    InstanceSort,
    InstancesResult,
    Node,
    NodeApply,
    NodeApplyResult,
    NodeApplyResultList,
    NodeList,
)
from cognite.client.data_classes.data_modeling.query import (
    Query,
    QueryResult,
)
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.utils._identifier import DataModelingIdentifierSequence

from ._data_modeling_executor import get_data_modeling_executor

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class _NodeOrEdgeList(CogniteResourceList):
    _RESOURCE = (Node, Edge)  # type: ignore[assignment]

    @classmethod
    def _load(
        cls, resource_list: list[dict[str, Any]] | str, cognite_client: Optional[CogniteClient] = None
    ) -> _NodeOrEdgeList:
        resource_list = json.loads(resource_list) if isinstance(resource_list, str) else resource_list
        resources: list[Node | Edge] = [
            Node.load(data) if data["instanceType"] == "node" else Edge.load(data) for data in resource_list
        ]
        return cls(resources, None)

    def as_ids(self) -> list[NodeId | EdgeId]:
        return [instance.as_id() for instance in self]


class _NodeOrEdgeResourceAdapter:
    @classmethod
    def _load(cls, data: str | dict, cognite_client: Optional[CogniteClient] = None) -> Node | Edge:
        data = json.loads(data) if isinstance(data, str) else data
        if data["instanceType"] == "node":
            return Node.load(data)
        return Edge.load(data)


class _NodeOrEdgeApplyResultList(CogniteResourceList):
    _RESOURCE = (NodeApplyResult, EdgeApplyResult)  # type: ignore[assignment]

    @classmethod
    def _load(
        cls, resource_list: list[dict[str, Any]] | str, cognite_client: Optional[CogniteClient] = None
    ) -> _NodeOrEdgeApplyResultList:
        resource_list = json.loads(resource_list) if isinstance(resource_list, str) else resource_list
        resources: list[NodeApplyResult | EdgeApplyResult] = [
            NodeApplyResult.load(data) if data["instanceType"] == "node" else EdgeApplyResult.load(data)
            for data in resource_list
        ]
        return cls(resources, None)

    def as_ids(self) -> list[NodeId | EdgeId]:
        return [result.as_id() for result in self]


class _NodeOrEdgeApplyResultAdapter:
    @classmethod
    def _load(
        cls, data: str | dict, cognite_client: Optional[CogniteClient] = None
    ) -> NodeApplyResult | EdgeApplyResult:
        data = json.loads(data) if isinstance(data, str) else data
        if data["instanceType"] == "node":
            return NodeApplyResult.load(data)
        return EdgeApplyResult.load(data)


class _NodeOrEdgeApplyAdapter:
    @classmethod
    def _load(cls, data: str | dict, cognite_client: Optional[CogniteClient] = None) -> NodeApply | EdgeApply:
        data = json.loads(data) if isinstance(data, str) else data
        if data["instanceType"] == "node":
            return NodeApply.load(data)
        return EdgeApply.load(data)


class InstancesAPI(APIClient):
    _RESOURCE_PATH = "/models/instances"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        instance_type: Literal["node"] = "node",
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[Node]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: None,
        instance_type: Literal["edge"],
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[Edge]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        instance_type: Literal["node"] = "node",
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[NodeList]:
        ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        instance_type: Literal["edge"],
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[EdgeList]:
        ...

    def __call__(
        self,
        chunk_size: int | None = None,
        instance_type: Literal["node", "edge"] = "node",
        limit: int | None = None,
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> Iterator[Edge] | Iterator[EdgeList] | Iterator[Node] | Iterator[NodeList]:
        """Iterate over nodes or edges.
        Fetches instances as they are iterated over, so you keep a limited number of instances in memory.

        Args:
            chunk_size (int, optional): Number of data_models to return in each chunk. Defaults to yielding
                                        one instance at a time.
            instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
            limit (int, optional): Maximum number of instances to return. Default to return all items.
            include_typing (bool): Whether to return property type information as part of the result.
            sources (list[ViewId] | ViewId): Views to retrieve properties from.
            sort (list[InstanceSort | dict] | InstanceSort | dict): How you want the listed instances information ordered.
            filter (dict | Filter): Advanced filtering of instances.

        Yields:
            Edge | Node | EdgeList | NodeList: yields Instance one by one if chunk_size is not specified, else NodeList/EdgeList objects.
        """
        other_params = self._create_other_params(
            include_typing=include_typing, instance_type=instance_type, sort=sort, sources=sources
        )

        if instance_type == "node":
            resource_cls: type = _NodeOrEdgeResourceAdapter
            list_cls: type = NodeList
        elif instance_type == "edge":
            resource_cls, list_cls = _NodeOrEdgeResourceAdapter, EdgeList
        else:
            raise ValueError(f"Invalid instance type: {instance_type}")

        return cast(
            Union[Iterator[Edge], Iterator[EdgeList], Iterator[Node], Iterator[NodeList]],
            self._list_generator(
                list_cls=list_cls,
                resource_cls=resource_cls,
                method="POST",
                chunk_size=chunk_size,
                limit=limit,
                filter=filter.dump() if isinstance(filter, Filter) else filter,
                other_params=other_params,
            ),
        )

    def __iter__(self) -> Iterator[Node]:
        """Iterate over instances
        Fetches instances as they are iterated over, so you keep a limited number of instances in memory.
        Yields:
            Instance: yields Instances one by one.
        """
        return cast(Iterator[Node], self(None, "node"))

    def retrieve(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None = None,
        include_typing: bool = False,
    ) -> InstancesResult:
        """`Retrieve one or more instance by id(s). <https://developer.cognite.com/api#tag/Instances/tag/Instances/operation/byExternalIdsInstances>`_

        Args:
            nodes (NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Node ids
            edges (EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Edge ids
            sources (ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence(View) None): Retrieve properties from the listed - by reference - views.
            include_typing (bool): Whether to return property type information as part of the result.

        Returns:
            InstancesResult: Requested instances.

        Examples:

            Retrieve instances by id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.instances.retrieve(nodes=("mySpace", "myNodeExternalId"),
                ...                                          edges=("mySpace", "myEdgeExternalId"),
                ...                                          sources=("mySpace", "myViewExternalId", "myViewVersion")
                ...                                         )

            Retrieve nodes an edges using the built in data class

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId, EdgeId, ViewId
                >>> c = CogniteClient()
                >>> res = c.data_modeling.instances.retrieve(NodeId("mySpace", "myNode"),
                ...                                          EdgeId("mySpace", "myEdge"),
                ...                                          ViewId("mySpace", "myViewExternalId", "myViewVersion")
                ...                                         )

            Retrieve nodes an edges using the the view object as source

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId, EdgeId
                >>> c = CogniteClient()
                >>> res = c.data_modeling.instances.retrieve(NodeId("mySpace", "myNode"),
                ...                                          EdgeId("mySpace", "myEdge"),
                ...                                          sources=("myspace", "myView")
                ...                                         )
        """
        identifiers = self._load_node_and_edge_ids(nodes, edges)
        other_params = self._create_other_params(
            include_typing=include_typing,
            sources=sources,
            sort=None,
            instance_type=None,
        )

        res = self._retrieve_multiple(
            list_cls=_NodeOrEdgeList,
            resource_cls=_NodeOrEdgeResourceAdapter,  # type: ignore[type-var]
            identifiers=identifiers,
            other_params=other_params,
            executor=get_data_modeling_executor(),
        )

        return InstancesResult(
            nodes=NodeList([node for node in res if isinstance(node, Node)]),
            edges=EdgeList([edge for edge in res if isinstance(edge, Edge)]),
        )

    def _load_node_and_edge_ids(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None,
    ) -> DataModelingIdentifierSequence:
        nodes_seq: Sequence[NodeId | tuple[str, str]]
        if isinstance(nodes, NodeId) or (isinstance(nodes, tuple) and isinstance(nodes[0], str)):
            nodes_seq = [nodes]  # type: ignore[list-item]
        else:
            nodes_seq = nodes  # type: ignore[assignment]

        edges_seq: Sequence[EdgeId | tuple[str, str]]
        if isinstance(edges, EdgeId) or (isinstance(edges, tuple) and isinstance(edges[0], str)):
            edges_seq = [edges]  # type: ignore[list-item]
        else:
            edges_seq = edges  # type: ignore[assignment]

        identifiers = []
        if nodes_seq:
            node_ids = _load_identifier(nodes_seq, "node")
            identifiers.extend(node_ids._identifiers)
        if edges_seq:
            edge_ids = _load_identifier(edges_seq, "edge")
            identifiers.extend(edge_ids._identifiers)

        return DataModelingIdentifierSequence(identifiers, is_singleton=False)

    def delete(
        self,
        nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
    ) -> InstancesDeleteResult:
        """`Delete one or more instances <https://developer.cognite.com/api#tag/Instances/tag/Instances/operation/deleteBulk>`_

        Args:
            nodes (NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Node ids
            edges (EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None): Edge ids

        Returns:
            The instance(s) which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete instances by id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.data_modeling.instances.delete(nodes=("mySpace", "myNode"))

            Delete nodes and edges using the built in data class

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId, EdgeId
                >>> c = CogniteClient()
                >>> c.data_modeling.instances.delete(NodeId('mySpace', 'myNode'), EdgeId('mySpace', 'myEdge'))

            Delete all nodes from a NodeList

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId, EdgeId
                >>> c = CogniteClient()
                >>> my_view = c.data_modeling.views.retrieve('mySpace', 'myView')
                >>> my_nodes = c.data_modeling.instances.list(instance_type='node', sources=my_view, limit=None)
                >>> c.data_modeling.instances.delete(nodes=my_nodes.as_ids())
        """
        identifiers = self._load_node_and_edge_ids(nodes, edges)
        deleted_instances = cast(
            List,
            self._delete_multiple(
                identifiers, wrap_ids=True, returns_items=True, executor=get_data_modeling_executor()
            ),
        )
        node_ids = [NodeId.load(item) for item in deleted_instances if item["instanceType"] == "node"]
        edge_ids = [EdgeId.load(item) for item in deleted_instances if item["instanceType"] == "edge"]
        return InstancesDeleteResult(node_ids, edge_ids)

    @classmethod
    def _create_other_params(
        cls,
        *,
        include_typing: bool,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None,
        instance_type: Literal["node", "edge"] | None,
    ) -> dict[str, Any]:
        other_params: dict[str, Any] = {"includeTyping": include_typing}
        if sources:
            other_params["sources"] = (
                [cls._dump_instance_source(source) for source in sources]
                if isinstance(sources, Sequence)
                else [cls._dump_instance_source(sources)]
            )
        if sort:
            if isinstance(sort, (InstanceSort, dict)):
                other_params["sort"] = [cls._dump_instance_sort(sort)]
            else:
                other_params["sort"] = [cls._dump_instance_sort(s) for s in sort]
        if instance_type:
            other_params["instanceType"] = instance_type
        return other_params

    @classmethod
    def _dump_instance_source(cls, source: ViewIdentifier | View) -> dict:
        instance_source: ViewIdentifier
        if isinstance(source, View):
            instance_source = source.as_id()
        else:
            instance_source = source
        return {"source": ViewId.load(instance_source).dump(camel_case=True)}

    @classmethod
    def _dump_instance_sort(cls, sort: InstanceSort | dict) -> dict:
        return sort.dump(camel_case=True) if isinstance(sort, InstanceSort) else sort

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
        """`Add or update (upsert) instances. <https://developer.cognite.com/api#tag/Instances/tag/Instances/operation/applyNodeAndEdges>`_

        Args:
            nodes (NodeApply | Sequence[NodeApply] | None = None): Nodes to apply
            edges (EdgeApply | Sequence[EdgeApply] | None = None): Edges to apply
            auto_create_start_nodes (bool): Whether to create missing start nodes for edges when ingesting. By default,
                                            the start node of an edge must exist before it can be ingestested.
            auto_create_end_nodes (bool): Whether to create missing end nodes for edges when ingesting. By default,
                                          the end node of an edge must exist before it can be ingestested.
            auto_create_direct_relations (bool): Whether to create missing direct relation targets when ingesting.
            skip_on_version_conflict (bool): If existingVersion is specified on any of the nodes/edges in the input,
                                             the default behaviour is that the entire ingestion will fail when version
                                             conflicts occur. If skipOnVersionConflict is set to true, items with
                                             version conflicts will be skipped instead. If no version is specified for
                                             nodes/edges, it will do the writing directly.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing
                            values with the supplied values (true)? Or should we merge in new values for properties
                            together with the existing values (false)? Note: This setting applies for all nodes or
                            edges specified in the ingestion call.
        Returns:
            InstancesApplyResult: Created instance(s)

        Examples:

            Create new node without data:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import EdgeApply, NodeOrEdgeData, NodeApply
                >>> c = CogniteClient()
                >>> nodes = [NodeApply("mySpace", "myNodeId")]
                >>> res = c.data_modeling.instances.apply(nodes)

            Create two nodes with data with an one to many edge, and a one to one edge

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import EdgeApply, NodeOrEdgeData, NodeApply, ViewId
                >>> person = NodeApply("mySpace", "person:arnold_schwarzenegger", sources=[
                ...                        NodeOrEdgeData(
                ...                               ViewId("mySpace", "PersonView", "v1"),
                ...                               {"name": "Arnold Schwarzenegger", "birthYear": 1947})
                ... ])
                >>> actor = NodeApply("mySpace", "actor:arnold_schwarzenegger", sources=[
                ...                        NodeOrEdgeData(
                ...                               ViewId("mySpace", "ActorView", "v1"),
                ...                               {"wonOscar": False,
                ...                               # This is a one-to-one edge from actor to person
                ...                                "person": {"space": "mySpace", "externalId": "person:arnold_schwarzenegger"}})
                ... ])
                >>> # This is one to many edge, in this case from Person to role
                >>> # (a person can have multiple roles, in this model for example Actor and Director)
                >>> person_to_actor = EdgeApply(space="mySpace",
                ...                                       external_id="relation:arnold_schwarzenegger:actor",
                ...                                       type=("Person", "roles"),
                ...                                       start_node=("person", "arnold_schwarzenegger"),
                ...                                       end_node=("actor", "arnold_schwarzenegger"),
                ... )
                >>> res = c.data_modeling.instances.apply([person, actor], [person_to_actor])

            Create new edge an automatically create end nodes.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import EdgeApply
                >>> c = CogniteClient()
                >>> edge = EdgeApply(space="mySpace",
                ...                            external_id="relation:sylvester_stallone:actor",
                ...                            type=("Person", "roles"),
                ...                            start_node=("person", "sylvester_stallone"),
                ...                            end_node=("actor", "sylvester_stallone"),
                ... )
                >>> res = c.data_modeling.instances.apply(edges=edge, auto_create_start_nodes=True, auto_create_end_nodes=True)

        """
        other_parameters = {
            "autoCreateStartNodes": auto_create_start_nodes,
            "autoCreateEndNodes": auto_create_end_nodes,
            "autoCreateDirectRelations": auto_create_direct_relations,
            "skipOnVersionConflict": skip_on_version_conflict,
            "replace": replace,
        }
        nodes = nodes or []
        nodes = nodes if isinstance(nodes, Sequence) else [nodes]

        edges = edges or []
        edges = edges if isinstance(edges, Sequence) else [edges]

        res = self._create_multiple(
            items=(*nodes, *edges),
            list_cls=_NodeOrEdgeApplyResultList,
            resource_cls=_NodeOrEdgeApplyResultAdapter,  # type: ignore[type-var]
            extra_body_fields=other_parameters,
            input_resource_cls=_NodeOrEdgeApplyAdapter,  # type: ignore[arg-type]
            executor=get_data_modeling_executor(),
        )
        return InstancesApplyResult(
            nodes=NodeApplyResultList([item for item in res if isinstance(item, NodeApplyResult)]),
            edges=EdgeApplyResultList([item for item in res if isinstance(item, EdgeApplyResult)]),
        )

    @overload
    def search(
        self,
        view: ViewId,
        query: str,
        instance_type: Literal["node"] = "node",
        properties: list[str] | None = None,
        filter: Filter | dict | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
    ) -> NodeList:
        ...

    @overload
    def search(
        self,
        view: ViewId,
        query: str,
        instance_type: Literal["edge"],
        properties: list[str] | None = None,
        filter: Filter | dict | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
    ) -> EdgeList:
        ...

    def search(
        self,
        view: ViewId,
        query: str,
        instance_type: Literal["node", "edge"] = "node",
        properties: list[str] | None = None,
        filter: Filter | dict | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
    ) -> NodeList | EdgeList:
        """`Search instances <https://developer.cognite.com/api/v1/#tag/Instances/operation/searchInstances>`_

        Args:
            view (ViewId): View to search in.
            query (str): Query string that will be parsed and used for search.
            instance_type (Literal["node", "edge"]): Whether to search for nodes or edges.
            properties (list[str]): Optional array of properties you want to search through.
                                    If you do not specify one or more properties, the service will
                                    search all text fields within the view.
            filter (dict | Filter): Advnanced filtering of instances.
            limit (int, optional): Maximum number of instances to return. Default to 1000. Set to -1, float("inf") or None
                to return all items.

        Returns:
            EdgeList | NodeList: Search result with matching nodes or edges.

        Examples:

            Search for Arnold in the person view in the name property:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> c = CogniteClient()
                >>> res = c.data_modeling.instances.search(ViewId("mySpace", "PersonView", "v1"), query="Arnold", properties=["name"])

            Search for Quentin in the person view in the name property, but only born before 1970:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> import cognite.client.data_classes.data_modeling.filters as filters
                >>> c = CogniteClient()
                >>> born_after_1970 = filters.Range(["mySpace", "PersonView/v1", "birthYear"], gt=1970)
                >>> res = c.data_modeling.instances.search(ViewId("mySpace", "PersonView", "v1"),
                ... query="Quentin", properties=["name"], filter=born_after_1970)

        """
        if instance_type == "node":
            list_cls: Union[Type[NodeList], Type[EdgeList]] = NodeList
        elif instance_type == "edge":
            list_cls = EdgeList
        else:
            raise ValueError(f"Invalid instance type: {instance_type}")

        body = {"view": view.dump(camel_case=True), "query": query, "instanceType": instance_type, "limit": limit}
        if properties:
            body["properties"] = properties
        if filter:
            body["filter"] = filter.dump() if isinstance(filter, Filter) else filter

        res = self._post(url_path=self._RESOURCE_PATH + "/search", json=body)
        return list_cls._load(res.json()["items"], cognite_client=None)

    def aggregate(
        self,
        view: ViewId,
        aggregates: MetricAggregation | dict | Sequence[MetricAggregation | dict],
        instance_type: Literal["node", "edge"] = "node",
        group_by: Sequence[str] | None = None,
        query: str | None = None,
        properties: Sequence[str] | None = None,
        filter: Filter | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
    ) -> InstanceAggregationResultList:
        """`Aggregate data across nodes/edges <https://developer.cognite.com/api/v1/#tag/Instances/operation/aggregateInstances>`_

        Args:
            view (ViewId): View to to aggregate over.
            aggregates (MetricAggregation | dict | Sequence[MetricAggregation | dict]):  The properties to aggregate over.
            instance_type (Literal["node", "edge"]): Whether to search for nodes or edges.
            group_by (Optional[Sequence[str]]): The selection of fields to group the results by when doing aggregations.
                                  You can specify up to 5 items to group by.
            query (Optional[str]): Query string that will be parsed and used for search.
            properties (Optional[Sequence[str]]): Optional array of properties you want to search through.
                                    If you do not specify one or more properties, the service will
                                    search all text fields within the view.
            filter (Optional[Filter]): Advnanced filtering of instances.
            limit (int, optional): Maximum number of instances to return. Default to 1000. Set to -1, float("inf") or None
                to return all items.

        Returns:
            InstanceAggregationResultList: Node or edge aggregation results.

        Examples:

            Get the average run time in minutes for movies grouped by release year:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import ViewId, aggregations as aggs
                >>> c = CogniteClient()
                >>> avg_run_time = aggs.Avg("runTimeMinutes")
                >>> view_id = ViewId("mySpace", "PersonView", "v1")
                >>> res = c.data_modeling.instances.aggregate(view_id, [avg_run_time], group_by=["releaseYear"])

        """
        if instance_type not in ("node", "edge"):
            raise ValueError(f"Invalid instance type: {instance_type}")
        body: Dict[str, Any] = {"view": view.dump(camel_case=True), "instanceType": instance_type, "limit": limit}
        aggregate_seq: Sequence[Aggregation | dict] = aggregates if isinstance(aggregates, Sequence) else [aggregates]
        body["aggregates"] = [
            agg.dump(camel_case=True) if isinstance(agg, Aggregation) else agg for agg in aggregate_seq
        ]
        if group_by:
            body["groupBy"] = group_by
        if filter:
            body["filter"] = filter.dump() if isinstance(filter, Filter) else filter
        if query:
            body["query"] = query
        if properties:
            body["properties"] = properties

        res = self._post(url_path=self._RESOURCE_PATH + "/aggregate", json=body)
        return InstanceAggregationResultList._load(res.json()["items"], cognite_client=None)

    @overload
    def histogram(
        self,
        view: ViewId,
        histograms: Histogram,
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: Sequence[str] | None = None,
        filter: Filter | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
    ) -> HistogramValue:
        ...

    @overload
    def histogram(
        self,
        view: ViewId,
        histograms: Sequence[Histogram],
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: Sequence[str] | None = None,
        filter: Filter | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
    ) -> list[HistogramValue]:
        ...

    def histogram(
        self,
        view: ViewId,
        histograms: Histogram | Sequence[Histogram],
        instance_type: Literal["node", "edge"] = "node",
        query: str | None = None,
        properties: Sequence[str] | None = None,
        filter: Filter | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
    ) -> HistogramValue | list[HistogramValue]:
        """`Produces histograms for nodes/edges <https://developer.cognite.com/api/v1/#tag/Instances/operation/aggregateInstances>`_

        Args:
            view (ViewId): View to to aggregate over.
            histograms (Histogram | Sequence[Histogram]):  The properties to aggregate over.
            instance_type (Literal["node", "edge"]): Whether to search for nodes or edges.
            query (Optional[str]): Query string that will be parsed and used for search.
            properties (Optional[Sequence[str]]): Optional array of properties you want to search through.
                                    If you do not specify one or more properties, the service will
                                    search all text fields within the view.
            filter (Optional[Filter]): Advnanced filtering of instances.
            limit (int, optional): Maximum number of instances to return. Default to 1000. Set to -1, float("inf") or None
                to return all items.

        Returns:
            list[HistogramValue]: Node or edge aggregation results.

        Examples:

            Find the number of people born per decade:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import aggregations as aggs, ViewId
                >>> c = CogniteClient()
                >>> birth_by_decade = aggs.Histogram("birthYear", interval=10.0)
                >>> view_id = ViewId("mySpace", "PersonView", "v1")
                >>> res = c.data_modeling.instances.histogram(view_id, birth_by_decade)
        """
        if instance_type not in ("node", "edge"):
            raise ValueError(f"Invalid instance type: {instance_type}")
        body: Dict[str, Any] = {"view": view.dump(camel_case=True), "instanceType": instance_type, "limit": limit}

        if isinstance(histograms, Sequence):
            histogram_seq: Sequence[Histogram] = histograms
            is_singleton = False
        elif isinstance(histograms, Histogram):
            histogram_seq = [histograms]
            is_singleton = True
        else:
            raise TypeError(f"Expected Histogram or sequence of Histograms, got {type(histograms)}")

        for histogram in histogram_seq:
            if not isinstance(histogram, Histogram):
                raise ValueError(f"Not a histogram: {histogram}")

        body["aggregates"] = [histogram.dump(camel_case=True) for histogram in histogram_seq]
        if filter:
            body["filter"] = filter.dump() if isinstance(filter, Filter) else filter
        if query:
            body["query"] = query
        if properties:
            body["properties"] = properties

        res = self._post(url_path=self._RESOURCE_PATH + "/aggregate", json=body)
        if is_singleton:
            return HistogramValue.load(res.json()["items"][0]["aggregates"][0])
        else:
            return [HistogramValue.load(item["aggregates"][0]) for item in res.json()["items"]]

    def query(self, query: Query) -> QueryResult:
        """`Advanced query interface for nodes/edges. <https://developer.cognite.com/api/v1/#tag/Instances/operation/queryContent>`_

        The Data Modelling API exposes an advanced query interface. The query interface supports parameterization,
        recursive edge traversal, chaining of result sets, and granular property selection.

        Args:
            query: Query.

        Returns:
            QueryResult: The resulting nodes and/or edges from the query.

        Examples:

            Find actors in movies released before 2000 sorted by actor name:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.query import Query, Select, NodeResultSetExpression, EdgeResultSetExpression, SourceSelector
                >>> from cognite.client.data_classes.data_modeling.filters import Range, Equals
                >>> from cognite.client.data_classes.data_modeling.ids import ViewId
                >>> c = CogniteClient()
                >>> movie_id = ViewId("mySpace", "MovieView", "v1")
                >>> actor_id = ViewId("mySpace", "ActorView", "v1")
                >>> query = Query(
                ...         with_ = {
                ...             "movies": NodeResultSetExpression(filter=Range(movie_id.as_property_ref("releaseYear"), lt=2000)),
                ...             "actors_in_movie": EdgeResultSetExpression(from_="movies", filter=Equals(["edge", "type"], {"space": movie_id.space, "externalId": "Movie.actors"})),
                ...             "actors": NodeResultSetExpression(from_="actors_in_movie"),
                ...         },
                ...         select = {
                ...             "actors": Select(
                ...                            [SourceSelector(actor_id, ["name"])], sort=[actor_id.as_property_ref("name")]),
                ...         },
                ... )
                >>> res = c.data_modeling.instances.query(query)
        """
        return self._query_or_sync(query, "query")

    def sync(self, query: Query) -> QueryResult:
        """`Subscription to changes for nodes/edges. <https://developer.cognite.com/api/v1/#tag/Instances/operation/syncContent>`_

        Subscribe to changes for nodes and edges in a project, matching a supplied filter.

        Args:
            query: Query.

        Returns:
            QueryResult: The resulting nodes and/or edges from the query.

        Examples:

            Find actors in movies released before 2000 sorted by actor name:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.query import Query, Select, NodeResultSetExpression, EdgeResultSetExpression, SourceSelector
                >>> from cognite.client.data_classes.data_modeling.filters import Range, Equals
                >>> from cognite.client.data_classes.data_modeling.ids import ViewId
                >>> c = CogniteClient()
                >>> movie_id = ViewId("mySpace", "MovieView", "v1")
                >>> actor_id = ViewId("mySpace", "ActorView", "v1")
                >>> query = Query(
                ...         with_ = {
                ...             "movies": NodeResultSetExpression(filter=Range(movie_id.as_property_ref("releaseYear"), lt=2000)),
                ...             "actors_in_movie": EdgeResultSetExpression(from_="movies", filter=Equals(["edge", "type"], {"space": movie_id.space, "externalId": "Movie.actors"})),
                ...             "actors": NodeResultSetExpression(from_="actors_in_movie"),
                ...         },
                ...         select = {
                ...             "actors": Select(
                ...                            [SourceSelector(actor_id, ["name"])], sort=[actor_id.as_property_ref("name")]),
                ...         },
                ... )
                >>> res = c.data_modeling.instances.sync(query)
                >>> # Added a new movie with actors released before 2000
                >>> query.cursors = res.cursors
                >>> res_new = c.data_modeling.instances.sync(query)

            In the last example, the res_new will only contain the actors that have been added with the new movie.
        """
        return self._query_or_sync(query, "sync")

    def _query_or_sync(self, query: Query, endpoint: Literal["query", "sync"]) -> QueryResult:
        body = query.dump(camel_case=True)

        result = self._post(url_path=self._RESOURCE_PATH + f"/{endpoint}", json=body)

        json_payload = result.json()
        default_by_reference = query.instance_type_by_result_expression()
        results = QueryResult.load(json_payload["items"], default_by_reference, json_payload["nextCursor"])

        return results

    @overload
    def list(
        self,
        instance_type: Literal["node"] = "node",
        include_typing: bool = False,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> NodeList:
        ...

    @overload
    def list(
        self,
        instance_type: Literal["edge"],
        include_typing: bool = False,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> EdgeList:
        ...

    def list(
        self,
        instance_type: Literal["node", "edge"] = "node",
        include_typing: bool = False,
        sources: ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View] | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> NodeList | EdgeList:
        """`List instances <https://developer.cognite.com/api#tag/Instances/tag/Instances/operation/advancedListInstance>`_

        Args:
            instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
            include_typing (bool): Whether to return property type information as part of the result.
            sources (ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence(View) | None): Views to retrieve properties from.
            limit (int, optional): Maximum number of instances to return. Default to 1000. Set to -1, float("inf") or None
                to return all items.
            sort (Sequence[InstanceSost] | InstanceSort | dict): How you want the listed instances information ordered.
            filter (dict | Filter): Advanced filtering of instances.

        Returns:
            Union[EdgeList, NodeList]: List of requested instances

        Examples:

            List instances and limit to 5:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> instance_list = c.data_modeling.instances.list(limit=5)

            Iterate over instances:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for instance in c.data_modeling.instances:
                ...     instance # do something with the instance

            Iterate over chunks of instances to reduce memory load:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for instance_list in c.data_modeling.instances(chunk_size=100):
                ...     instance_list # do something with the instances
        """
        other_params = self._create_other_params(
            include_typing=include_typing, instance_type=instance_type, sort=sort, sources=sources
        )

        if instance_type == "node":
            resource_cls: type = _NodeOrEdgeResourceAdapter
            list_cls: type = NodeList
        elif instance_type == "edge":
            resource_cls, list_cls = _NodeOrEdgeResourceAdapter, EdgeList
        else:
            raise ValueError(f"Invalid instance type: {instance_type}")

        return cast(
            Union[NodeList, EdgeList],
            self._list(
                list_cls=list_cls,
                resource_cls=resource_cls,
                method="POST",
                limit=limit,
                filter=filter.dump() if isinstance(filter, Filter) else filter,
                other_params=other_params,
            ),
        )
