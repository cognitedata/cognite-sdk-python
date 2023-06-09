from __future__ import annotations

from typing import Iterator, List, Literal, Sequence, Type, Union, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT
from cognite.client.data_classes.data_modeling.filters import Filter
from cognite.client.data_classes.data_modeling.ids import (
    EdgeId,
    NodeId,
    ViewId,
    load_identifier,
)
from cognite.client.data_classes.data_modeling.instances import (
    Edge,
    EdgeApply,
    EdgeApplyResult,
    EdgeApplyResultList,
    EdgeList,
    InstanceFilter,
    InstanceSort,
    Node,
    NodeApply,
    NodeApplyResult,
    NodeApplyResultList,
    NodeEdgeApplyResult,
    NodeEdgeApplyResultList,
    NodeList,
)


class InstancesAPI(APIClient):
    _RESOURCE_PATH = "/models/instances"

    @overload
    def __call__(
        self,
        chunk_size: None,
        instance_type: Literal["node"],
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
        instance_type: Literal["node"],
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
            Instance | InstanceList: yields Instance one by one if chunk_size is not specified, else InstanceList objects.
        """
        other_params = self._create_other_params(include_typing, instance_type, sort, sources)

        resource_cls, list_cls = self._get_classes(instance_type)

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

    @overload
    def retrieve(
        self, ids: NodeId, sources: list[ViewId] | ViewId | None = None, include_typing: bool = False
    ) -> Node | None:
        ...

    @overload
    def retrieve(
        self, ids: Sequence[NodeId], sources: list[ViewId] | ViewId | None = None, include_typing: bool = False
    ) -> NodeList:
        ...

    @overload
    def retrieve(
        self, ids: EdgeId, sources: list[ViewId] | ViewId | None = None, include_typing: bool = False
    ) -> Edge | None:
        ...

    @overload
    def retrieve(
        self, ids: Sequence[EdgeId], sources: list[ViewId] | ViewId | None = None, include_typing: bool = False
    ) -> EdgeList:
        ...

    @overload
    def retrieve(
        self, ids: tuple[str, str, str], sources: list[ViewId] | ViewId | None = None, include_typing: bool = False
    ) -> Node | Edge:
        ...

    @overload
    def retrieve(
        self,
        ids: Sequence[tuple[str, str, str]],
        sources: list[ViewId] | ViewId | None = None,
        include_typing: bool = False,
    ) -> NodeList | EdgeList:
        ...

    def retrieve(
        self,
        ids: NodeId
        | EdgeId
        | Sequence[NodeId]
        | Sequence[EdgeId]
        | tuple[str, str, str]
        | Sequence[tuple[str, str, str]],
        sources: list[ViewId] | ViewId | None = None,
        include_typing: bool = False,
    ) -> Node | Edge | NodeList | EdgeList | None:
        """`Retrieve one or more instance by id(s). <https://docs.cognite.com/api/v1/#tag/Instances/operation/byExternalIdsInstances>`_
        Args:
            ids (InstanceId | Sequence[InstanceId]): Identifier for instance(s).
            sources (list[ViewId] | None): Retrieve properties from the listed - by reference - views.
            include_typing (bool): Whether to return property type information as part of the result.
        Returns:
            Optional[Instance]: Requested instance or None if it does not exist.
        Examples:
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.instances.retrieve(('node', 'mySpace', 'myNode'))
        """
        instance_type = self._get_instance_type(ids)
        if instance_type in ["node", "edge"]:
            instance_type = cast(Literal["node", "edge"], instance_type)
            identifier = load_identifier(ids, instance_type)
            other_params = self._create_other_params(include_typing, instance_type, sources=sources, sort=None)
            # The byids endpoint does not have instance_type
            other_params.pop("instanceType", None)

            resource_cls, list_cls = self._get_classes(instance_type)
            return cast(
                Union[Node, Edge, NodeList, EdgeList, None],
                self._retrieve_multiple(
                    list_cls=list_cls, resource_cls=resource_cls, identifiers=identifier, other_params=other_params
                ),
            )
        else:
            raise NotImplementedError

    @overload
    def delete(self, id: NodeId | Sequence[NodeId]) -> list[NodeId]:
        ...

    @overload
    def delete(self, id: EdgeId | Sequence[EdgeId]) -> list[EdgeId]:
        ...

    @overload
    def delete(self, id: tuple[str, str, str] | Sequence[tuple[str, str, str]]) -> list[NodeId] | list[EdgeId]:
        ...

    def delete(
        self,
        id: NodeId
        | EdgeId
        | Sequence[NodeId]
        | Sequence[EdgeId]
        | tuple[str, str, str]
        | Sequence[tuple[str, str, str]],
    ) -> list[NodeId] | list[EdgeId]:
        """`Delete one or more instances <https://docs.cognite.com/api/v1/#tag/Instances/operation/deleteBulk>`_
        Args:
            id (InstanceId | Sequence[InstanceId): The instance identifier(s).
        Returns:
            The instance(s) which has been deleted. Empty list if nothing was deleted.
        Examples:
            Delete instances by id::
                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.data_modeling.instances.delete(("node", "myNode", "mySpace"))
        """
        instance_type = self._get_instance_type(id)
        if instance_type in {"node", "edge"}:
            instance_type = cast(Literal["node", "edge"], instance_type)

            deleted_instances = cast(
                List,
                self._delete_multiple(
                    identifiers=load_identifier(id, instance_type), wrap_ids=True, returns_items=True
                ),
            )

            if instance_type == "node":
                return [NodeId(space=item["space"], external_id=item["externalId"]) for item in deleted_instances]
            elif instance_type == "edge":
                return [EdgeId(space=item["space"], external_id=item["externalId"]) for item in deleted_instances]

            raise ValueError(f"Invalid instance type {instance_type}")
        else:
            raise NotImplementedError

    @classmethod
    def _create_other_params(
        cls,
        include_typing: bool,
        instance_type: Literal["node", "edge"],
        sort: list[InstanceSort | dict] | InstanceSort | dict | None,
        sources: ViewId | Sequence[ViewId] | None,
    ) -> dict:
        if isinstance(sources, Sequence) or sources is None:
            other_params = InstanceFilter(include_typing, sources, instance_type).dump(camel_case=True)
        else:
            other_params = InstanceFilter(include_typing, [sources], instance_type).dump(camel_case=True)

        if sort:
            if isinstance(sort, (InstanceSort, dict)):
                other_params["sort"] = [cls._dump_instance_sort(sort)]
            else:
                other_params["sort"] = [cls._dump_instance_sort(s) for s in sort]
        return other_params

    @classmethod
    def _dump_instance_sort(cls, sort: InstanceSort | dict) -> dict:
        return sort.dump(camel_case=True) if isinstance(sort, InstanceSort) else sort

    @classmethod
    @overload
    def _get_classes(cls, instance_type: Literal["node"]) -> tuple[Type[Node], Type[NodeList]]:
        ...

    @classmethod
    @overload
    def _get_classes(cls, instance_type: Literal["edge"]) -> tuple[Type[Edge], Type[EdgeList]]:
        ...

    @classmethod
    def _get_classes(
        cls, instance_type: Literal["node", "edge", "mix"]
    ) -> tuple[Type[Node], Type[NodeList]] | tuple[Type[Edge], Type[EdgeList]] | tuple[None, None]:
        if instance_type == "node":
            return Node, NodeList
        elif instance_type == "edge":
            return Edge, EdgeList
        elif instance_type == "mix":
            return None, None
        raise ValueError(f"Unsupported {instance_type=}")

    @classmethod
    @overload
    def _get_apply_result_classes(
        cls, instance_type: Literal["node"]
    ) -> tuple[Type[NodeApplyResult], Type[NodeApplyResultList]]:
        ...

    @classmethod
    @overload
    def _get_apply_result_classes(
        cls, instance_type: Literal["edge"]
    ) -> tuple[Type[EdgeApplyResult], Type[EdgeApplyResultList]]:
        ...

    @classmethod
    @overload
    def _get_apply_result_classes(
        cls, instance_type: Literal["mix"]
    ) -> tuple[Type[NodeEdgeApplyResult], Type[NodeEdgeApplyResultList]]:
        ...

    @classmethod
    def _get_apply_result_classes(
        cls, instance_type: Literal["node", "edge", "mix"]
    ) -> tuple[Type[NodeApplyResult], Type[NodeApplyResultList]] | tuple[
        Type[EdgeApplyResult], Type[EdgeApplyResultList]
    ] | tuple[Type[NodeEdgeApplyResult], Type[NodeEdgeApplyResultList]]:
        if instance_type == "node":
            return NodeApplyResult, NodeApplyResultList
        elif instance_type == "edge":
            return EdgeApplyResult, EdgeApplyResultList
        elif instance_type == "mix":
            return NodeEdgeApplyResult, NodeEdgeApplyResultList
        raise ValueError(f"Unsupported {instance_type=}")

    @overload
    def apply(
        self,
        instance: NodeApply,
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> NodeApplyResult:
        ...

    @overload
    def apply(
        self,
        instance: EdgeApply,
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> EdgeApplyResult:
        ...

    @overload
    def apply(
        self,
        instance: Sequence[NodeApply | EdgeApply],
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> NodeEdgeApplyResultList:
        ...

    def apply(
        self,
        instance: NodeApply | EdgeApply | Sequence[NodeApply | EdgeApply],
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> NodeApplyResult | EdgeApplyResult | NodeEdgeApplyResultList:
        """`Add or update (upsert) instances. <https://docs.cognite.com/api/v1/#tag/Instances/operation/applyNodeAndEdges>`_

        Args:
            instance (instance: Instance | Sequence[Instance]): Instance or instances of instances to create or update.
            auto_create_start_nodes (bool): Whether to create missing start nodes for edges when ingesting. By default,
                                            the start node of an edge must exist before it can be ingestested.
            auto_create_end_nodes (bool): Whether to create missing end nodes for edges when ingesting. By default,
                                          the end node of an edge must exist before it can be ingestested.
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
            Node | NodeList | Edge | EdgeList: Created instance(s)

        Examples:

            Create new node:

                >>> from cognite.client import CogniteClient
                >>> import cognite.client.data_modeling as dm
                >>> c = CogniteClient()
                >>> nodes = [dm.ApplyNode("mySpace", "myNodeId")]
                >>> res = c.data_modeling.instances.apply(nodes)
        """
        other_parameters = {
            "autoCreateStartNodes": auto_create_start_nodes,
            "autoCreateEndNodes": auto_create_end_nodes,
            "skipOnVersionConflict": skip_on_version_conflict,
            "replace": replace,
        }
        instance_type = self._get_instance_type(instance)
        resource_cls, list_cls = self._get_apply_result_classes(instance_type)

        return cast(
            Union[NodeApplyResult, EdgeApplyResult, NodeEdgeApplyResultList],
            self._create_multiple(  # type: ignore[type-var]
                list_cls=list_cls, resource_cls=resource_cls, items=instance, extra_body_fields=other_parameters  # type: ignore[arg-type]
            ),
        )

    @classmethod
    def _get_instance_type(
        cls,
        instance: NodeApply
        | EdgeApply
        | Sequence
        | NodeId
        | EdgeId
        | tuple[str, str, str]
        | Sequence[tuple[str, str, str]],
    ) -> Literal["node", "edge", "mix"]:
        if isinstance(instance, (NodeApply, NodeId)):
            return "node"
        elif isinstance(instance, (EdgeApply, EdgeId)):
            return "edge"
        elif (
            isinstance(instance, Sequence)
            and len(instance) > 0
            and isinstance(instance[0], (NodeApply, NodeId, EdgeApply, EdgeId))
        ):
            return "mix"
        elif isinstance(instance, tuple) and len(instance) > 0 and isinstance(instance[0], str):
            return cast(Literal["node", "edge"], instance[0])
        elif isinstance(instance, tuple) and len(instance) > 0 and isinstance(instance[0], tuple):
            return "mix"
        raise ValueError(f"Unsupported {instance=}")

    @overload
    def list(
        self,
        instance_type: Literal["node"],
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> NodeList:
        ...

    @overload
    def list(
        self,
        instance_type: Literal["edge"],
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> EdgeList:
        ...

    def list(
        self,
        instance_type: Literal["node", "edge"] = "node",
        include_typing: bool = False,
        sources: list[ViewId] | ViewId | None = None,
        limit: int = INSTANCES_LIST_LIMIT_DEFAULT,
        sort: list[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict | None = None,
    ) -> NodeList | EdgeList:
        """`List instances <https://docs.cognite.com/api/v1/#tag/Instances/operation/advancedListInstance>`_

        Args:
            instance_type(Literal["node", "edge"]): Whether to query for nodes or edges.
            include_typing (bool): Whether to return property type information as part of the result.
            sources (list[ViewId] | ViewId): Views to retrieve properties from.
            limit (int, optional): Maximum number of instances to return. Default to 1000. Set to -1, float("inf") or None
                to return all items.
            sort (list[InstanceSost] | InstanceSort | dict): How you want the listed instances information ordered.
            filter (dict | Filter): Advnanced filtering of instances.

        Returns:
            InstanceList: List of requested instances

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
        other_params = self._create_other_params(include_typing, instance_type, sort, sources)

        resource_cls, list_cls = self._get_classes(instance_type)

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
