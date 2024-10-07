from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject, UnknownCogniteObject
from cognite.client.data_classes.data_modeling.ids import ContainerId, PropertyId, ViewId, ViewIdentifier
from cognite.client.data_classes.data_modeling.instances import (
    Edge,
    EdgeListWithCursor,
    InstanceSort,
    Node,
    NodeListWithCursor,
    PropertyValue,
    TargetUnit,
    TypeInformation,
)
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.data_classes.filters import Filter

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class SourceSelector(CogniteObject):
    source: ViewId
    properties: list[str] | None = None
    target_units: list[TargetUnit] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"source": self.source.dump(camel_case)}
        if self.properties is not None:
            output["properties"] = self.properties
        if self.target_units:
            output["targetUnits" if camel_case else "target_units"] = [
                unit.dump(camel_case) for unit in self.target_units
            ]
        return output

    @classmethod
    def _load(
        cls,
        resource: dict[str, Any] | SourceSelector | ViewIdentifier | View,
        cognite_client: CogniteClient | None = None,
    ) -> Self:
        if isinstance(resource, cls):
            return resource
        elif isinstance(resource, dict):
            if "source" in resource:
                view_id = ViewId.load(resource["source"])
            else:
                # This is in case only a ViewId is passed in.
                view_id = ViewId.load(resource)
            return cls(
                source=view_id,
                properties=resource.get("properties"),
                target_units=[TargetUnit.load(unit) for unit in resource.get("targetUnits", [])] or None,
            )

        if isinstance(resource, View):
            view_id = resource.as_id()
        else:
            view_id = ViewId.load(resource)  # type: ignore[arg-type]
        return cls(source=view_id)

    @classmethod
    def _load_list(
        cls, data: ViewIdentifier | View | SourceSelector | Sequence[ViewIdentifier | View | SourceSelector]
    ) -> list[SourceSelector]:
        if isinstance(data, (View, SourceSelector, ViewId)) or (
            isinstance(data, tuple) and 2 <= len(data) <= 3 and all(isinstance(v, str) for v in data)
        ):
            data = [data]

        return [cls._load(v) for v in data]  # type: ignore[arg-type]


@dataclass
class Select(CogniteObject):
    sources: list[SourceSelector] = field(default_factory=list)
    sort: list[InstanceSort] = field(default_factory=list)
    limit: int | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if self.sources:
            output["sources"] = [source.dump(camel_case) for source in self.sources]
        if self.sort:
            output["sort"] = [s.dump(camel_case) for s in self.sort]
        if self.limit:
            output["limit"] = self.limit
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            sources=[SourceSelector.load(source) for source in resource.get("sources", [])],
            sort=[InstanceSort.load(s) for s in resource.get("sort", [])],
            limit=resource.get("limit"),
        )


class Query(CogniteObject):
    r"""Query allows you to do advanced queries on the data model.

    Args:
        with_ (dict[str, ResultSetExpression]): A dictionary of result set expressions to use in the query. The keys are used to reference the result set expressions in the select and parameters.
        select (dict[str, Select]): A dictionary of select expressions to use in the query. The keys must match the keys in the with\_ dictionary. The select expressions define which properties to include in the result set.
        parameters (dict[str, PropertyValue] | None): Values in filters can be parameterised. Parameters are provided as part of the query object, and referenced in the filter itself.
        cursors (Mapping[str, str | None] | None): A dictionary of cursors to use in the query. These are for pagination purposes, for example, in the sync endpoint.
    """

    def __init__(
        self,
        with_: dict[str, ResultSetExpression],
        select: dict[str, Select],
        parameters: dict[str, PropertyValue] | None = None,
        cursors: Mapping[str, str | None] | None = None,
    ) -> None:
        if not_matching := set(select) - set(with_):
            raise ValueError(
                f"The select keys must match the with keys, the following are not matching: {not_matching}"
            )
        self.with_ = with_
        self.select = select
        self.parameters = parameters
        self.cursors = cursors or dict.fromkeys(select)

    def instance_type_by_result_expression(self) -> dict[str, type[NodeListWithCursor] | type[EdgeListWithCursor]]:
        return {
            k: NodeListWithCursor if isinstance(v, NodeResultSetExpression) else EdgeListWithCursor
            for k, v in self.with_.items()
        }

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "with": {k: v.dump(camel_case) for k, v in self.with_.items()},
            "select": {k: v.dump(camel_case) for k, v in self.select.items()},
        }
        if self.parameters:
            output["parameters"] = dict(self.parameters.items())
        if self.cursors:
            output["cursors"] = dict(self.cursors.items())
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        parameters = dict(resource["parameters"].items()) if "parameters" in resource else None
        cursors = dict(resource["cursors"].items()) if "cursors" in resource else None
        return cls(
            with_={k: ResultSetExpression.load(v) for k, v in resource["with"].items()},
            select={k: Select.load(v) for k, v in resource["select"].items()},
            parameters=parameters,
            cursors=cursors,
        )

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and self.dump() == other.dump()


class ResultSetExpression(CogniteObject, ABC):
    def __init__(
        self,
        from_: str | None,
        filter: Filter | None,
        limit: int | None,
        sort: list[InstanceSort] | None,
        direction: Literal["outwards", "inwards"] = "outwards",
        chain_to: Literal["destination", "source"] = "destination",
        skip_already_deleted: bool = True,
    ):
        self.from_ = from_
        self.filter = filter
        self.limit = limit
        self.sort = sort
        self.direction = direction
        self.chain_to = chain_to
        self.skip_already_deleted = skip_already_deleted

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]: ...

    @classmethod
    def _load(
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> NodeResultSetExpression | EdgeResultSetExpression:
        if "nodes" in resource:
            return NodeResultSetExpression._load(resource, cognite_client)
        elif "edges" in resource:
            return EdgeResultSetExpression._load(resource, cognite_client)
        else:
            return UnknownCogniteObject.load(resource)  # type: ignore[return-value]

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and self.dump() == other.dump()


class NodeResultSetExpression(ResultSetExpression):
    """Describes how to query for nodes in the data model.

    Args:
        from_ (str | None): Chain your result-expression based on this view.
        filter (Filter | None): Filter the result set based on this filter.
        sort (list[InstanceSort] | None): Sort the result set based on this list of sort criteria.
        limit (int | None): Limit the result set to this number of instances.
        through (list[str] | tuple[str, str, str] | PropertyId | None): Chain your result-expression through this container or view. The property must be a reference to a direct relation property. `from_` must be defined. The tuple must be on the form (space, container, property) or (space, view/version, property).
        direction (Literal['outwards', 'inwards']): The direction to use when traversing direct relations. Only applicable when through is specified.
        chain_to (Literal['destination', 'source']): Control which side of the edge to chain to. The chain_to option is only applicable if the result rexpression referenced in `from` contains edges. `source` will chain to start if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e `direction=inwards`, it will chain to end. `destination` (default) will chain to end if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e, `direction=inwards`, it will chain to start.
        skip_already_deleted (bool): If set to False, the API will return instances that have been soft deleted before sync was initiated. Soft deletes that happen after the sync is initiated and a cursor generated, are always included in the result. Soft deleted instances are identified by having deletedTime set.
    """

    def __init__(
        self,
        from_: str | None = None,
        filter: Filter | None = None,
        sort: list[InstanceSort] | None = None,
        limit: int | None = None,
        through: list[str] | tuple[str, str, str] | PropertyId | None = None,
        direction: Literal["outwards", "inwards"] = "outwards",
        chain_to: Literal["destination", "source"] = "destination",
        skip_already_deleted: bool = True,
    ) -> None:
        super().__init__(
            from_=from_,
            filter=filter,
            limit=limit,
            sort=sort,
            direction=direction,
            chain_to=chain_to,
            skip_already_deleted=skip_already_deleted,
        )
        self.through: PropertyId | None = self._init_through(through)

    def _init_through(self, through: list[str] | tuple[str, str, str] | PropertyId | None) -> PropertyId | None:
        if isinstance(through, PropertyId):
            return through
        if isinstance(through, (list, tuple)):
            if len(through) != 3:
                raise ValueError(
                    f"`through` must be on the form (space, container, property) or (space, view/version, property), was {self.through}"
                )
            # set source to ViewId if '/' in through[1] , else set it to ContainerId
            source: ViewId | ContainerId = (
                ViewId(space=through[0], external_id=through[1].split("/")[0], version=through[1].split("/")[1])
                if "/" in through[1]
                else ContainerId(space=through[0], external_id=through[1])
            )
            return PropertyId(source=source, property=through[2])
        return None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        query_node = resource["nodes"]
        through = query_node.get("through")
        return cls(
            from_=query_node.get("from"),
            filter=Filter.load(query_node["filter"]) if "filter" in query_node else None,
            chain_to=query_node.get("chainTo"),
            direction=query_node.get("direction"),
            through=PropertyId.load(through) if through is not None else None,
            sort=[InstanceSort.load(sort) for sort in resource.get("sort", [])],
            limit=resource.get("limit"),
            skip_already_deleted=resource.get("skipAlreadyDeleted", True),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        nodes: dict[str, Any] = {}
        if self.from_:
            nodes["from"] = self.from_
        if self.filter:
            nodes["filter"] = self.filter.dump()
        if self.through:
            nodes["through"] = self.through.dump(camel_case=camel_case)
        if self.chain_to:
            nodes["chainTo" if camel_case else "chain_to"] = self.chain_to
        if self.direction:
            nodes["direction"] = self.direction

        output: dict[str, Any] = {"nodes": nodes}
        if self.sort:
            output["sort"] = [s.dump(camel_case=camel_case) for s in self.sort]
        if self.limit:
            output["limit"] = self.limit
        if not self.skip_already_deleted:
            output["skipAlreadyDeleted" if camel_case else "skip_already_deleted"] = self.skip_already_deleted

        return output


class EdgeResultSetExpression(ResultSetExpression):
    """Describes how to query for edges in the data model.

    Args:
        from_ (str | None): Chain your result expression from this edge.
        max_distance (int | None): The largest - max - number of levels to traverse.
        direction (Literal['outwards', 'inwards']): The direction to use when traversing.
        filter (Filter | None): Filter the result set based on this filter.
        node_filter (Filter | None): Filter the result set based on this filter.
        termination_filter (Filter | None): Filter the result set based on this filter.
        limit_each (int | None): Limit the number of returned edges for each of the source nodes in the result set. The indicated uniform limit applies to the result set from the referenced from. limitEach only has meaning when you also specify maxDistance=1 and from.
        sort (list[InstanceSort] | None): Sort the result set based on this list of sort criteria.
        post_sort (list[InstanceSort] | None): Sort the result set based on this list of sort criteria.
        limit (int | None): Limit the result set to this number of instances.
        chain_to (Literal['destination', 'source']): Control which side of the edge to chain to. The chain_to option is only applicable if the result rexpression referenced in `from` contains edges. `source` will chain to start if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e `direction=inwards`, it will chain to end. `destination` (default) will chain to end if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e, `direction=inwards`, it will chain to start.
        skip_already_deleted (bool): If set to False, the API will return instances that have been soft deleted before sync was initiated. Soft deletes that happen after the sync is initiated and a cursor generated, are always included in the result. Soft deleted instances are identified by having deletedTime set.
    """

    def __init__(
        self,
        from_: str | None = None,
        max_distance: int | None = None,
        direction: Literal["outwards", "inwards"] = "outwards",
        filter: Filter | None = None,
        node_filter: Filter | None = None,
        termination_filter: Filter | None = None,
        limit_each: int | None = None,
        sort: list[InstanceSort] | None = None,
        post_sort: list[InstanceSort] | None = None,
        limit: int | None = None,
        chain_to: Literal["destination", "source"] = "destination",
        skip_already_deleted: bool = True,
    ) -> None:
        super().__init__(
            from_=from_,
            filter=filter,
            limit=limit,
            sort=sort,
            direction=direction,
            chain_to=chain_to,
            skip_already_deleted=skip_already_deleted,
        )
        self.max_distance = max_distance
        self.node_filter = node_filter
        self.termination_filter = termination_filter
        self.limit_each = limit_each
        self.post_sort = post_sort

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        query_edge = resource["edges"]
        term_flt = Filter.load(query_edge["terminationFilter"]) if "terminationFilter" in query_edge else None
        return cls(
            from_=query_edge.get("from"),
            max_distance=query_edge.get("maxDistance"),
            direction=query_edge.get("direction"),
            filter=Filter.load(query_edge["filter"]) if "filter" in query_edge else None,
            node_filter=Filter.load(query_edge["nodeFilter"]) if "nodeFilter" in query_edge else None,
            termination_filter=term_flt,
            limit_each=query_edge.get("limitEach"),
            chain_to=query_edge.get("chainTo"),
            sort=[InstanceSort.load(sort) for sort in resource.get("sort", [])],
            post_sort=[InstanceSort.load(sort) for sort in resource.get("postSort", [])],
            limit=resource.get("limit"),
            skip_already_deleted=resource.get("skipAlreadyDeleted", True),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        edges: dict[str, Any] = {}
        if self.from_:
            edges["from"] = self.from_
        if self.max_distance:
            edges["maxDistance" if camel_case else "max_distance"] = self.max_distance
        if self.direction:
            edges["direction"] = self.direction
        if self.filter:
            edges["filter"] = self.filter.dump()
        if self.node_filter:
            edges["nodeFilter" if camel_case else "node_filter"] = self.node_filter.dump()
        if self.termination_filter:
            edges["terminationFilter" if camel_case else "termination_filter"] = self.termination_filter.dump()
        if self.limit_each:
            edges["limitEach" if camel_case else "limit_each"] = self.limit_each
        if self.chain_to:
            edges["chainTo" if camel_case else "chain_to"] = self.chain_to

        output: dict[str, Any] = {"edges": edges}
        if self.sort:
            output["sort"] = [s.dump(camel_case=camel_case) for s in self.sort]
        if self.post_sort:
            output["postSort" if camel_case else "post_sort"] = [s.dump(camel_case=camel_case) for s in self.post_sort]
        if self.limit:
            output["limit"] = self.limit
        if not self.skip_already_deleted:
            output["skipAlreadyDeleted" if camel_case else "skip_already_deleted"] = self.skip_already_deleted
        return output


class QueryResult(UserDict):
    def __getitem__(self, item: str) -> NodeListWithCursor | EdgeListWithCursor:
        return super().__getitem__(item)

    @property
    def cursors(self) -> dict[str, str]:
        return {key: value.cursor for key, value in self.items()}

    @classmethod
    def load(
        cls,
        resource: dict[str, Any],
        instance_list_type_by_result_expression_name: dict[str, type[NodeListWithCursor] | type[EdgeListWithCursor]],
        cursors: dict[str, Any],
        typing: dict[str, Any] | None = None,
    ) -> QueryResult:
        instance = cls()
        typing_nodes = TypeInformation._load(typing["nodes"]) if typing and "nodes" in typing else None
        typing_edges = TypeInformation._load(typing["edges"]) if typing and "edges" in typing else None
        for key, values in resource.items():
            cursor = cursors.get(key)
            if not values:
                instance[key] = instance_list_type_by_result_expression_name[key]([], cursor)
            elif values[0]["instanceType"] == "node":
                instance[key] = NodeListWithCursor([Node._load(node) for node in values], cursor, typing_nodes)
            elif values[0]["instanceType"] == "edge":
                instance[key] = EdgeListWithCursor([Edge._load(edge) for edge in values], cursor, typing_edges)
            else:
                raise ValueError(f"Unexpected instance type {values[0].get('instanceType')}")

        return instance

    def get_nodes(self, result_expression: str) -> NodeListWithCursor:
        result = super().__getitem__(result_expression)
        if isinstance(result, NodeListWithCursor):
            return result
        raise RuntimeError(
            f"{result_expression} is not a {NodeListWithCursor.__name__}. Try {self.get_edges.__name__} instead."
        )

    def get_edges(self, result_expression: str) -> EdgeListWithCursor:
        result = super().__getitem__(result_expression)
        if isinstance(result, EdgeListWithCursor):
            return result
        raise RuntimeError(
            f"{result_expression} is not a {EdgeListWithCursor.__name__}. Try {self.get_nodes.__name__} instead."
        )


# class SetOperationResultSetExpression(ResultSetExpression):
#     ...
#
#
# class UnionAllTableExpression(SetOperationResultSetExpression):
#     def __init__(
#         self, union_all: list[QuerySetOperationTableExpression | str], except_: list[str] = None, limit: int = None
#     ):
#         self.union_all = union_all
#         self.except_ = except_
#         self.limit = limit
#
#
# class UnionTableExpression(SetOperationResultSetExpression):
#     def __init__(
#         self, union: list[QuerySetOperationTableExpression | str], except_: list[str] = None, limit: int = None
#     ):
#         self.union = union
#         self.except_ = except_
#         self.limit = limit
#
#
# class IntersectTableExpression(SetOperationResultSetExpression):
#     def __init__(
#         self, intersection: list[QuerySetOperationTableExpression | str], except_: list[str] = None, limit: int = None
#     ):
#         self.intersect = intersection
#         self.except_ = except_
#         self.limit = limit
