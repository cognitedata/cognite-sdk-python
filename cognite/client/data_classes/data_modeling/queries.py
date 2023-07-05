from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import UserDict
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Mapping, Optional, Type

from cognite.client.data_classes.data_modeling.filters import Filter
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    Edge,
    EdgeList,
    InstanceSort,
    Node,
    NodeList,
    PropertyValue,
)


@dataclass
class SourceSelector:
    source: ViewId
    properties: list[str]

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            "source": self.source.dump(camel_case),
            "properties": self.properties,
        }

    @classmethod
    def load(cls, data: dict | str) -> SourceSelector:
        data = json.loads(data) if isinstance(data, str) else data
        return cls(
            source=ViewId.load(data["source"]),
            properties=data["properties"],
        )


@dataclass
class Select:
    sources: list[SourceSelector] = field(default_factory=lambda: [])
    sort: list[InstanceSort] = field(default_factory=lambda: [])
    limit: Optional[int] = None

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: Dict[str, Any] = {}
        if self.sources:
            output["sources"] = [
                {"source": source.source.dump(camel_case), "properties": source.properties} for source in self.sources
            ]
        if self.sort:
            output["sort"] = [s.dump(camel_case) for s in self.sort]
        if self.limit:
            output["limit"] = self.limit
        return output

    @classmethod
    def load(cls, data: dict | str) -> Select:
        data = json.loads(data) if isinstance(data, str) else data
        return cls(
            sources=[SourceSelector.load(source) for source in data.get("sources", [])],
            sort=[InstanceSort._load(s) for s in data.get("sort", [])],
            limit=data.get("limit"),
        )


class Query:
    """
    Query allows you to do advanced queries on the data model.

    Args:
        with_ (dict[str, ResultSetExpression]): A dictionary of result set expressions to use in the query. The keys
                                                are used to reference the result set expressions in the select and
                                                parameters.
        select (dict[str, Select]): A dictionary of select expressions to use in the query. The keys must match the
                                    keys in the with_ dictionary. The select expressions define which properties to
                                    include in the result set.
        parameters (Optional[dict[str, PropertyValue]]): Values in filters can be parameterised. Parameters are provided
                                                         as part of the query object, and referenced in the filter itself.
        cursors (Optional[Mapping[str, Optional[str]]]): A dictionary of cursors to use in the query. These are for
                                                         pagination purposes, for example, in the sync endpoint.zs
    """

    def __init__(
        self,
        with_: dict[str, ResultSetExpression],
        select: dict[str, Select],
        parameters: Optional[dict[str, PropertyValue]] = None,
        cursors: Optional[Mapping[str, Optional[str]]] = None,
    ) -> None:
        with_keys = set(with_)
        if not_matching := set(select) - with_keys:
            raise ValueError(
                f"The select keys must match the with keys, the following are not matching: {not_matching}"
            )
        if parameters and (not_matching := set(parameters) - with_keys):
            raise ValueError(
                f"The parameters keys must match the with keys, the following are not matching: {not_matching}"
            )

        self.with_ = with_
        self.select = select
        self.parameters = parameters
        self.cursors = cursors or {k: None for k in select}

    def default_instance_list_by_reference(self) -> dict[str, Type[NodeList] | Type[EdgeList]]:
        return {k: NodeList if isinstance(v, NodeResultSetExpression) else EdgeList for k, v in self.with_.items()}

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: Dict[str, Any] = {
            "with": {k: v.dump(camel_case) for k, v in self.with_.items()},
            "select": {k: v.dump(camel_case) for k, v in self.select.items()},
        }
        if self.parameters:
            output["parameters"] = dict(self.parameters.items())
        if self.cursors:
            output["cursors"] = dict(self.cursors.items())
        return output

    @classmethod
    def load(cls, result_set_expression: dict[str, Any]) -> ResultSetExpression:
        if "sort" in result_set_expression:
            sort = [InstanceSort(**sort) for sort in result_set_expression["sort"]]
        else:
            sort = []

        if "nodes" in result_set_expression:
            node = QueryNode.load(result_set_expression["nodes"])
            return NodeResultSetExpression(node, sort, result_set_expression.get("limit"))
        elif "edges" in result_set_expression:
            edge = QueryEdge.load(result_set_expression["edges"])
            return EdgeSetExpression(edge, sort, result_set_expression.get("limit"))
        else:
            raise NotImplementedError(f"Unknown query type: {result_set_expression}")


@dataclass
class ViewPropertyReference:
    view: ViewId
    identifier: str

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "view": self.view.dump(camel_case, include_type=True),
            "identifier": self.identifier,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> ViewPropertyReference:
        return cls(
            view=ViewId.load(data["view"]),
            identifier=data["identifier"],
        )


@dataclass
class QueryNode:
    from_: Optional[str] = None
    through: Optional[ViewPropertyReference] = None
    filter: Optional[Filter] = None

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: Dict[str, Any] = {}
        if self.from_:
            output["from"] = self.from_
        if self.through:
            output["through"] = self.through.dump(camel_case=camel_case)
        if self.filter:
            output["filter"] = self.filter.dump()
        return output

    @classmethod
    def load(cls, data: str | dict[str, Any]) -> QueryNode:
        data = json.loads(data) if isinstance(data, str) else data
        return cls(
            from_=data.get("from"),
            through=ViewPropertyReference.load(data["through"]) if "through" in data else None,
            filter=Filter.load(data["filter"]) if "filter" in data else None,
        )


@dataclass
class QueryEdge:
    from_: Optional[str] = None
    max_distance: Optional[int] = None
    direction: Literal["outwards", "inwards"] = "outwards"
    filter: Optional[Filter] = None
    node_filter: Optional[Filter] = None
    termination_filter: Optional[Filter] = None
    limit_each: Optional[int] = None

    @classmethod
    def load(cls, data: str | dict) -> QueryEdge:
        data = json.loads(data) if isinstance(data, str) else data
        return cls(
            from_=data["from"],
            max_distance=data["maxDistance"],
            direction=data["direction"],
            filter=Filter.load(data["filter"]) if data["filter"] else None,
            node_filter=Filter.load(data["nodeFilter"]) if data["nodeFilter"] else None,
            termination_filter=Filter.load(data["terminationFilter"]) if data["terminationFilter"] else None,
            limit_each=data["limitEach"],
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: Dict[str, Any] = {}
        if self.from_:
            output["from"] = self.from_
        if self.max_distance:
            output["maxDistance" if camel_case else "max_distance"] = self.max_distance
        if self.direction:
            output["direction"] = self.direction
        if self.filter:
            output["filter"] = self.filter.dump()
        if self.node_filter:
            output["nodeFilter" if camel_case else "node_filter"] = self.node_filter.dump()
        if self.termination_filter:
            output["terminationFilter" if camel_case else "termination_filter"] = self.termination_filter.dump()
        if self.limit_each:
            output["limitEach" if camel_case else "limit_each"] = self.limit_each
        return output


class ResultSetExpression(ABC):
    @abstractmethod
    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        ...

    @classmethod
    def load(cls, query: dict[str, Any]) -> ResultSetExpression:
        if "sort" in query:
            sort = [InstanceSort(**sort) for sort in query["sort"]]
        else:
            sort = []

        if "nodes" in query:
            node = QueryNode.load(query["nodes"])
            return NodeResultSetExpression(node, sort, query.get("limit"))
        elif "edges" in query:
            edge = QueryEdge.load(query["edges"])
            return EdgeSetExpression(edge, sort, query.get("limit"))
        else:
            raise NotImplementedError(f"Unknown query type: {query}")

    def __eq__(self, other: Any) -> bool:
        return type(other) == type(self) and self.dump() == other.dump()


class NodeResultSetExpression(ResultSetExpression):
    def __init__(self, nodes: QueryNode, sort: list[InstanceSort] = None, limit: int = None):
        self.nodes = nodes
        self.sort = sort
        self.limit = limit

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: Dict[str, Any] = {"nodes": self.nodes.dump(camel_case=camel_case)}
        if self.sort:
            output["sort"] = [s.dump(camel_case=camel_case) for s in self.sort]
        if self.limit:
            output["limit"] = self.limit

        return output


class EdgeSetExpression(ResultSetExpression):
    def __init__(
        self,
        edges: QueryEdge,
        sort: list[InstanceSort] = None,
        post_sort: list[InstanceSort] = None,
        limit: int = None,
    ):
        self.sort = sort
        self.post_sort = post_sort
        self.limit = limit
        self.edges = edges

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: Dict[str, Any] = {"edges": self.edges.dump(camel_case=camel_case)}
        if self.sort:
            output["sort"] = [s.dump(camel_case=camel_case) for s in self.sort]
        if self.post_sort:
            output["postSort" if camel_case else "post_sort"] = [s.dump(camel_case=camel_case) for s in self.post_sort]
        if self.limit:
            output["limit"] = self.limit

        return output


class QueryResult(UserDict):
    def __getitem__(self, item: str) -> NodeList | EdgeList:
        return super().__getitem__(item)

    @property
    def cursors(self) -> dict[str, str]:
        return {key: value.cursor for key, value in self.items()}

    @classmethod
    def load(
        cls, data: dict[str, Any] | str, default_by_reference: dict[str, Type[ResultSetExpression]]
    ) -> QueryResult:
        data = json.loads(data) if isinstance(data, str) else data
        instance = cls()
        for key, values in data.items():
            if not values:
                instance[key] = default_by_reference[key]()
            elif values[0].get("instanceType") == "node":
                instance[key] = NodeList._load(values)
            elif values[0].get("instanceType") == "edge":
                instance[key] = EdgeList._load(values)
            else:
                raise ValueError(f"Unexpected instance type {values[0].get('instanceType')}")
        return instance

    def update_json(
        self, data: dict[str, Any] | str, default_by_reference: dict[str, Type[NodeList] | Type[EdgeList]]
    ) -> None:
        data = json.loads(data) if isinstance(data, str) else data
        for key, values in data.items():
            if key not in self:
                self[key] = default_by_reference[key]([])
            if isinstance(self[key], NodeList):
                self[key].extend([Node.load(v) for v in values])
            elif isinstance(self[key], EdgeList):
                self[key].extend([Edge.load(v) for v in values])
            else:
                raise ValueError(f"Unexpected instance type {type(self[key])}")

    def update_cursors(self, cursors: dict[str, Any] | None) -> None:
        cursors = cursors or {}
        for key, value in self.items():
            # The sync endpoint eventually returns None, so for that case we keep the last cursor.
            value.cursor = cursors.get(key, value.cursor)


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
