from __future__ import annotations

import json
from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

from cognite.client.data_classes.data_modeling.filters import Filter
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import InstanceSort


class Query(ABC):
    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def load(cls, query: dict[str, Any]) -> Query:
        if "sort" in query:
            sort = [InstanceSort(**sort) for sort in query["sort"]]
        else:
            sort = []

        if "nodes" in query:
            node = QueryNode.load(query["nodes"])
            return QueryNodeTableExpression(node, sort, query.get("limit"))
        elif "edges" in query:
            edge = QueryEdge.load(query["edges"])
            return QueryEdgeTableExpression(edge, sort, query.get("limit"))
        else:
            raise NotImplementedError(f"Unknown query type: {query}")

    def __eq__(self, other: Any) -> bool:
        return type(self) == type(other) and self.dump() == other.dump()


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


class QueryNodeTableExpression(Query):
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


class QueryEdgeTableExpression(Query):
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


class QuerySetOperationTableExpression(Query):
    ...


class QueryUnionAllTableExpression(QuerySetOperationTableExpression):
    def __init__(
        self, union_all: list[QuerySetOperationTableExpression | str], except_: list[str] = None, limit: int = None
    ):
        self.union_all = union_all
        self.except_ = except_
        self.limit = limit


class QueryUnionTableExpression(QuerySetOperationTableExpression):
    def __init__(
        self, union: list[QuerySetOperationTableExpression | str], except_: list[str] = None, limit: int = None
    ):
        self.union = union
        self.except_ = except_
        self.limit = limit


class QueryIntersectTableExpression(QuerySetOperationTableExpression):
    def __init__(
        self, intersection: list[QuerySetOperationTableExpression | str], except_: list[str] = None, limit: int = None
    ):
        self.intersect = intersection
        self.except_ = except_
        self.limit = limit
