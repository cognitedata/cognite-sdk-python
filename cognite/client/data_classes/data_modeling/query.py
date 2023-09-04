from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import UserDict
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, cast

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    EdgeList,
    EdgeListWithCursor,
    InstanceSort,
    NodeList,
    NodeListWithCursor,
    PropertyValue,
)
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._auxiliary import local_import


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
    sources: list[SourceSelector] = field(default_factory=list)
    sort: list[InstanceSort] = field(default_factory=list)
    limit: int | None = None

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: dict[str, Any] = {}
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
            sort=[InstanceSort.load(s) for s in data.get("sort", [])],
            limit=data.get("limit"),
        )


class Query:
    r"""Query allows you to do advanced queries on the data model.

    Args:
        with\_ (dict[str, ResultSetExpression]): No description.
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
        with_keys = set(with_)
        if not_matching := set(select) - with_keys:
            raise ValueError(
                f"The select keys must match the with keys, the following are not matching: {not_matching}"
            )

        self.with_ = with_
        self.select = select
        self.parameters = parameters
        self.cursors = cursors or {k: None for k in select}

    def instance_type_by_result_expression(self) -> dict[str, type[NodeList] | type[EdgeList]]:
        return {k: NodeList if isinstance(v, NodeResultSetExpression) else EdgeList for k, v in self.with_.items()}

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
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
    def load_yaml(cls, data: str) -> Query:
        yaml = cast(Any, local_import("yaml"))
        return cls.load(yaml.safe_load(data))

    @classmethod
    def load(cls, data: str | dict[str, Any]) -> Query:
        data = json.loads(data) if isinstance(data, str) else data

        if not (with_ := data.get("with")):
            raise ValueError("The query must contain a with key")

        loaded: dict[str, Any] = {"with_": {k: ResultSetExpression.load(v) for k, v in with_.items()}}
        if not (select := data.get("select")):
            raise ValueError("The query must contain a select key")
        loaded["select"] = {k: Select.load(v) for k, v in select.items()}

        if parameters := data.get("parameters"):
            loaded["parameters"] = dict(parameters.items())
        if cursors := data.get("cursors"):
            loaded["cursors"] = dict(cursors.items())
        return cls(**loaded)

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and self.dump() == other.dump()


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
            query_node = query["nodes"]
            node = {
                "from_": query_node.get("from"),
                "filter": Filter.load(query_node["filter"]) if "filter" in query_node else None,
            }
            return NodeResultSetExpression(sort=sort, limit=query.get("limit"), **node)
        elif "edges" in query:
            query_edge = query["edges"]
            edge = {
                "from_": query_edge.get("from"),
                "max_distance": query_edge.get("maxDistance"),
                "direction": query_edge.get("direction"),
                "filter": Filter.load(query_edge["filter"]) if "filter" in query_edge else None,
                "node_filter": Filter.load(query_edge["nodeFilter"]) if "nodeFilter" in query_edge else None,
                "termination_filter": Filter.load(query_edge["terminationFilter"])
                if "terminationFilter" in query_edge
                else None,
                "limit_each": query_edge.get("limitEach"),
            }
            return EdgeResultSetExpression(**edge, sort=sort, limit=query.get("limit"))
        else:
            raise NotImplementedError(f"Unknown query type: {query}")

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and self.dump() == other.dump()


class NodeResultSetExpression(ResultSetExpression):
    def __init__(
        self,
        from_: str | None = None,
        filter: Filter | None = None,
        sort: list[InstanceSort] | None = None,
        limit: int | None = None,
    ):
        self.from_ = from_
        self.filter = filter
        self.sort = sort
        self.limit = limit

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: dict[str, Any] = {"nodes": {}}
        nodes = output["nodes"]
        if self.from_:
            nodes["from"] = self.from_
        if self.filter:
            nodes["filter"] = self.filter.dump()

        if self.sort:
            output["sort"] = [s.dump(camel_case=camel_case) for s in self.sort]
        if self.limit:
            output["limit"] = self.limit

        return output


class EdgeResultSetExpression(ResultSetExpression):
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
    ):
        self.from_ = from_
        self.max_distance = max_distance
        self.direction = direction
        self.filter = filter
        self.node_filter = node_filter
        self.termination_filter = termination_filter
        self.limit_each = limit_each
        self.sort = sort
        self.post_sort = post_sort
        self.limit = limit

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: dict[str, Any] = {"edges": {}}
        edges = output["edges"]
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

        if self.sort:
            output["sort"] = [s.dump(camel_case=camel_case) for s in self.sort]
        if self.post_sort:
            output["postSort" if camel_case else "post_sort"] = [s.dump(camel_case=camel_case) for s in self.post_sort]
        if self.limit:
            output["limit"] = self.limit

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
        data: dict[str, Any] | str,
        default_by_reference: dict[str, type[NodeList] | type[EdgeList]],
        cursors: dict[str, Any] | None = None,
    ) -> QueryResult:
        data = json.loads(data) if isinstance(data, str) else data
        instance = cls()
        for key, values in data.items():
            if not values:
                instance[key] = default_by_reference[key]([])
            elif values[0].get("instanceType") == "node":
                instance[key] = NodeListWithCursor._load(values)
                if cursors:
                    instance[key].cursor = cursors.get(key)
            elif values[0].get("instanceType") == "edge":
                instance[key] = EdgeListWithCursor._load(values)
                if cursors:
                    instance[key].cursor = cursors.get(key)
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
