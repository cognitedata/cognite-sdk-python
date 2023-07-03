from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Literal

from cognite.client.data_classes.data_modeling.filters import Filter
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import InstanceSort


class Query(ABC):
    _filter_name: str

    def dump(self) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def load(cls, query: dict[str, Any]) -> Query:
        raise NotImplementedError


@dataclass
class ViewPropertyReference:
    view: ViewId
    identifier: str


@dataclass
class QueryNode:
    from_: str
    through: ViewPropertyReference
    filter: Filter


@dataclass
class QueryEdge:
    from_: str
    max_distance: int
    direction: Literal["outwards", "inwards"]
    filter: Filter
    node_filter: Filter
    termination_filter: Filter
    limit_each: int


class QueryNodeTableExpression(Query):
    def __init__(self, sort: list[InstanceSort], limit: int, nodes: dict[str, QueryNode]):
        self.sort = sort
        self.limit = limit
        self.nodes = nodes


class QueryEdgeTableExpression(Query):
    def __init__(
        self, sort: list[InstanceSort], post_sort: list[InstanceSort], limit: int, edges: dict[str, QueryEdge]
    ):
        self.sort = sort
        self.post_sort = post_sort
        self.limit = limit
        self.edges = edges


class QuerySetOperationTableExpression(Query):
    ...


class QueryUnionAllTableExpression(QuerySetOperationTableExpression):
    def __init__(self, union_all: list[Query | str], except_: list[str], limit: int):
        self.union_all = union_all
        self.except_ = except_
        self.limit = limit


class QueryUnionTableExpression(QuerySetOperationTableExpression):
    def __init__(self, union: list[Query | str], except_: list[str], limit: int):
        self.union = union
        self.except_ = except_
        self.limit = limit


class QueryIntersectTableExpression(QuerySetOperationTableExpression):
    def __init__(self, intersection: list[Query | str], except_: list[str], limit: int):
        self.intersect = intersection
        self.except_ = except_
        self.limit = limit
