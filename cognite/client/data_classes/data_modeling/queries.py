from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Literal, Optional

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
    from_: Optional[str] = None
    through: Optional[ViewPropertyReference] = None
    filter: Optional[Filter] = None


@dataclass
class QueryEdge:
    from_: Optional[str] = None
    max_distance: Optional[int] = None
    direction: Literal["outwards", "inwards"] = "outwards"
    filter: Optional[Filter] = None
    node_filter: Optional[Filter] = None
    termination_filter: Optional[Filter] = None
    limit_each: Optional[int] = None


class QueryNodeTableExpression(Query):
    def __init__(self, nodes: dict[str, QueryNode], sort: list[InstanceSort] = None, limit: int = None):
        self.nodes = nodes
        self.sort = sort
        self.limit = limit


class QueryEdgeTableExpression(Query):
    def __init__(
        self,
        edges: dict[str, QueryEdge],
        sort: list[InstanceSort] = None,
        post_sort: list[InstanceSort] = None,
        limit: int = None,
    ):
        self.sort = sort
        self.post_sort = post_sort
        self.limit = limit
        self.edges = edges


class QuerySetOperationTableExpression(Query):
    ...


class QueryUnionAllTableExpression(QuerySetOperationTableExpression):
    def __init__(self, union_all: list[Query | str], except_: list[str] = None, limit: int = None):
        self.union_all = union_all
        self.except_ = except_
        self.limit = limit


class QueryUnionTableExpression(QuerySetOperationTableExpression):
    def __init__(self, union: list[Query | str], except_: list[str] = None, limit: int = None):
        self.union = union
        self.except_ = except_
        self.limit = limit


class QueryIntersectTableExpression(QuerySetOperationTableExpression):
    def __init__(self, intersection: list[Query | str], except_: list[str] = None, limit: int = None):
        self.intersect = intersection
        self.except_ = except_
        self.limit = limit
