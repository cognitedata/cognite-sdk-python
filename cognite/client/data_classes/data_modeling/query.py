from __future__ import annotations

from abc import ABC
from collections import UserDict
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from typing_extensions import Never, Self, assert_never

from cognite.client.data_classes._base import CogniteResource, UnknownCogniteResource
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
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling.debug import DebugInfo


@dataclass
class SourceSelector(CogniteResource):
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
class SelectBase(CogniteResource, ABC):
    sources: list[SourceSelector] = field(default_factory=list)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {}
        if self.sources:
            output["sources"] = [source.dump(camel_case) for source in self.sources]
        return output


@dataclass
class SelectSync(SelectBase):
    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            sources=[SourceSelector.load(source) for source in resource.get("sources", [])],
        )


@dataclass
class Select(SelectBase):
    sort: list[InstanceSort] = field(default_factory=list)
    limit: int | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.sort:
            output["sort"] = [s.dump(camel_case) for s in self.sort]
        if self.limit:
            output["limit"] = self.limit
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            sources=[SourceSelector.load(source) for source in resource.get("sources", [])],
            sort=[InstanceSort.load(s) for s in resource.get("sort", [])],
            limit=resource.get("limit"),
        )


_T_ResultSetExpression = TypeVar("_T_ResultSetExpression", bound="ResultSetExpressionBase")
_T_Select = TypeVar("_T_Select", bound=SelectBase)


@dataclass
class QueryBase(CogniteResource, ABC, Generic[_T_ResultSetExpression, _T_Select]):
    """Abstract base class for normal queries and sync queries"""

    with_: MutableMapping[str, _T_ResultSetExpression]
    select: MutableMapping[str, _T_Select]
    parameters: Mapping[str, PropertyValue] = field(default_factory=dict)
    cursors: Mapping[str, str | None] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not_matching := set(self.select) - set(self.with_):
            raise ValueError(
                f"The select keys must match the with keys, the following are not matching: {not_matching}"
            )
        if not self.cursors:
            self.cursors = dict.fromkeys(self.select)

    def instance_type_by_result_expression(self) -> dict[str, type[NodeListWithCursor] | type[EdgeListWithCursor]]:
        return {
            k: NodeListWithCursor
            if isinstance(v, NodeResultSetExpression | NodeResultSetExpressionSync)
            else EdgeListWithCursor
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
    def _load_base(
        cls,
        resource: dict[str, Any],
        result_set_class: type[_T_ResultSetExpression],
        select_class: type[_T_Select],
    ) -> Self:
        parameters = dict(resource["parameters"].items()) if "parameters" in resource else {}
        cursors = dict(resource["cursors"].items()) if "cursors" in resource else {}
        return cls(
            with_={k: result_set_class._load(v) for k, v in resource["with"].items()},
            select={k: select_class._load(v) for k, v in resource["select"].items()},
            parameters=parameters,
            cursors=cursors,
        )


@dataclass
class Query(QueryBase["ResultSetExpression", Select]):
    r"""Query allows you to do advanced queries on the data model.

    Args:
        with_ (Mapping[str, ResultSetExpression]): A dictionary of result set expressions to use in the query. The keys are used to reference the result set expressions in the select and parameters.
        select (Mapping[str, Select]): A dictionary of select expressions to use in the query. The keys must match the keys in the with\_ dictionary. The select expressions define which properties to include in the result set.
        parameters (Mapping[str, PropertyValue] | None): Values in filters can be parameterised. Parameters are provided as part of the query object, and referenced in the filter itself.
        cursors (Mapping[str, str | None] | None): A dictionary of cursors to use in the query. These allow for pagination.
    """

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls._load_base(resource, ResultSetExpression, Select)


@dataclass
class QuerySync(QueryBase["ResultSetExpressionSync", SelectSync]):
    r"""Sync allows you to do subscribe to changes in instances.

    Args:
        with_ (MutableMapping[str, _T_ResultSetExpression]): A dictionary of result set expressions to use in the query. The keys are used to reference the result set expressions in the select and parameters.
        select (MutableMapping[str, _T_Select]): A dictionary of select expressions to use in the query. The keys must match the keys in the with\_ dictionary. The select expressions define which properties to include in the result set.
        parameters (Mapping[str, PropertyValue]): Values in filters can be parameterised. Parameters are provided as part of the query object, and referenced in the filter itself.
        cursors (Mapping[str, str | None]): A dictionary of cursors to use in the query. These allow for pagination.
        allow_expired_cursors_and_accept_missed_deletes (bool): Sync cursors expire after 3 days because soft-deleted instances are cleaned up after this grace period, so a client using a cursor older than that risks missing deletes. If set to True, the API will allow the use of expired cursors.
    """

    allow_expired_cursors_and_accept_missed_deletes: bool = False

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.allow_expired_cursors_and_accept_missed_deletes:
            key = (
                "allowExpiredCursorsAndAcceptMissedDeletes"
                if camel_case
                else "allow_expired_cursors_and_accept_missed_deletes"
            )
            output[key] = self.allow_expired_cursors_and_accept_missed_deletes
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        base = cls._load_base(resource, ResultSetExpressionSync, SelectSync)
        base.allow_expired_cursors_and_accept_missed_deletes = resource.get(
            "allowExpiredCursorsAndAcceptMissedDeletes", False
        )
        return base


@dataclass
class ResultSetExpressionBase(CogniteResource, ABC):
    @staticmethod
    def _load_sort(resource: dict[str, Any], name: str) -> list[InstanceSort]:
        return [InstanceSort.load(sort) for sort in resource.get(name, [])]

    @staticmethod
    def _init_through(through: list[str] | tuple[str, str, str] | PropertyId | None) -> PropertyId | None:
        def error() -> Never:
            raise ValueError(
                f"`through` must be on the form (space, container, property) or (space, view/version, property), was {through}"
            )

        match through:
            case None:
                return None
            case PropertyId():
                return through
            case [space, xid_maybe_version, prop]:
                # set source to ViewId if '/' in through[1] , else set it to ContainerId
                source: ViewId | ContainerId
                match xid_maybe_version.split("/"):
                    case [xid]:
                        source = ContainerId(space, xid)
                    case [xid, version]:
                        source = ViewId(space, xid, version)
                    case _:
                        error()
                return PropertyId(source=source, property=prop)
            case _:
                error()


@dataclass
class ResultSetExpression(ResultSetExpressionBase, ABC):
    @classmethod
    def _load(cls, resource: dict[str, Any]) -> ResultSetExpression:
        if "nodes" in resource:
            return NodeResultSetExpression._load(resource)
        elif "edges" in resource:
            return EdgeResultSetExpression._load(resource)
        elif "union" in resource or "unionAll" in resource or "intersection" in resource:
            return SetOperation._load(resource)
        else:
            return UnknownCogniteResource.load(resource)  # type: ignore[return-value]


@dataclass
class NodeOrEdgeResultSetExpression(ResultSetExpression, ABC):
    from_: str | None = None
    filter: Filter | None = None
    limit: int | None = None
    sort: list[InstanceSort] = field(default_factory=list)
    direction: Literal["outwards", "inwards"] = "outwards"
    chain_to: Literal["destination", "source"] = "destination"


@dataclass
class NodeResultSetExpression(NodeOrEdgeResultSetExpression):
    """Describes how to query for nodes in the data model.

    Args:
        from_ (str | None): Chain your result-expression based on this view.
        filter (Filter | None): Filter the result set based on this filter.
        sort (list[InstanceSort] | None): Sort the result set based on this list of sort criteria.
        limit (int | None): Limit the result set to this number of instances.
        through (list[str] | tuple[str, str, str] | PropertyId | None): Chain your result-expression through this container or view. The property must be a reference to a direct relation property. `from_` must be defined. The tuple must be on the form (space, container, property) or (space, view/version, property).
        direction (Literal['outwards', 'inwards']): The direction to use when traversing direct relations. Only applicable when through is specified.
        chain_to (Literal['destination', 'source']): Control which side of the edge to chain to. The chain_to option is only applicable if the result rexpression referenced in `from` contains edges. `source` will chain to start if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e `direction=inwards`, it will chain to end. `destination` (default) will chain to end if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e, `direction=inwards`, it will chain to start.
    """

    through: PropertyId | None = None

    # Overriding __init__ to be more liberal in what we accept for through
    def __init__(
        self,
        from_: str | None = None,
        filter: Filter | None = None,
        sort: list[InstanceSort] | None = None,
        limit: int | None = None,
        through: list[str] | tuple[str, str, str] | PropertyId | None = None,
        direction: Literal["outwards", "inwards"] = "outwards",
        chain_to: Literal["destination", "source"] = "destination",
    ) -> None:
        super().__init__(
            from_=from_, filter=filter, sort=sort or [], limit=limit, direction=direction, chain_to=chain_to
        )
        self.through = self._init_through(through)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        query_node = resource["nodes"]
        through = query_node.get("through")
        return cls(
            from_=query_node.get("from"),
            filter=Filter._load_if(query_node.get("filter")),
            chain_to=query_node.get("chainTo"),
            direction=query_node.get("direction"),
            through=PropertyId.load(through) if through is not None else None,
            sort=cls._load_sort(resource, "sort"),
            limit=resource.get("limit"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        nodes: dict[str, Any] = {}
        if self.from_:
            nodes["from"] = self.from_
        if self.filter is not None:
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

        return output


@dataclass
class EdgeResultSetExpression(NodeOrEdgeResultSetExpression):
    """Describes how to query for edges in the data model.

    Args:
        from_ (str | None): Chain your result expression from this edge.
        filter (Filter | None): Filter the result set based on this filter.
        limit (int | None): Limit the result set to this number of instances.
        sort (list[InstanceSort]): Sort the result set based on this list of sort criteria.
        direction (Literal['outwards', 'inwards']): The direction to use when traversing.
        chain_to (Literal['destination', 'source']): Control which side of the edge to chain to. The chain_to option is only applicable if the result rexpression referenced in `from` contains edges. `source` will chain to start if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e `direction=inwards`, it will chain to end. `destination` (default) will chain to end if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e, `direction=inwards`, it will chain to start.
        max_distance (int | None): The largest - max - number of levels to traverse.
        node_filter (Filter | None): Filter the result set based on this filter.
        termination_filter (Filter | None): Filter the result set based on this filter.
        limit_each (int | None): Limit the number of returned edges for each of the source nodes in the result set. The indicated uniform limit applies to the result set from the referenced from. limitEach only has meaning when you also specify maxDistance=1 and from.
        post_sort (list[InstanceSort]): Sort the result set based on this list of sort criteria.
    """

    max_distance: int | None = None
    node_filter: Filter | None = None
    termination_filter: Filter | None = None
    limit_each: int | None = None
    post_sort: list[InstanceSort] = field(default_factory=list)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        query_edge = resource["edges"]
        return cls(
            from_=query_edge.get("from"),
            max_distance=query_edge.get("maxDistance"),
            direction=query_edge.get("direction"),
            filter=Filter._load_if(query_edge.get("filter")),
            node_filter=Filter._load_if(query_edge.get("nodeFilter")),
            termination_filter=Filter._load_if(query_edge.get("terminationFilter")),
            limit_each=query_edge.get("limitEach"),
            chain_to=query_edge.get("chainTo"),
            sort=cls._load_sort(resource, "sort"),
            post_sort=cls._load_sort(resource, "postSort"),
            limit=resource.get("limit"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        edges: dict[str, Any] = {}
        if self.from_:
            edges["from"] = self.from_
        if self.max_distance:
            edges["maxDistance" if camel_case else "max_distance"] = self.max_distance
        if self.direction:
            edges["direction"] = self.direction
        if self.filter is not None:
            edges["filter"] = self.filter.dump()
        if self.node_filter is not None:
            edges["nodeFilter" if camel_case else "node_filter"] = self.node_filter.dump()
        if self.termination_filter is not None:
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
        return output


SyncMode = Literal["one_phase", "two_phase", "no_backfill"]


@dataclass
class ResultSetExpressionSync(ResultSetExpressionBase, ABC):
    from_: str | None = None
    filter: Filter | None = None
    limit: int | None = None
    direction: Literal["outwards", "inwards"] = "outwards"
    chain_to: Literal["destination", "source"] = "destination"
    skip_already_deleted: bool = True
    sync_mode: SyncMode | None = None
    backfill_sort: list[InstanceSort] = field(default_factory=list)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> ResultSetExpressionSync:
        if "nodes" in resource:
            return NodeResultSetExpressionSync._load(resource)
        elif "edges" in resource:
            return EdgeResultSetExpressionSync._load(resource)
        else:
            return UnknownCogniteResource.load(resource)  # type: ignore[return-value]

    @staticmethod
    def _load_sync_mode(sync_mode: str | None) -> SyncMode | None:
        match sync_mode:
            case None:
                return None
            case "onePhase":
                return "one_phase"
            case "twoPhase":
                return "two_phase"
            case "noBackfill":
                return "no_backfill"
            case _:
                raise ValueError(f"Invalid sync mode {sync_mode}")

    @staticmethod
    def _dump_sync_mode(sync_mode: SyncMode, camel_case: bool = True) -> str:
        match sync_mode:
            case "one_phase":
                return "onePhase" if camel_case else "one_phase"
            case "two_phase":
                return "twoPhase" if camel_case else "two_phase"
            case "no_backfill":
                return "noBackfill" if camel_case else "no_backfill"
            case _:
                assert_never(sync_mode)


@dataclass
class NodeResultSetExpressionSync(ResultSetExpressionSync):
    """Describes how to query for nodes in the data model.

    Args:
        from_ (str | None): Chain your result-expression based on this view.
        filter (Filter | None): Filter the result set based on this filter.
        limit (int | None): Limit the result set to this number of instances.
        through (list[str] | tuple[str, str, str] | PropertyId | None): Chain your result-expression through this container or view. The property must be a reference to a direct relation property. `from_` must be defined. The tuple must be on the form (space, container, property) or (space, view/version, property).
        direction (Literal['outwards', 'inwards']): The direction to use when traversing direct relations. Only applicable when through is specified.
        chain_to (Literal['destination', 'source']): Control which side of the edge to chain to. The chain_to option is only applicable if the result rexpression referenced in `from` contains edges. `source` will chain to start if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e `direction=inwards`, it will chain to end. `destination` (default) will chain to end if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e, `direction=inwards`, it will chain to start.
        skip_already_deleted (bool): If set to False, the API will return instances that have been soft deleted before sync was initiated. Soft deletes that happen after the sync is initiated and a cursor generated, are always included in the result. Soft deleted instances are identified by having deletedTime set.
        sync_mode (Literal['one_phase', 'two_phase', 'no_backfill'] | None): Specify whether to sync instances in a single phase; in a backfill phase followed by live updates, or without any backfill. Only valid for sync operations.
        backfill_sort (list[InstanceSort] | None): Sort the result set during the backfill phase of a two phase sync. Only valid with sync_mode = "two_phase". The sort must be backed by a cursorable index.
    """

    through: PropertyId | None = None

    # Overriding __init__ to be more liberal in what we accept for through
    def __init__(
        self,
        from_: str | None = None,
        filter: Filter | None = None,
        limit: int | None = None,
        through: list[str] | tuple[str, str, str] | PropertyId | None = None,
        direction: Literal["outwards", "inwards"] = "outwards",
        chain_to: Literal["destination", "source"] = "destination",
        skip_already_deleted: bool = True,
        sync_mode: Literal["one_phase", "two_phase", "no_backfill"] | None = None,
        backfill_sort: list[InstanceSort] | None = None,
    ) -> None:
        super().__init__(
            from_=from_,
            filter=filter,
            limit=limit,
            direction=direction,
            chain_to=chain_to,
            skip_already_deleted=skip_already_deleted,
            sync_mode=sync_mode,
            backfill_sort=backfill_sort or [],
        )
        self.through = self._init_through(through)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        query_node = resource["nodes"]
        through = query_node.get("through")
        return cls(
            from_=query_node.get("from"),
            filter=Filter._load_if(query_node.get("filter")),
            chain_to=query_node.get("chainTo"),
            direction=query_node.get("direction"),
            through=PropertyId.load(through) if through is not None else None,
            limit=resource.get("limit"),
            skip_already_deleted=resource.get("skipAlreadyDeleted", True),
            sync_mode=cls._load_sync_mode(resource.get("mode")),
            backfill_sort=cls._load_sort(resource, "backfillSort"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        nodes: dict[str, Any] = {}
        if self.from_:
            nodes["from"] = self.from_
        if self.filter is not None:
            nodes["filter"] = self.filter.dump()
        if self.through:
            nodes["through"] = self.through.dump(camel_case=camel_case)
        if self.chain_to:
            nodes["chainTo" if camel_case else "chain_to"] = self.chain_to
        if self.direction:
            nodes["direction"] = self.direction

        output: dict[str, Any] = {"nodes": nodes}
        if self.limit:
            output["limit"] = self.limit
        if not self.skip_already_deleted:
            output["skipAlreadyDeleted" if camel_case else "skip_already_deleted"] = self.skip_already_deleted
        if self.sync_mode:
            output["mode"] = self._dump_sync_mode(self.sync_mode, camel_case=camel_case)
        if self.backfill_sort:
            output["backfillSort" if camel_case else "backfill_sort"] = [
                s.dump(camel_case=camel_case) for s in self.backfill_sort
            ]

        return output


@dataclass
class EdgeResultSetExpressionSync(ResultSetExpressionSync):
    """Describes how to query for edges in the data model.

    Args:
        from_ (str | None): Chain your result expression from this edge.
        filter (Filter | None): Filter the result set based on this filter.
        limit (int | None): Limit the result set to this number of instances.
        direction (Literal['outwards', 'inwards']): The direction to use when traversing.
        chain_to (Literal['destination', 'source']): Control which side of the edge to chain to. The chain_to option is only applicable if the result rexpression referenced in `from` contains edges. `source` will chain to start if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e `direction=inwards`, it will chain to end. `destination` (default) will chain to end if you're following edges outwards i.e `direction=outwards`. If you're following edges inwards i.e, `direction=inwards`, it will chain to start.
        skip_already_deleted (bool): If set to False, the API will return instances that have been soft deleted before sync was initiated. Soft deletes that happen after the sync is initiated and a cursor generated, are always included in the result. Soft deleted instances are identified by having deletedTime set.
        sync_mode (SyncMode | None): Specify whether to sync instances in a single phase; in a backfill phase followed by live updates, or without any backfill. Only valid for sync operations.
        backfill_sort (list[InstanceSort]): Sort the result set during the backfill phase of a two phase sync. Only valid with sync_mode = "two_phase". The sort must be backed by a cursorable index.
        max_distance (int | None): The largest - max - number of levels to traverse.
        node_filter (Filter | None): Filter the result set based on this filter.
        termination_filter (Filter | None): Filter the result set based on this filter.
    """

    max_distance: int | None = None
    node_filter: Filter | None = None
    termination_filter: Filter | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        query_edge = resource["edges"]
        return cls(
            from_=query_edge.get("from"),
            max_distance=query_edge.get("maxDistance"),
            direction=query_edge.get("direction"),
            filter=Filter._load_if(query_edge.get("filter")),
            node_filter=Filter._load_if(query_edge.get("nodeFilter")),
            termination_filter=Filter._load_if(query_edge.get("terminationFilter")),
            chain_to=query_edge.get("chainTo"),
            limit=resource.get("limit"),
            skip_already_deleted=resource.get("skipAlreadyDeleted", True),
            sync_mode=cls._load_sync_mode(resource.get("mode")),
            backfill_sort=cls._load_sort(resource, "backfillSort"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        edges: dict[str, Any] = {}
        if self.from_:
            edges["from"] = self.from_
        if self.max_distance:
            edges["maxDistance" if camel_case else "max_distance"] = self.max_distance
        if self.direction:
            edges["direction"] = self.direction
        if self.filter is not None:
            edges["filter"] = self.filter.dump()
        if self.node_filter is not None:
            edges["nodeFilter" if camel_case else "node_filter"] = self.node_filter.dump()
        if self.termination_filter is not None:
            edges["terminationFilter" if camel_case else "termination_filter"] = self.termination_filter.dump()
        if self.chain_to:
            edges["chainTo" if camel_case else "chain_to"] = self.chain_to

        output: dict[str, Any] = {"edges": edges}
        if self.limit:
            output["limit"] = self.limit
        if not self.skip_already_deleted:
            output["skipAlreadyDeleted" if camel_case else "skip_already_deleted"] = self.skip_already_deleted
        if self.sync_mode:
            output["mode"] = self._dump_sync_mode(self.sync_mode, camel_case=camel_case)
        if self.backfill_sort:
            output["backfillSort" if camel_case else "backfill_sort"] = [
                s.dump(camel_case=camel_case) for s in self.backfill_sort
            ]
        return output


class QueryResult(UserDict):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._debug: DebugInfo | None = None

    def __getitem__(self, item: str) -> NodeListWithCursor | EdgeListWithCursor:
        return super().__getitem__(item)

    @property
    def cursors(self) -> dict[str, str]:
        return {key: value.cursor for key, value in self.items()}

    @property
    def debug(self) -> DebugInfo | None:
        return self._debug

    @classmethod
    def load(
        cls,
        resource: dict[str, Any],
        instance_list_type_by_result_expression_name: dict[str, type[NodeListWithCursor] | type[EdgeListWithCursor]],
        cursors: dict[str, Any],
        typing: dict[str, Any] | None = None,
        debug: dict[str, Any] | None = None,
    ) -> QueryResult:
        from cognite.client.data_classes.data_modeling.debug import DebugInfo

        instance = cls()
        typing_nodes = TypeInformation._load(typing["nodes"]) if typing and "nodes" in typing else None
        typing_edges = TypeInformation._load(typing["edges"]) if typing and "edges" in typing else None

        instance._debug = debug_info = DebugInfo._load(debug) if debug is not None else None

        for key, values in resource.items():
            cursor = cursors.get(key)
            if not values:
                # When no results, inspection can't tell us if it's nodes or edges:
                instance_lst_cls = instance_list_type_by_result_expression_name[key]
                instance[key] = instance_lst_cls(
                    [],
                    cursor=cursor,
                    typing=typing_nodes if instance_lst_cls is NodeListWithCursor else typing_edges,
                    debug=debug_info,
                )
            elif values[0]["instanceType"] == "node":
                instance[key] = NodeListWithCursor(
                    [Node._load(node) for node in values],
                    cursor=cursor,
                    typing=typing_nodes,
                    debug=debug_info,
                )
            elif values[0]["instanceType"] == "edge":
                instance[key] = EdgeListWithCursor(
                    [Edge._load(edge) for edge in values],
                    cursor=cursor,
                    typing=typing_edges,
                    debug=debug_info,
                )
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


@dataclass
class SetOperation(ResultSetExpression, ABC):
    @classmethod
    def _load(cls, resource: dict[str, Any]) -> SetOperation:
        if "union" in resource:
            return Union._load(resource)
        elif "unionAll" in resource:
            return UnionAll._load(resource)
        elif "intersection" in resource:
            return Intersection._load(resource)
        else:
            raise ValueError(f"Unknown set operation {resource}")


@dataclass
class Union(SetOperation):
    union: Sequence[str | SetOperation]
    except_: SequenceNotStr[str] | None = None
    limit: int | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Union:
        union = resource["union"]
        except_ = resource.get("except")
        return cls(
            union=[item if isinstance(item, str) else SetOperation._load(item) for item in union],
            except_=[str(item) for item in except_] if except_ else None,
            limit=resource.get("limit"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "union": [item if isinstance(item, str) else item.dump(camel_case) for item in self.union]
        }
        if self.except_:
            output["except"] = self.except_
        if self.limit:
            output["limit"] = self.limit
        return output


@dataclass
class UnionAll(SetOperation):
    union_all: Sequence[str | SetOperation]
    except_: SequenceNotStr[str] | None = None
    limit: int | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> UnionAll:
        union = resource["unionAll"]
        except_ = resource.get("except")
        return cls(
            union_all=[item if isinstance(item, str) else SetOperation._load(item) for item in union],
            except_=[str(item) for item in except_] if except_ else None,
            limit=resource.get("limit"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "unionAll": [item if isinstance(item, str) else item.dump(camel_case) for item in self.union_all]
        }
        if self.except_:
            output["except"] = self.except_
        if self.limit:
            output["limit"] = self.limit
        return output


@dataclass
class Intersection(SetOperation):
    intersection: Sequence[str | SetOperation]
    except_: SequenceNotStr[str] | None = None
    limit: int | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Intersection:
        union = resource["intersection"]
        except_ = resource.get("except")
        return cls(
            intersection=[item if isinstance(item, str) else SetOperation._load(item) for item in union],
            except_=[str(item) for item in except_] if except_ else None,
            limit=resource.get("limit"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "intersection": [item if isinstance(item, str) else item.dump(camel_case) for item in self.intersection]
        }
        if self.except_:
            output["except"] = self.except_
        if self.limit:
            output["limit"] = self.limit
        return output
