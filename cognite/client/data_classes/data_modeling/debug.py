from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserList
from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Any, Literal, get_args, get_type_hints

from typing_extensions import Self

from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.data_classes.data_modeling.instances import InstanceSort
from cognite.client.utils._auxiliary import all_concrete_subclasses
from cognite.client.utils._importing import local_import


@dataclass(kw_only=True)
class DebugParameters:
    """
    Debug parameters for debugging and analyzing queries.

    Args:
        emit_results (bool): Include the query result in the response. emit_results=False is required for advanced query analysis features.
        timeout (int | None): Query timeout in milliseconds. Can be used to override the default timeout when analysing queries. Requires emit_results=False.
        profile (bool): Most thorough level of query analysis. Requires emit_results=False.
    """

    emit_results: bool = True
    timeout: int | None = None
    profile: bool = False

    def dump(self, camel_case: bool = True) -> dict[str, bool | int]:
        res = asdict(self)
        if camel_case:
            res["emitResults"] = res.pop("emit_results")
        if self.timeout is None:
            res.pop("timeout")
        return res


@dataclass
class DebugNotice(ABC):
    """
    A notice that provides insight into the query's execution. It can highlight potential performance issues,
    offer an optimization suggestion, or explain an aspect of the query processing. Each notice falls into a
    category, such as indexing, sorting, filtering, or cursoring, to help identify areas for improvement.
    """

    @classmethod
    def load(cls, data: dict[str, Any]) -> DebugNotice:
        key = (data["code"], data["category"])
        notice_cls = _DEBUG_NOTICE_BY_CODE_AND_CATEGORY.get(key, UnknownDebugNotice)
        return notice_cls._load(data)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    @abstractmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        raise NotImplementedError


@dataclass
class InvalidDebugOptionsNotice(DebugNotice, ABC):
    pass


@dataclass
class ExcessiveTimeoutNotice(InvalidDebugOptionsNotice):
    code: Literal["excessiveTimeout"]
    category: Literal["invalidDebugOptions"]
    level: Literal["warning"]
    hint: Literal["Cannot specify too large timeout"]
    timeout: int

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            hint=data["hint"],
            timeout=data["timeout"],
        )


@dataclass
class NoTimeoutWithResultsNotice(InvalidDebugOptionsNotice):
    code: Literal["noTimeoutWithResults"]
    category: Literal["invalidDebugOptions"]
    level: Literal["warning"]
    hint: Literal["Ignoring timeout setting since emitResults=true"]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            hint=data["hint"],
        )


@dataclass
class CursoringNotice(DebugNotice, ABC):
    pass


@dataclass
class IntractableDirectRelationsCursorNotice(CursoringNotice):
    code: Literal["intractableDirectRelationsCursor"]
    category: Literal["cursoring"]
    level: Literal["warning"]
    grade: Literal["D"]
    hint: Literal[
        "Cursoring combined with chaining to a list of direct relations requires a run-time sort and is therefore inherently intractable."
    ]
    result_expression: str

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            grade=data["grade"],
            hint=data["hint"],
            result_expression=data["resultExpression"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        if camel_case:
            obj["resultExpression"] = obj.pop("result_expression")
        return obj


@dataclass
class IndexingNotice(DebugNotice, ABC):
    pass


@dataclass
class UnindexedThroughNotice(IndexingNotice):
    code: Literal["unindexedThrough"]
    category: Literal["indexing"]
    level: Literal["warning"]
    grade: Literal["E"]
    hint: Literal[
        "The result expression has a `through` on an unindexed direct relation. This will become untenable when the container grows."
    ]
    result_expression: str
    property: list[str]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            grade=data["grade"],
            hint=data["hint"],
            result_expression=data["resultExpression"],
            property=data["property"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        if camel_case:
            obj["resultExpression"] = obj.pop("result_expression")
        return obj


@dataclass
class ContainersWithoutIndexesInvolvedNotice(IndexingNotice):
    code: Literal["containersWithoutIndexesInvolved"]
    category: Literal["indexing"]
    level: Literal["warning"]
    grade: Literal["C"] | None
    hint: Literal["The query is using one or more containers that doesn't have any indexes declared."]
    result_expression: str | None
    containers: list[ContainerId]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            grade=data.get("grade"),
            hint=data["hint"],
            result_expression=data.get("resultExpression"),
            containers=[ContainerId.load(c) for c in data["containers"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        if camel_case:
            obj["resultExpression"] = obj.pop("result_expression")
        if self.containers:
            obj["containers"] = [c.dump(camel_case=camel_case) for c in self.containers]
        return obj


@dataclass
class FilteringNotice(DebugNotice, ABC):
    pass


@dataclass
class SelectiveExternalIDFilterNotice(FilteringNotice):
    code: Literal["selectiveExternalIDFilter"]
    category: Literal["filtering"]
    level: Literal["info"]
    grade: Literal["A"]
    hint: Literal["The filter on the instances' primary key should always be selective."]
    result_expression: str
    via_from: str | None

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            grade=data["grade"],
            hint=data["hint"],
            result_expression=data["resultExpression"],
            via_from=data.get("viaFrom"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        if camel_case:
            obj["resultExpression"] = obj.pop("result_expression")
            obj["viaFrom"] = obj.pop("via_from")
        return obj


@dataclass
class SignificantHasDataFiltersNotice(FilteringNotice):
    code: Literal["significantHasDataFiltering"]
    category: Literal["filtering"]
    level: Literal["warning"]
    grade: Literal["C"]
    hint: Literal[
        "The provided `hasData` filters expand to several joins, which can be problematic depending on data distribution. Consider `requires` constraints, which can enable query time join pruning."
    ]
    result_expression: str
    containers: list[ContainerId]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            grade=data["grade"],
            hint=data["hint"],
            result_expression=data["resultExpression"],
            containers=[ContainerId.load(c) for c in data["containers"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        if camel_case:
            obj["resultExpression"] = obj.pop("result_expression")
        if self.containers:
            obj["containers"] = [c.dump(camel_case=camel_case) for c in self.containers]
        return obj


@dataclass
class SignificantPostFilteringNotice(FilteringNotice):
    code: Literal["significantPostFiltering"]
    category: Literal["filtering"]
    level: Literal["warning"]
    grade: Literal["C"]
    hint: Literal["A lot more rows than selected by the expression are involved to compute the result."]
    result_expression: str
    limit: int
    max_involved_rows: int

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            grade=data["grade"],
            hint=data["hint"],
            result_expression=data["resultExpression"],
            limit=data["limit"],
            max_involved_rows=data["maxInvolvedRows"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        if camel_case:
            obj["resultExpression"] = obj.pop("result_expression")
            obj["maxInvolvedRows"] = obj.pop("max_involved_rows")
        return obj


@dataclass
class SortingNotice(DebugNotice, ABC):
    pass


@dataclass
class SortNotBackedByIndexNotice(SortingNotice):
    code: Literal["sortNotBackedByIndex"]
    category: Literal["sorting"]
    level: Literal["warning"]
    grade: Literal["C"]
    hint: Literal[
        "The sort is not backed by a cursorable index, which means query time sorting is necessary, which in turn means a lot more data must be read. Consider whether a cursorable index is a good fit."
    ]
    result_expression: str
    sort: list[InstanceSort]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            grade=data["grade"],
            hint=data["hint"],
            result_expression=data["resultExpression"],
            sort=[InstanceSort.load(s) for s in data["sort"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        if camel_case:
            obj["resultExpression"] = obj.pop("result_expression")
        if self.sort:
            obj["sort"] = [s.dump(camel_case=camel_case) for s in self.sort]
        return obj


@dataclass
class FilterMatchesCursorableSortNotice(SortingNotice):
    code: Literal["filterMatchesCursorableSort"]
    category: Literal["sorting"]
    level: Literal["info"]
    grade: Literal["A", "B"]
    hint: Literal["The provided filter and sort combination appear to match a backing index."]
    result_expression: str
    sort: list[InstanceSort]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            grade=data["grade"],
            hint=data["hint"],
            result_expression=data["resultExpression"],
            sort=[InstanceSort.load(s) for s in data["sort"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        if camel_case:
            obj["resultExpression"] = obj.pop("result_expression")
        if self.sort:
            obj["sort"] = [s.dump(camel_case=camel_case) for s in self.sort]
        return obj


@dataclass
class UnknownDebugNotice(DebugNotice):
    data: dict[str, Any]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(data=data)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if camel_case is False:
            import warnings

            warnings.warn(
                f"{type(self).__name__}.dump(...) does not support 'camel_case=False', returning original data. "
                "Please raise an issue on Github to add support for this debug notice type.",
                stacklevel=2,
            )
        return self.data.copy()


class DebugNoticeList(UserList[DebugNotice]):
    def __init__(self, initlist: list[DebugNotice]):
        super().__init__(initlist or [])

    @classmethod
    def _load(cls, data: list[dict[str, Any]]) -> Self:
        return cls([DebugNotice.load(item) for item in data])

    def dump(self, camel_case: bool = True) -> list[dict[str, Any]]:
        return [notice.dump(camel_case=camel_case) for notice in self]

    def _repr_html_(self) -> str:
        pandas = local_import("pandas")
        dumped = self.dump(camel_case=False)
        return pandas.DataFrame(dumped)._repr_html_()


def _create_debug_notice_subclass_map() -> dict[tuple[str, str], type[DebugNotice]]:
    lookup = {}
    subclasses: set[type[DebugNotice]] = all_concrete_subclasses(DebugNotice, exclude={UnknownDebugNotice})  # type: ignore[type-abstract]
    for sub_cls in subclasses:
        annots = get_type_hints(sub_cls)
        code = get_args(annots["code"])[0]
        category = get_args(annots["category"])[0]
        lookup[code, category] = sub_cls
    return lookup


_DEBUG_NOTICE_BY_CODE_AND_CATEGORY = MappingProxyType(_create_debug_notice_subclass_map())
