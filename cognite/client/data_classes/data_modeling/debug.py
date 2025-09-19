from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, Any, Literal, NoReturn, cast, get_args, get_type_hints

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.data_classes.data_modeling.ids import ContainerId
from cognite.client.data_classes.data_modeling.instances import InstanceSort
from cognite.client.utils._auxiliary import all_concrete_subclasses

if TYPE_CHECKING:
    from cognite.client import CogniteClient


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
        res: dict[str, bool | int] = {
            "emitResults" if camel_case else "emit_results": self.emit_results,
            "profile": self.profile,
        }
        if self.timeout is not None:
            res["timeout"] = self.timeout
        return res


@dataclass
class DebugNotice(CogniteResource, ABC):
    """
    A notice that provides insight into the query's execution. It can highlight potential performance issues,
    offer an optimization suggestion, or explain an aspect of the query processing. Each notice falls into a
    category, such as indexing, sorting, filtering, or cursoring, to help identify areas for improvement.
    """

    category: str
    code: str
    hint: str
    level: str

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> DebugNotice:
        subclass_lookup = _create_debug_notice_subclass_map()
        try:
            key = (data["code"], data["category"])
            return subclass_lookup[key]._load(data)
        except KeyError:
            return cast(DebugNotice, UnknownDebugNotice._load(data))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "category": self.category,
            "code": self.code,
            "hint": self.hint,
            "level": self.level,
        }


@dataclass
class InvalidDebugOptionsNotice(DebugNotice, ABC):
    pass


@dataclass
class ExcessiveTimeoutNotice(InvalidDebugOptionsNotice):
    code: Literal["excessiveTimeout"]
    category: Literal["invalidDebugOptions"]
    level: Literal["warning"]
    hint: str
    timeout: int

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            hint=data["hint"],
            timeout=data["timeout"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        obj["timeout"] = self.timeout
        return obj


@dataclass
class NoTimeoutWithResultsNotice(InvalidDebugOptionsNotice):
    code: Literal["noTimeoutWithResults"]
    category: Literal["invalidDebugOptions"]
    level: Literal["warning"]
    hint: str

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
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
    hint: str
    result_expression: str

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
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
        obj["grade"] = self.grade
        obj["resultExpression" if camel_case else "result_expression"] = self.result_expression
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
    hint: str
    result_expression: str
    property: list[str]

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
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
        obj.update(
            {
                "resultExpression" if camel_case else "result_expression": self.result_expression,
                "grade": self.grade,
                "property": self.property,
            }
        )
        return obj


@dataclass
class ContainersWithoutIndexesInvolvedNotice(IndexingNotice):
    code: Literal["containersWithoutIndexesInvolved"]
    category: Literal["indexing"]
    level: Literal["warning"]
    grade: Literal["C"]
    hint: str
    result_expression: str | None
    containers: list[ContainerId]

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            code=data["code"],
            category=data["category"],
            level=data["level"],
            grade=data["grade"],
            hint=data["hint"],
            result_expression=data.get("resultExpression"),
            containers=[ContainerId.load(c) for c in data["containers"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        obj.update(
            {
                "grade": self.grade,
                "containers": [c.dump(camel_case=camel_case) for c in self.containers],
                "resultExpression" if camel_case else "result_expression": self.result_expression,
            }
        )
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
    hint: str
    result_expression: str
    via_from: str | None

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
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
        obj.update(
            {
                "resultExpression" if camel_case else "result_expression": self.result_expression,
                "viaFrom" if camel_case else "via_from": self.via_from,
                "grade": self.grade,
            }
        )
        return obj


@dataclass
class SignificantHasDataFiltersNotice(FilteringNotice):
    code: Literal["significantHasDataFiltering"]
    category: Literal["filtering"]
    level: Literal["warning"]
    grade: Literal["C"]
    hint: str
    result_expression: str
    containers: list[ContainerId]

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
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
        obj.update(
            {
                "grade": self.grade,
                "resultExpression" if camel_case else "result_expression": self.result_expression,
                "containers": [c.dump(camel_case=camel_case) for c in self.containers],
            }
        )
        return obj


@dataclass
class SignificantPostFilteringNotice(FilteringNotice):
    code: Literal["significantPostFiltering"]
    category: Literal["filtering"]
    level: Literal["warning"]
    grade: Literal["C"]
    hint: str
    result_expression: str
    limit: int
    max_involved_rows: int

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
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
        obj.update(
            {
                "grade": self.grade,
                "resultExpression" if camel_case else "result_expression": self.result_expression,
                "limit": self.limit,
                "maxInvolvedRows" if camel_case else "max_involved_rows": self.max_involved_rows,
            }
        )
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
    hint: str
    result_expression: str
    sort: list[InstanceSort]

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
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
        obj.update(
            {
                "grade": self.grade,
                "resultExpression" if camel_case else "result_expression": self.result_expression,
                "sort": [s.dump(camel_case=camel_case) for s in self.sort],
            }
        )
        return obj


@dataclass
class FilterMatchesCursorableSortNotice(SortingNotice):
    code: Literal["filterMatchesCursorableSort"]
    category: Literal["sorting"]
    level: Literal["info"]
    grade: Literal["A", "B"]
    hint: str
    result_expression: str
    sort: list[InstanceSort]

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
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
        obj.update(
            {
                "grade": self.grade,
                "resultExpression" if camel_case else "result_expression": self.result_expression,
                "sort": [s.dump(camel_case=camel_case) for s in self.sort],
            }
        )
        return obj


@dataclass
class UnknownDebugNotice(DebugNotice):
    category: str | None  # type: ignore [assignment]
    code: str | None  # type: ignore [assignment]
    hint: str | None  # type: ignore [assignment]
    level: str | None  # type: ignore [assignment]
    data: dict[str, Any]

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        data = data.copy()
        category = data.pop("category", None)
        code = data.pop("code", None)
        hint = data.pop("hint", None)
        level = data.pop("level", None)
        return cls(category=category, code=code, hint=hint, level=level, data=data)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if camel_case is False:
            import warnings

            warnings.warn(
                f"{type(self).__name__}.dump(...) does not support 'camel_case=False', returning original data. "
                "Please raise an issue on Github to add support for this debug notice type.",
                stacklevel=2,
            )
        output = {}
        if self.category is not None:
            output["category"] = self.category
        if self.code is not None:
            output["code"] = self.code
        if self.hint is not None:
            output["hint"] = self.hint
        if self.level is not None:
            output["level"] = self.level
        return output | self.data.copy()


class DebugNoticeList(CogniteResourceList[DebugNotice]):
    _RESOURCE = DebugNotice

    def get(self, *a: Any, **kw: Any) -> NoReturn:
        raise NotImplementedError


@cache
def _create_debug_notice_subclass_map() -> dict[tuple[str, str], type[DebugNotice]]:
    from cognite.client import CogniteClient

    lookup = {}
    subclasses: set[type[DebugNotice]] = all_concrete_subclasses(DebugNotice)
    for sub_cls in subclasses:
        # When we do 'get_type_hints', we evaluate forward refs., and we reference InstanceSort in a file
        # that only imports CogniteClient while TYPE_CHECKING, so it is not available at runtime which we need:
        annots = get_type_hints(sub_cls, globalns=globals() | {"CogniteClient": CogniteClient})
        code = get_args(annots["code"])[0]
        category = get_args(annots["category"])[0]
        lookup[code, category] = sub_cls
    return lookup
