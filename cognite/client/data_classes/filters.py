from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Mapping, Optional, Sequence, Tuple, Union, cast, final

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId


PropertyReference = Union[Tuple[str, ...], List[str]]

RawValue = Union[str, float, bool, Sequence, Mapping[str, Any]]


@dataclass
class PropertyReferenceValue:
    property: PropertyReference


@dataclass
class ParameterValue:
    parameter: str


FilterValue = Union[RawValue, PropertyReferenceValue, ParameterValue]
FilterValueList = Union[Sequence[RawValue], PropertyReferenceValue, ParameterValue]


def _dump_filter_value(filter_value: FilterValueList | FilterValue) -> Any:
    if isinstance(filter_value, PropertyReferenceValue):
        return {"property": filter_value.property}
    if isinstance(filter_value, ParameterValue):
        return {"parameter": filter_value.parameter}
    else:
        return filter_value


def _load_filter_value(value: Any) -> FilterValue | FilterValueList:
    if isinstance(value, Mapping) and len(value.keys()) == 1:
        (value_key,) = value
        if value_key == "property":
            return PropertyReferenceValue(value[value_key])
        if value_key == "parameter":
            return ParameterValue(value[value_key])
    return value


class Filter(ABC):
    _filter_name: str

    def dump(self) -> dict[str, Any]:
        return {self._filter_name: self._filter_body()}

    @classmethod
    def load(cls, filter_: dict[str, Any]) -> Filter:
        (filter_name,) = filter_
        filter_body = filter_[filter_name]

        if filter_name == And._filter_name:
            return And(*(Filter.load(filter_) for filter_ in filter_body))
        elif filter_name == Or._filter_name:
            return Or(*(Filter.load(filter_) for filter_ in filter_body))
        elif filter_name == Not._filter_name:
            return Not(Filter.load(filter_body))
        elif filter_name == Nested._filter_name:
            return Nested(scope=filter_body["scope"], filter=Filter.load(filter_body["filter"]))
        elif filter_name == MatchAll._filter_name:
            return MatchAll()
        elif filter_name == HasData._filter_name:
            return HasData(containers=filter_body.get("containers"), views=filter_body.get("views"))
        elif filter_name == Range._filter_name:
            return Range(
                property=filter_body["property"],
                gt=_load_filter_value(filter_body.get("gt")),
                gte=_load_filter_value(filter_body.get("gte")),
                lt=_load_filter_value(filter_body.get("lt")),
                lte=_load_filter_value(filter_body.get("lte")),
            )
        elif filter_name == Overlaps._filter_name:
            return Overlaps(
                start_property=filter_body.get("startProperty"),
                end_property=filter_body.get("endProperty"),
                gt=_load_filter_value(filter_body.get("gt")),
                gte=_load_filter_value(filter_body.get("gte")),
                lt=_load_filter_value(filter_body.get("lt")),
                lte=_load_filter_value(filter_body.get("lte")),
            )
        elif filter_name == Equals._filter_name:
            return Equals(
                property=filter_body["property"],
                value=_load_filter_value(filter_body.get("value")),
            )
        elif filter_name == In._filter_name:
            return In(
                property=filter_body["property"],
                values=cast(FilterValueList, _load_filter_value(filter_body.get("values"))),
            )
        elif filter_name == Exists._filter_name:
            return Exists(property=filter_body["property"])
        elif filter_name == Prefix._filter_name:
            return Prefix(
                property=filter_body["property"],
                value=_load_filter_value(filter_body["value"]),
            )
        elif filter_name == ContainsAny._filter_name:
            return ContainsAny(
                property=filter_body["property"],
                values=cast(FilterValueList, _load_filter_value(filter_body["values"])),
            )
        else:
            raise ValueError(f"Unknown filter type: {filter_name}")

    @abstractmethod
    def _filter_body(self) -> list | dict:
        ...

    def _involved_filter_types(self) -> set[type[Filter]]:
        output = {type(self)}
        if isinstance(self, CompoundFilter):
            for filter_ in self._filters:
                output.update(filter_._involved_filter_types())
        return output


class CompoundFilter(Filter):
    _filter_name = "compound"

    def __init__(self, *filters: Filter):
        self._filters = filters

    def _filter_body(self) -> list | dict:
        return [filter_.dump() for filter_ in self._filters]


class FilterWithProperty(Filter):
    _filter_name = "propertyFilter"

    def __init__(self, property: PropertyReference):
        self._property = property

    def _filter_body(self) -> dict:
        return {"property": self._property}


class FilterWithPropertyAndValue(FilterWithProperty):
    _filter_name = "propertyAndValueFilter"

    def __init__(self, property: PropertyReference, value: FilterValue):
        super().__init__(property)
        self._value = value

    def _filter_body(self) -> dict[str, Any]:
        return {"property": self._property, "value": _dump_filter_value(self._value)}


class FilterWithPropertyAndValueList(FilterWithProperty):
    _filter_name = "propertyAndValueListFilter"

    def __init__(self, property: PropertyReference, values: FilterValueList):
        super().__init__(property)
        self._values = values

    def _filter_body(self) -> dict[str, Any]:
        return {"property": self._property, "values": _dump_filter_value(self._values)}


@final
class And(CompoundFilter):
    _filter_name = "and"


@final
class Or(CompoundFilter):
    _filter_name = "or"


@final
class Not(CompoundFilter):
    _filter_name = "not"

    def __init__(self, filter: Filter):
        super().__init__(filter)

    def _filter_body(self) -> dict:
        return self._filters[0].dump()


@final
class Nested(Filter):
    _filter_name = "nested"

    def __init__(self, scope: PropertyReference, filter: Filter) -> None:
        self._scope = scope
        self._filter = filter

    def _filter_body(self) -> dict[str, Any]:
        return {"scope": self._scope, "filter": self._filter.dump()}


@final
class MatchAll(Filter):
    _filter_name = "matchAll"

    def _filter_body(self) -> dict[str, Any]:
        return {}


@final
class HasData(Filter):
    _filter_name = "hasData"

    def __init__(
        self,
        containers: Optional[Sequence[tuple[str, str] | ContainerId]] = None,
        views: Optional[Sequence[tuple[str, str, str] | ViewId]] = None,
    ):
        from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId

        self.__containers: List[ContainerId] = [ContainerId.load(container) for container in (containers or [])]
        self.__views: List[ViewId] = [ViewId.load(view) for view in (views or [])]

    def _filter_body(self) -> dict:
        return {
            "views": [view.as_tuple() for view in self.__views],
            "containers": [container.as_tuple() for container in self.__containers],
        }


@final
class Range(FilterWithProperty):
    _filter_name = "range"

    def __init__(
        self,
        property: PropertyReference,
        gt: Optional[FilterValue] = None,
        gte: Optional[FilterValue] = None,
        lt: Optional[FilterValue] = None,
        lte: Optional[FilterValue] = None,
    ):
        super().__init__(property)
        self._gt = gt
        self._gte = gte
        self._lt = lt
        self._lte = lte

    def _filter_body(self) -> dict[str, Any]:
        body = {"property": self._property}
        if self._gt is not None:
            body["gt"] = _dump_filter_value(self._gt)
        if self._gte is not None:
            body["gte"] = _dump_filter_value(self._gte)
        if self._lt is not None:
            body["lt"] = _dump_filter_value(self._lt)
        if self._lte is not None:
            body["lte"] = _dump_filter_value(self._lte)
        return body


@final
class Overlaps(Filter):
    _filter_name = "overlaps"

    def __init__(
        self,
        start_property: PropertyReference,
        end_property: PropertyReference,
        gt: Optional[FilterValue] = None,
        gte: Optional[FilterValue] = None,
        lt: Optional[FilterValue] = None,
        lte: Optional[FilterValue] = None,
    ):
        self._start_property = start_property
        self._end_property = end_property
        self._gt = gt
        self._gte = gte
        self._lt = lt
        self._lte = lte

    def _filter_body(self) -> dict[str, Any]:
        body = {
            "startProperty": self._start_property,
            "endProperty": self._end_property,
        }

        if self._gt is not None:
            body["gt"] = _dump_filter_value(self._gt)
        if self._gte is not None:
            body["gte"] = _dump_filter_value(self._gte)
        if self._lt is not None:
            body["lt"] = _dump_filter_value(self._lt)
        if self._lte is not None:
            body["lte"] = _dump_filter_value(self._lte)
        return body


@final
class Equals(FilterWithPropertyAndValue):
    _filter_name = "equals"


@final
class In(FilterWithPropertyAndValueList):
    _filter_name = "in"


@final
class Exists(FilterWithProperty):
    _filter_name = "exists"


@final
class Prefix(FilterWithPropertyAndValue):
    _filter_name = "prefix"


@final
class ContainsAny(FilterWithPropertyAndValueList):
    _filter_name = "containsAny"


@final
class ContainsAll(FilterWithPropertyAndValueList):
    _filter_name = "containsAll"
