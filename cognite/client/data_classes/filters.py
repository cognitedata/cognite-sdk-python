from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Mapping, Sequence, Tuple, Union, cast, final

from typing_extensions import TypeAlias

from cognite.client.data_classes._base import EnumProperty, Geometry
from cognite.client.data_classes.labels import Label
from cognite.client.utils._text import to_camel_case

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId


PropertyReference: TypeAlias = Union[str, Tuple[str, ...], List[str], EnumProperty]

RawValue: TypeAlias = Union[str, float, bool, Sequence, Mapping[str, Any], Label]


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
        return {
            "property": filter_value.property.as_reference()
            if isinstance(filter_value.property, EnumProperty)
            else filter_value.property
        }
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


def _dump_property(property_: PropertyReference, camel_case: bool) -> list[str] | tuple[str, ...]:
    if isinstance(property_, EnumProperty):
        return property_.as_reference()
    elif isinstance(property_, str):
        return [to_camel_case(property_) if camel_case else property_]
    elif isinstance(property_, (list, tuple)):
        output = [to_camel_case(p) if camel_case else p for p in property_]
        return tuple(output) if isinstance(property_, tuple) else output
    else:
        raise ValueError(f"Invalid property format {property_}")


class Filter(ABC):
    _filter_name: str

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {self._filter_name: self._filter_body(camel_case)}

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
            containers = []
            views = []
            for view_or_space in filter_body:
                if view_or_space["type"] == "container":
                    containers.append((view_or_space["space"], view_or_space["externalId"]))
                else:
                    views.append((view_or_space["space"], view_or_space["externalId"], view_or_space.get("version")))
            return HasData(containers=containers, views=views)
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
        elif filter_name == ContainsAll._filter_name:
            return ContainsAll(
                property=filter_body["property"],
                values=cast(FilterValueList, _load_filter_value(filter_body["values"])),
            )
        elif filter_name == GeoJSONIntersects._filter_name:
            return GeoJSONIntersects(
                property=filter_body["property"],
                geometry=Geometry._load(filter_body["geometry"]),
            )
        elif filter_name == GeoJSONDisjoint._filter_name:
            return GeoJSONDisjoint(
                property=filter_body["property"],
                geometry=Geometry._load(filter_body["geometry"]),
            )
        elif filter_name == GeoJSONWithin._filter_name:
            return GeoJSONWithin(
                property=filter_body["property"],
                geometry=Geometry._load(filter_body["geometry"]),
            )
        elif filter_name == InAssetSubtree._filter_name:
            return InAssetSubtree(
                property=filter_body["property"],
                value=_load_filter_value(filter_body["value"]),
            )
        elif filter_name == Search._filter_name:
            return Search(
                property=filter_body["property"],
                value=_load_filter_value(filter_body["value"]),
            )
        else:
            raise ValueError(f"Unknown filter type: {filter_name}")

    @abstractmethod
    def _filter_body(self, camel_case_property: bool) -> list | dict:
        ...

    def _involved_filter_types(self) -> set[type[Filter]]:
        output = {type(self)}
        if isinstance(self, CompoundFilter):
            for filter_ in self._filters:
                output.update(filter_._involved_filter_types())
        return output


def _validate_filter(filter: Filter | dict | None, supported_filters: frozenset[type[Filter]], api_name: str) -> None:
    if filter is None or isinstance(filter, dict):
        return
    if not_supported := (filter._involved_filter_types() - supported_filters):
        names = [f.__name__ for f in not_supported]
        raise ValueError(f"The filters {names} are not supported for {api_name}")


class CompoundFilter(Filter):
    _filter_name = "compound"

    def __init__(self, *filters: Filter) -> None:
        self._filters = filters

    def _filter_body(self, camel_case_property: bool) -> list | dict:
        return [filter_.dump(camel_case_property) for filter_ in self._filters]


class FilterWithProperty(Filter):
    _filter_name = "propertyFilter"

    def __init__(self, property: PropertyReference) -> None:
        self._property = property

    def _dump_property(self, camel_case: bool) -> list[str] | tuple[str, ...]:
        return _dump_property(self._property, camel_case)

    def _filter_body(self, camel_case_property: bool) -> dict:
        return {"property": self._dump_property(camel_case_property)}


class FilterWithPropertyAndValue(FilterWithProperty):
    _filter_name = "propertyAndValueFilter"

    def __init__(self, property: PropertyReference, value: FilterValue) -> None:
        super().__init__(property)
        self._value = value

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {"property": self._dump_property(camel_case_property), "value": _dump_filter_value(self._value)}


class FilterWithPropertyAndValueList(FilterWithProperty):
    _filter_name = "propertyAndValueListFilter"

    def __init__(self, property: PropertyReference, values: FilterValueList) -> None:
        super().__init__(property)
        self._values = values

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {"property": self._dump_property(camel_case_property), "values": _dump_filter_value(self._values)}


@final
class And(CompoundFilter):
    _filter_name = "and"


@final
class Or(CompoundFilter):
    _filter_name = "or"


@final
class Not(CompoundFilter):
    _filter_name = "not"

    def __init__(self, filter: Filter) -> None:
        super().__init__(filter)

    def _filter_body(self, camel_case_property: bool) -> dict:
        return self._filters[0].dump(camel_case_property)


@final
class Nested(Filter):
    _filter_name = "nested"

    def __init__(self, scope: PropertyReference, filter: Filter) -> None:
        self._scope = scope
        self._filter = filter

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {"scope": self._scope, "filter": self._filter.dump(camel_case_property)}


@final
class MatchAll(Filter):
    _filter_name = "matchAll"

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {}


@final
class HasData(Filter):
    _filter_name = "hasData"

    def __init__(
        self,
        containers: Sequence[tuple[str, str] | ContainerId] | None = None,
        views: Sequence[tuple[str, str, str] | ViewId] | None = None,
    ) -> None:
        from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId

        self.__containers: list[ContainerId] = [ContainerId.load(container) for container in (containers or [])]
        self.__views: list[ViewId] = [ViewId.load(view) for view in (views or [])]

    def _filter_body(self, camel_case_property: bool) -> list:
        views = [v.as_tuple() for v in self.__views]
        filter_body_views = [
            {"type": "view", "space": space, "externalId": externalId, "version": version}
            for space, externalId, version in views
        ]
        containers = [c.as_tuple() for c in self.__containers]
        filter_body_containers = [
            {"type": "container", "space": space, "externalId": externalId} for space, externalId in containers
        ]
        return filter_body_views + filter_body_containers


@final
class Range(FilterWithProperty):
    _filter_name = "range"

    def __init__(
        self,
        property: PropertyReference,
        gt: FilterValue | None = None,
        gte: FilterValue | None = None,
        lt: FilterValue | None = None,
        lte: FilterValue | None = None,
    ) -> None:
        super().__init__(property)
        self._gt = gt
        self._gte = gte
        self._lt = lt
        self._lte = lte

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        body = {"property": self._dump_property(camel_case_property)}
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
        gt: FilterValue | None = None,
        gte: FilterValue | None = None,
        lt: FilterValue | None = None,
        lte: FilterValue | None = None,
    ) -> None:
        self._start_property = start_property
        self._end_property = end_property
        self._gt = gt
        self._gte = gte
        self._lt = lt
        self._lte = lte

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        body = {
            "startProperty": _dump_property(self._start_property, camel_case_property),
            "endProperty": _dump_property(self._end_property, camel_case_property),
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


class GeoJSON(FilterWithProperty, ABC):
    _filter_name = "geojson"

    def __init__(self, property: PropertyReference, geometry: Geometry):
        super().__init__(property)
        self._geometry = geometry

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {"property": self._dump_property(camel_case_property), "geometry": self._geometry.dump(camel_case=True)}


@final
class GeoJSONIntersects(GeoJSON):
    _filter_name = "geojsonIntersects"


@final
class GeoJSONDisjoint(GeoJSON):
    _filter_name = "geojsonDisjoint"


@final
class GeoJSONWithin(GeoJSON):
    _filter_name = "geojsonWithin"


@final
class InAssetSubtree(FilterWithPropertyAndValue):
    _filter_name = "inAssetSubtree"


@final
class Search(FilterWithPropertyAndValue):
    _filter_name = "search"
