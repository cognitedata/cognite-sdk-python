from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, NoReturn, TypeAlias, cast, final

from cognite.client.data_classes._base import EnumProperty, Geometry
from cognite.client.data_classes.labels import Label
from cognite.client.utils._text import convert_all_keys_to_camel_case, to_camel_case
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client.data_classes.data_modeling.ids import ContainerId, ViewId


PropertyReference: TypeAlias = str | tuple[str, ...] | list[str] | EnumProperty

RawValue: TypeAlias = str | float | bool | Sequence | Mapping[str, Any] | Label


@dataclass
class PropertyReferenceValue:
    property: PropertyReference


@dataclass
class ParameterValue:
    parameter: str


FilterValue: TypeAlias = RawValue | PropertyReferenceValue | ParameterValue
FilterValueList: TypeAlias = Sequence[RawValue] | PropertyReferenceValue | ParameterValue


def _dump_filter_value(value: FilterValueList | FilterValue) -> Any:
    if isinstance(value, PropertyReferenceValue):
        if isinstance(value.property, EnumProperty):
            return {"property": value.property.as_reference()}
        return {"property": value.property}

    elif isinstance(value, ParameterValue):
        return {"parameter": value.parameter}

    elif hasattr(value, "dump"):
        return value.dump()

    elif isinstance(value, SequenceNotStr):
        return list(map(_dump_filter_value, value))

    return value


def _load_filter_value(value: Any) -> FilterValue | FilterValueList:
    if isinstance(value, Mapping) and len(value.keys()) == 1:
        ((value_key, to_load),) = value.items()
        if value_key == "property":
            return PropertyReferenceValue(to_load)
        if value_key == "parameter":
            return ParameterValue(to_load)
    return value


def _dump_property(property_: PropertyReference, camel_case: bool) -> list[str] | tuple[str, ...]:
    if isinstance(property_, EnumProperty):
        return property_.as_reference()
    elif isinstance(property_, str):
        return [to_camel_case(property_) if camel_case else property_]
    elif isinstance(property_, (list, tuple)):
        if len(property_) == 1:
            return [to_camel_case(property_[0])] if camel_case else property_
        else:
            return property_
    else:
        raise ValueError(f"Invalid property format {property_}")


class Filter(ABC):
    _filter_name: str

    def dump(self, camel_case_property: bool = False) -> dict[str, Any]:
        """
        Dump the filter to a dictionary.

        Args:
            camel_case_property (bool): Whether to camel case the property names. Defaults to False. Typically,
                when the filter is used in data modeling, the property names should not be changed,
                while when used with Assets, Events, Sequences, or Files, the property names should be camel cased.

        Returns:
            dict[str, Any]: The filter as a dictionary.

        """
        return {self._filter_name: self._filter_body(camel_case_property=camel_case_property)}

    @classmethod
    def load(cls, filter_: dict[str, Any]) -> Filter:
        if (filter_body := filter_.get(And._filter_name)) is not None:
            return And(*(Filter.load(filter_) for filter_ in filter_body))
        elif (filter_body := filter_.get(Or._filter_name)) is not None:
            return Or(*(Filter.load(filter_) for filter_ in filter_body))
        elif (filter_body := filter_.get(Not._filter_name)) is not None:
            return Not(Filter.load(filter_body))
        elif (filter_body := filter_.get(Nested._filter_name)) is not None:
            return Nested(scope=filter_body["scope"], filter=Filter.load(filter_body["filter"]))
        elif MatchAll._filter_name in filter_:
            return MatchAll()
        elif (filter_body := filter_.get(HasData._filter_name)) is not None:
            containers = []
            views = []
            for view_or_space in filter_body:
                if view_or_space["type"] == "container":
                    containers.append((view_or_space["space"], view_or_space["externalId"]))
                else:
                    views.append((view_or_space["space"], view_or_space["externalId"], view_or_space.get("version")))
            return HasData(containers=containers, views=views)
        elif (filter_body := filter_.get(Range._filter_name)) is not None:
            return Range(
                property=filter_body["property"],
                gt=_load_filter_value(filter_body.get("gt")),
                gte=_load_filter_value(filter_body.get("gte")),
                lt=_load_filter_value(filter_body.get("lt")),
                lte=_load_filter_value(filter_body.get("lte")),
            )
        elif (filter_body := filter_.get(Overlaps._filter_name)) is not None:
            return Overlaps(
                start_property=filter_body.get("startProperty"),
                end_property=filter_body.get("endProperty"),
                gt=_load_filter_value(filter_body.get("gt")),
                gte=_load_filter_value(filter_body.get("gte")),
                lt=_load_filter_value(filter_body.get("lt")),
                lte=_load_filter_value(filter_body.get("lte")),
            )
        elif (filter_body := filter_.get(Equals._filter_name)) is not None:
            return Equals(
                property=filter_body["property"],
                value=_load_filter_value(filter_body.get("value")),
            )
        elif (filter_body := filter_.get(In._filter_name)) is not None:
            return In(
                property=filter_body["property"],
                values=cast(FilterValueList, _load_filter_value(filter_body.get("values"))),
            )
        elif (filter_body := filter_.get(Exists._filter_name)) is not None:
            return Exists(property=filter_body["property"])
        elif (filter_body := filter_.get(Prefix._filter_name)) is not None:
            return Prefix(
                property=filter_body["property"],
                value=_load_filter_value(filter_body["value"]),
            )
        elif (filter_body := filter_.get(ContainsAny._filter_name)) is not None:
            return ContainsAny(
                property=filter_body["property"],
                values=cast(FilterValueList, _load_filter_value(filter_body["values"])),
            )
        elif (filter_body := filter_.get(ContainsAll._filter_name)) is not None:
            return ContainsAll(
                property=filter_body["property"],
                values=cast(FilterValueList, _load_filter_value(filter_body["values"])),
            )
        elif (filter_body := filter_.get(GeoJSONIntersects._filter_name)) is not None:
            return GeoJSONIntersects(
                property=filter_body["property"],
                geometry=Geometry.load(filter_body["geometry"]),
            )
        elif (filter_body := filter_.get(GeoJSONDisjoint._filter_name)) is not None:
            return GeoJSONDisjoint(
                property=filter_body["property"],
                geometry=Geometry.load(filter_body["geometry"]),
            )
        elif (filter_body := filter_.get(GeoJSONWithin._filter_name)) is not None:
            return GeoJSONWithin(
                property=filter_body["property"],
                geometry=Geometry.load(filter_body["geometry"]),
            )
        elif (filter_body := filter_.get(InAssetSubtree._filter_name)) is not None:
            return InAssetSubtree(
                property=filter_body["property"],
                values=cast(FilterValueList, _load_filter_value(filter_body["values"])),
            )
        elif (filter_body := filter_.get(Search._filter_name)) is not None:
            return Search(
                property=filter_body["property"],
                value=_load_filter_value(filter_body["value"]),
            )
        elif (filter_body := filter_.get(InvalidFilter._filter_name)) is not None:
            return InvalidFilter(
                previously_referenced_properties=filter_body["previouslyReferencedProperties"],
                filter_type=filter_body["filterType"],
            )
        else:
            filter_name, filter_body = next(iter(filter_.items()))
            return UnknownFilter(filter_name, filter_body)

    @abstractmethod
    def _filter_body(self, camel_case_property: bool) -> list | dict: ...

    def _involved_filter_types(self) -> set[type[Filter]]:
        output = {type(self)}
        if isinstance(self, CompoundFilter):
            for filter_ in self._filters:
                output.update(filter_._involved_filter_types())
        return output


class UnknownFilter(Filter):
    def __init__(self, filter_name: str, filter_body: dict[str, Any]) -> None:
        self.__actual_filter_name = filter_name
        self.__actual_filter_body = filter_body

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        if camel_case_property:
            return convert_all_keys_to_camel_case(self.__actual_filter_body)
        else:
            return self.__actual_filter_body

    def dump(self, camel_case_property: bool = False) -> dict[str, Any]:
        return {self.__actual_filter_name: self._filter_body(camel_case_property)}


class InvalidFilter(Filter):
    _filter_name = "invalid"

    def __init__(self, previously_referenced_properties: list[list[str]], filter_type: str) -> None:
        self.__previously_reference_properties = previously_referenced_properties
        self.__filter_type = filter_type

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        body = {
            "previously_referenced_properties": self.__previously_reference_properties,
            "filter_type": self.__filter_type,
        }
        if camel_case_property:
            return convert_all_keys_to_camel_case(body)
        else:
            return body

    def dump(self, camel_case_property: bool = False) -> dict[str, Any]:
        return {self._filter_name: self._filter_body(camel_case_property)}


def _validate_filter(
    filter: Filter | dict[str, Any] | None, supported_filters: frozenset[type[Filter]], api_name: str
) -> None:
    if filter is None or isinstance(filter, dict):
        return
    elif not isinstance(filter, Filter):
        raise TypeError(f"Invalid filter type: {type(filter)}. Must be Filter or dict")

    if not_supported := (filter._involved_filter_types() - supported_filters):
        names = [f.__name__ for f in not_supported]
        raise ValueError(f"The filters {names} are not supported for {api_name}")


class CompoundFilter(Filter, ABC):
    _filter_name: str

    def __init__(self, *filters: Filter) -> None:
        if not_flt := [flt for flt in filters if not isinstance(flt, Filter)]:
            raise TypeError(f"One or more invalid filters, expected Filter, got: {not_flt}")
        self._filters = filters

    def _filter_body(self, camel_case_property: bool) -> list | dict:
        return [filter_.dump(camel_case_property) for filter_ in self._filters]


class FilterWithProperty(Filter, ABC):
    _filter_name: str

    def __init__(self, property: PropertyReference) -> None:
        self._property = property

    def _dump_property(self, camel_case: bool) -> list[str] | tuple[str, ...]:
        return _dump_property(self._property, camel_case)

    def _filter_body(self, camel_case_property: bool) -> dict:
        return {"property": self._dump_property(camel_case_property)}


class FilterWithPropertyAndValue(FilterWithProperty, ABC):
    _filter_name: str

    def __init__(self, property: PropertyReference, value: FilterValue) -> None:
        super().__init__(property)
        self._value = value

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {"property": self._dump_property(camel_case_property), "value": _dump_filter_value(self._value)}


class FilterWithPropertyAndValueList(FilterWithProperty, ABC):
    _filter_name: str

    def __init__(self, property: PropertyReference, values: FilterValueList) -> None:
        super().__init__(property)
        self._values = values

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {"property": self._dump_property(camel_case_property), "values": _dump_filter_value(self._values)}


@final
class And(CompoundFilter):
    """A filter that combines multiple filters with a logical AND.

    Args:
        *filters (Filter): The filters to combine.

    Example:
        A filter that combines an Equals and an In filter:

        - Using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import And, Equals, In
            >>> flt = And(
            ...     Equals(("space", "view_xid/version", "some_property"), 42),
            ...     In(("space", "view_xid/version", "another_property"), ["a", "b", "c"]))

        - Using the ``View.as_property_ref`` method to reference the property:

            >>> from cognite.client.data_classes.filters import And, Equals, In
            >>> flt = And(
            ...     Equals(my_view.as_property_ref("some_property"), 42),
            ...     In(my_view.as_property_ref("another_property"), ["a", "b", "c"]))
    """

    _filter_name = "and"


@final
class Or(CompoundFilter):
    """A filter that combines multiple filters with a logical OR.

    Args:
        *filters (Filter): The filters to combine.

    Example:
        A filter that combines an Equals and an In filter:

        - Using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import Or, Equals, In
            >>> flt = Or(
            ...     Equals(("space", "view_xid/version", "some_property"), 42),
            ...     In(("space", "view_xid/version", "another_property"), ["a", "b", "c"]))

        - Using the ``View.as_property_ref`` method to reference the property:

            >>> flt = Or(
            ...     Equals(my_view.as_property_ref("some_property"), 42),
            ...     In(my_view.as_property_ref("another_property"), ["a", "b", "c"]))
    """

    _filter_name = "or"


@final
class Not(CompoundFilter):
    """A filter that negates another filter.

    Args:
        filter (Filter): The filter to negate.

    Example:
        A filter that negates an Equals filter:

        - Using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import Equals, Not
            >>> is_42 = Equals(("space", "view_xid/view_version", "some_property"), 42)
            >>> flt = Not(is_42)

        - Using the ``View.as_property_ref`` method to reference the property:

            >>> is_42 = Equals(my_view.as_property_ref("some_property"), 42)
            >>> flt = Not(is_42)
    """

    _filter_name = "not"

    def _filter_body(self, camel_case_property: bool) -> dict:
        return self._filters[0].dump(camel_case_property)


@final
class Nested(Filter):
    """A filter to apply to the node at the other side of a direct relation.

    Args:
        scope (PropertyReference): The direct relation property to traverse.
        filter (Filter): The filter to apply.

    Example:
        Assume you have two Views, viewA and viewB. viewA has a direct relation to viewB called "viewB-ID",
        and we want to filter the nodes on viewA based on the property "viewB-Property" on viewB.

        - A filter using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import Nested, Equals
            >>> flt = Nested(
            ...     scope=("space", "viewA_xid/view_version", "viewB-ID"),
            ...     filter=Equals(("space", "viewB_xid/view_version", "viewB-Property"), 42))

        - Composing the property reference using the ``View.as_property_ref`` method:

            >>> flt = Nested(
            ...     scope=viewA.as_property_ref("viewB-ID"),
            ...     filter=Equals(viewB.as_property_ref("viewB-Property"), 42))
    """

    _filter_name = "nested"

    def __init__(self, scope: PropertyReference, filter: Filter) -> None:
        self._scope = scope
        self._filter = filter

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {"scope": self._scope, "filter": self._filter.dump(camel_case_property)}


@final
class MatchAll(Filter):
    """A filter that matches all instances.

    Example:
        A filter that matches all instances:

            >>> from cognite.client.data_classes.filters import MatchAll
            >>> flt = MatchAll()
    """

    _filter_name = "matchAll"

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {}


@final
class HasData(Filter):
    """Return only instances that have data in the provided containers/views.

    Args:
        containers (Sequence[tuple[str, str] | ContainerId] | None): Containers to check for data.
        views (Sequence[tuple[str, str, str] | ViewId] | None): Views to check for data.

    Example:

        - Filter on having data in a specific container, by using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import HasData
            >>> flt = HasData(containers=[("some_space", "container_xid")])

        - Filter on having data in a specific view by using a ``ViewId``:

            >>> my_view = ViewId(space="some_space", external_id="view_xid", version="view_version")
            >>> flt = HasData(views=[my_view])
    """

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
    """Filters results based on a range of values.

    Args:
        property (PropertyReference): The property to filter on.
        gt (FilterValue | None): Greater than.
        gte (FilterValue | None): Greater than or equal to.
        lt (FilterValue | None): Less than.
        lte (FilterValue | None): Less than or equal to.

    Example:
        Filter that can be used to retrieve all instances with a property value greater than 42:

        - A filter using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import Range
            >>> flt = Range(("space", "view_xid/version", "some_property"), gt=42)

        - Composing the property reference using the ``View.as_property_ref`` method:

            >>> flt = Range(my_view.as_property_ref("some_property"), gt=42)
    """

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
    """Filters results based whether or not the provided range overlaps with the range given by the start and end
    properties.

    Args:
        start_property (PropertyReference): The property to filter on.
        end_property (PropertyReference): The property to filter on.
        gt (FilterValue | None): Greater than.
        gte (FilterValue | None): Greater than or equal to.
        lt (FilterValue | None): Less than.
        lte (FilterValue | None): Less than or equal to.


    Example:
        Filter that can be used to retrieve all instances with a range overlapping with (42, 100):

        - A filter using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import Overlaps
            >>> flt = Overlaps(
            ...     ("space", "view_xid/version", "some_start_property"),
            ...     ("space", "view_xid/version", "some_end_property"),
            ...     gt=42, lt=100)

        - Composing the property reference using the ``View.as_property_ref`` method:

            >>> flt = Overlaps(
            ...     my_view.as_property_ref("some_start_property"),
            ...     my_view.as_property_ref("some_end_property"),
            ...     gt=42, lt=100)
    """

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
    """Filters results based on whether the property equals the provided value.

    Args:
        property (PropertyReference): The property to filter on.
        value (FilterValue): The value to filter on.

    Example:
        Filter than can be used to retrieve items where the property value equals 42:

        - A filter using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import Equals
            >>> flt = Equals(("space", "view_xid/version", "some_property"), 42)

        - Composing the property reference using the ``View.as_property_ref`` method:

            >>> flt = Equals(my_view.as_property_ref("some_property"), 42)
    """

    _filter_name = "equals"


@final
class In(FilterWithPropertyAndValueList):
    """Filters results based on whether the property equals one of the provided values.

    When comparing two arrays, ``In`` and ``ContainsAny`` behave the same way. But ``In`` also allows you
    filter on a primitive property. i.e:

            >>> [1,2,3] in [3,4,5] => true
            >>> 1 in [1,2,3] => true

    Args:
        property (PropertyReference): The property to filter on.
        values (FilterValueList): The value(s) to filter on.

    Example:
        Filter than can be used to retrieve items where the property value equals 42 or 43 (or both):

        - A filter using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import In
            >>> filter = In(("space", "view_xid/version", "some_property"), [42, 43])

        - Composing the property reference using the ``View.as_property_ref`` method:

            >>> filter = In(my_view.as_property_ref("some_property"), [42, 43])
    """

    _filter_name = "in"


@final
class Exists(FilterWithProperty):
    """Filters results based on whether the property is set or not.

    Args:
        property (PropertyReference): The property to filter on.

    Example:
        Filter than can be used to retrieve items where the property value is set:

        - A filter using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import Exists
            >>> flt = Exists(("space", "view_xid/version", "some_property"))

        - Composing the property reference using the ``View.as_property_ref`` method:

            >>> flt = Exists(my_view.as_property_ref("some_property"))
    """

    _filter_name = "exists"


@final
class Prefix(FilterWithPropertyAndValue):
    """Prefix filter results based on whether the property starts with the provided value. When the property
    is a list, the list starts with the provided values.

    Args:
        property (PropertyReference): The property to filter on.
        value (FilterValue): The value to filter on.

    Example:
        Filter than can be used to retrieve items where the property value starts with "somePrefix":

        - A filter using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import Prefix
            >>> flt = Prefix(("space", "view_xid/version", "some_property"), "somePrefix")

        - Composing the property reference using the ``View.as_property_ref`` method:

            >>> flt = Prefix(my_view.as_property_ref("some_property"), "somePrefix")

        Filter that can be used to retrieve items where the property is a list of e.g. integers that starts with [1, 2, 3]:

            >>> flt = Prefix(my_view.as_property_ref("some_list_property"), [1, 2, 3])
    """

    _filter_name = "prefix"


@final
class ContainsAny(FilterWithPropertyAndValueList):
    """Returns results where the referenced property contains _any_ of the provided values.

    When comparing two arrays, ``In`` and ``ContainsAny`` behave the same way. But ``ContainsAny`` does
    not allow you filter on a primitive property. i.e in sudo code:

        >>> [1,2,3] in [3,4,5] => true
        >>> 1 in [1,2,3] => ERROR

    Args:
        property (PropertyReference): The property to filter on.
        values (FilterValueList): The value(s) to filter on.

    Example:
        Filter than can be used to retrieve items where the property value contains either 42 or 43:

        - A filter using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import ContainsAny
            >>> flt = ContainsAny(("space", "view_xid/version", "some_property"), [42, 43])

        - Composing the property reference using the ``View.as_property_ref`` method:

            >>> flt = ContainsAny(my_view.as_property_ref("some_property"), [42, 43])
    """

    _filter_name = "containsAny"


@final
class ContainsAll(FilterWithPropertyAndValueList):
    """Returns results where the referenced property contains _all_ of the provided values.

    Args:
        property (PropertyReference): The property to filter on.
        values (FilterValueList): The value to filter on.

    Example:
        Filter than can be used to retrieve items where the property value contains both 42 and 43:

        - A filter using a tuple to reference the property:

            >>> from cognite.client.data_classes.filters import ContainsAll
            >>> flt = ContainsAll(("space", "view_xid/version", "some_property"), [42, 43])

        - Composing the property reference using the ``View.as_property_ref`` method:

            >>> flt = ContainsAll(my_view.as_property_ref("some_property"), [42, 43])
    """

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
class InAssetSubtree(FilterWithPropertyAndValueList):
    """Filters results based on whether item/resource is connected to an asset with an ID (or external ID)
    that is in the subtree rooted at any of the provided IDs.

    Args:
        property (PropertyReference): The property to filter on, e.g. 'assetId' or 'assetExternalId'.
        values (FilterValueList): The value(s) to filter on.

    Example:

        Count the number of documents with a related asset in a subtree rooted at any of
        the specified external IDs, e.g. 'Plant_1' and 'Plant_2':

            >>> client.documents.aggregate_count(
            ...     filter=filters.InAssetSubtree(
            ...         property=DocumentProperty.asset_external_ids,
            ...         values=['Plant_1', 'Plant_2'],
            ...     )
            ... )
    """

    _filter_name = "inAssetSubtree"


@final
class Search(FilterWithPropertyAndValue):
    _filter_name = "search"


_BASIC_FILTERS: frozenset[type[Filter]] = frozenset(
    {And, Or, Not, In, Equals, Exists, Range, Prefix, ContainsAny, ContainsAll}
)


# ######################################################### #
# Custom filters below (custom meaning 'no API equivalent') #
# ######################################################### #


class SpaceFilter(FilterWithProperty):
    """Filters instances based on the space.

    Args:
        space (str | SequenceNotStr[str]): The space (or spaces) to filter on.
        instance_type (Literal['node', 'edge']): Type of instance to filter on. Defaults to "node".

    Example:
        Filter than can be used to retrieve nodes from space "space1" or "space2":

            >>> from cognite.client.data_classes.filters import SpaceFilter
            >>> flt = SpaceFilter(["space1", "space2"])

        Filter than can be used to retrieve edges only from "space3":

            >>> flt = SpaceFilter("space3", instance_type="edge")
    """

    _filter_name = "__space"

    def __init__(self, space: str | SequenceNotStr[str], instance_type: Literal["node", "edge"] = "node") -> None:
        super().__init__(property=[instance_type, "space"])
        space = [space] if isinstance(space, str) else list(space)
        single = len(space) == 1
        self._value = space[0] if single else space
        self._value_key = "value" if single else "values"
        self._filter_name = Equals._filter_name if single else In._filter_name
        self._involved_filter: set[type[Filter]] = {Equals if single else In}

    @classmethod
    def load(cls, filter_: dict[str, Any]) -> NoReturn:
        raise NotImplementedError("Custom filter 'SpaceFilter' can not be loaded")

    def _filter_body(self, camel_case_property: bool) -> dict[str, Any]:
        return {
            "property": self._dump_property(camel_case_property),
            self._value_key: _dump_filter_value(self._value),
        }

    def _involved_filter_types(self) -> set[type[Filter]]:
        return self._involved_filter
