from __future__ import annotations

from dataclasses import dataclass
from typing import List, Union

from cognite.client.data_classes.data_modeling.core import ContainerReference, ViewReference


@dataclass
class AndFilter:
    and_: dict


@dataclass
class OrFilter:
    or_: dict


@dataclass
class NotFilter:
    not_: dict


BoolFilter = Union[AndFilter, OrFilter, NotFilter]


RawPropertyList = Union[List[float], List[bool], List[str]]
RawPropertyValue = Union[str, float, bool, dict, RawPropertyList]


@dataclass
class ReferencePropertyValue:
    property: list[str]


@dataclass
class EqualObject:
    property: list[str]
    value: RawPropertyValue | ReferencePropertyValue


@dataclass
class InObject:
    property: list[str]
    value: RawPropertyList | ReferencePropertyValue


RangeValue = Union[str, int, float]


@dataclass
class RangeObject:
    property: list[str]
    gte: RangeValue | None = None
    gt: RangeValue | None = None
    lte: RangeValue | None = None
    lt: RangeValue | None = None


@dataclass
class ParameterizedPropertyValue:
    parameter: str


@dataclass
class PrefixObject:
    property: list[str]
    value: str | ParameterizedPropertyValue


@dataclass
class ExistsObject:
    property: list[str]


@dataclass
class EqualsFilter:
    equals: EqualObject


@dataclass
class InFilter:
    in_: InObject


@dataclass
class RangeFilter:
    range: RangeObject


@dataclass
class PrefixFilter:
    prefix: PrefixObject


@dataclass
class ExistsFilter:
    exists: ExistsObject


@dataclass
class ContainsAnyObject:
    property: list[str]
    values: list[RawPropertyList | ReferencePropertyValue]


@dataclass
class ContainsAnyFilter:
    contains_any: ContainsAnyObject


@dataclass
class ContainsAllObject:
    property: list[str]
    values: list[RawPropertyList | ReferencePropertyValue]


@dataclass
class ContainsAllFilter:
    contains_all: ContainsAllObject


@dataclass
class MatchAllObject:
    match: dict


@dataclass
class MatchAllFilter:
    match_all: MatchAllObject


@dataclass
class NestedObject:
    scope: list[str]
    filter: dict


@dataclass
class NestedFilter:
    nested: NestedObject


@dataclass
class OverlapObject:
    start_property: list[str]
    end_property: list[str]
    gte: RangeValue | None = None
    gt: RangeValue | None = None
    lte: RangeValue | None = None
    lt: RangeValue | None = None


@dataclass
class OverlapsFilter:
    overlaps: OverlapObject


@dataclass
class HasDataFilter:
    has_data: list[ViewReference | ContainerReference]


LeafFilter = Union[
    EqualsFilter,
    InFilter,
    RangeFilter,
    PrefixFilter,
    ExistsFilter,
    ContainsAnyFilter,
    ContainsAllFilter,
    MatchAllFilter,
    NestedFilter,
    OverlapsFilter,
    HasDataFilter,
]


DMSFilter = Union[BoolFilter, LeafFilter]


def load_dsl_filter(data: dict) -> DMSFilter:
    raise NotImplementedError()
