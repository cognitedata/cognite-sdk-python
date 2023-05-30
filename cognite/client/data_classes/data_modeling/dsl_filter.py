from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List, Literal, Union, get_args

from cognite.client.data_classes.data_modeling.shared import load_reference

__all__ = [
    "RawPropertyList",
    "ReferencePropertyValue",
    "FilterObject",
    "FilterPropertyObject",
    "EqualObject",
    "InObject",
    "RangeValue",
    "RangeObject",
    "ParameterizedPropertyValue",
    "PrefixObject",
    "ExistsObject",
    "ContainsAnyObject",
    "ContainsAllObject",
    "OverlapObject",
    "Logical",
    "ComplexOperation",
    "PropertyOperation",
    "FilterKey",
    "DSLFilter",
    "NestedObject",
    "load_dsl_filter",
    "dump_dsl_filter",
]

RawPropertyList = Union[List[float], List[bool], List[str]]
RawPropertyValue = Union[str, float, bool, dict, RawPropertyList]


@dataclass
class FilterObject:
    @classmethod
    def load(cls, data: dict) -> FilterObject:
        return cls(**data)


@dataclass
class FilterPropertyObject(FilterObject):
    property: list[str]


@dataclass
class ReferencePropertyValue:
    property: list[str]


@dataclass
class EqualObject(FilterPropertyObject):
    value: RawPropertyValue | ReferencePropertyValue


@dataclass
class InObject(FilterPropertyObject):
    values: RawPropertyList | ReferencePropertyValue


RangeValue = Union[str, int, float]


@dataclass
class RangeObject(FilterPropertyObject):
    gte: RangeValue | None = None
    gt: RangeValue | None = None
    lte: RangeValue | None = None
    lt: RangeValue | None = None


@dataclass
class ParameterizedPropertyValue:
    parameter: str


@dataclass
class PrefixObject(FilterPropertyObject):
    value: str | ParameterizedPropertyValue


@dataclass
class ExistsObject(FilterPropertyObject):
    ...


@dataclass
class ContainsAnyObject(FilterPropertyObject):
    values: list[RawPropertyList | ReferencePropertyValue]


@dataclass
class ContainsAllObject(FilterPropertyObject):
    values: list[RawPropertyList | ReferencePropertyValue]


@dataclass
class OverlapObject(FilterObject):
    start_property: list[str]
    end_property: list[str]
    gte: RangeValue | None = None
    gt: RangeValue | None = None
    lte: RangeValue | None = None
    lt: RangeValue | None = None


Logical = Literal["and", "or", "not"]
LOGICAL_SET = set(get_args(Logical))


PropertyOperation = Literal[
    "equals",
    "in",
    "range",
    "prefix",
    "exists",
    "containsAny",
    "containsAll",
]

ComplexOperation = Literal["matchAll", "nested", "overlaps", "hasData"]
Operation = Union[PropertyOperation, ComplexOperation]

PROPERTY_OPERATION_SET = set(get_args(PropertyOperation))

FilterKey = Union[Logical, Operation]

FILTER_KEY_SET = set(get_args(FilterKey))


DSLFilter = Dict[FilterKey, Union[List, Dict]]


@dataclass
class NestedObject(FilterObject):
    scope: list[str]
    filter: DSLFilter


def load_dsl_filter(data: dict) -> DSLFilter:
    dsl_filter: DSLFilter = {}

    for key, value in data.items():
        if key in LOGICAL_SET:
            parsed = [load_dsl_filter(entry) for entry in value]
        elif key in PROPERTY_OPERATION_SET or key in {"nested", "overlaps"}:
            parsed = {  # type: ignore[attr-defined]
                "equals": EqualObject,
                "in": InObject,
                "range": RangeObject,
                "prefix": PrefixObject,
                "exists": ExistsObject,
                "containsAny": ContainsAnyObject,
                "containsAll": ContainsAllObject,
                "overlaps": OverlapObject,
                "nested": NestedObject,
            }[key].load(value)
        elif key == "matchAll":
            parsed = load_dsl_filter(data)  # type: ignore[assignment]
        elif key == "hasData":
            parsed = [load_reference(v) for v in data]  # type: ignore[misc]
        else:
            raise NotImplementedError()
        dsl_filter[key] = parsed
    return dsl_filter


def dump_dsl_filter(filter_: DSLFilter | None) -> dict | None:
    if filter_ is None:
        return None
    dump = {}
    for key, value in filter_.items():
        if key in LOGICAL_SET:
            dump[key] = [dump_dsl_filter(entry) for entry in value]
        elif key in PROPERTY_OPERATION_SET:
            dump[key] = asdict(value, dict_factory=lambda x: {k: v for (k, v) in x if v is not None})  # type: ignore[return-value, arg-type]
        else:
            raise ValueError(f"Invalid filter key {key}. Supported {FILTER_KEY_SET}")
    return dump
