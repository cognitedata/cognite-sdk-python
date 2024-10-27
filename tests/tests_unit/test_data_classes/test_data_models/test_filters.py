from __future__ import annotations

import re
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Literal

import pytest
from _pytest.mark import ParameterSet

import cognite.client.data_classes.filters as f
from cognite.client._api.data_modeling.instances import InstancesAPI
from cognite.client.data_classes._base import EnumProperty
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling.data_types import DirectRelationReference
from cognite.client.data_classes.filters import Filter, UnknownFilter
from tests.utils import all_subclasses

if TYPE_CHECKING:
    from cognite.client import CogniteClient


def load_and_dump_equals_data() -> Iterator[ParameterSet]:
    yield pytest.param(
        f.And,
        {
            "and": [
                {"in": {"property": ["tag"], "values": ["1001_1", "1001_1"]}},
                {"range": {"property": ["weight"], "gte": 0, "lt": 99}},
            ]
        },
        id="In and Range",
    )

    yield pytest.param(
        f.Or,
        {
            "or": [
                {"equals": {"property": ["name"], "value": "Quentin Tarantino"}},
                {"prefix": {"property": ["full_name"], "value": "Dr."}},
                {
                    "exists": {
                        "property": ["room"],
                    }
                },
            ]
        },
        id="Equals or Prefix or Exists",
    )

    yield pytest.param(
        f.Not,
        {"not": {"equals": {"property": ["scenario"], "value": "scenario7"}}},
        id="Not equals",
    )

    yield pytest.param(
        f.Range,
        {"range": {"property": ["weight"], "lte": 100, "gt": 0}},
        id="Range, lte and gt",
    )

    yield pytest.param(
        f.And,
        {
            "and": [
                {
                    "overlaps": {
                        "startProperty": ["start_time"],
                        "endProperty": ["end_time"],
                        "gt": "2020-01-01T00:00:00Z",
                        "lte": "2021-01-01T00:00:00Z",
                    }
                },
                {"hasData": [{"type": "view", "space": "space", "externalId": "viewExternalId", "version": "v1"}]},
            ]
        },
        id="And hasData and overlaps",
    )

    yield pytest.param(
        f.Or,
        {
            "or": [
                {
                    "overlaps": {
                        "startProperty": ["start_time"],
                        "endProperty": ["end_time"],
                        "gte": "2020-01-01T00:00:00Z",
                    }
                },
                {"containsAny": {"property": ["tag"], "values": ["1001_1", "1001_1"]}},
            ]
        },
        id="Or overlaps or containsAny",
    )
    yield pytest.param(
        f.Nested,
        {
            "nested": {
                "scope": ("space", "container", "prop"),
                "filter": {"matchAll": {}},
            }
        },
        id="Nested match all",
    )

    yield pytest.param(
        f.Range,
        {"range": {"property": ["weight"], "gte": {"property": ["capacity"]}}},
        id="Range with gte another property",
    )

    yield pytest.param(
        f.Prefix, {"prefix": {"property": ["name"], "value": {"parameter": "param1"}}}, id="prefix with parameters"
    )
    yield pytest.param(
        f.Prefix,
        {
            "prefix": {
                "property": ["cdf_cdm", "CogniteAsset/v1", "path"],
                "value": [
                    {"space": "s", "externalId": "0"},
                    {"space": "s", "externalId": "1"},
                    {"space": "s", "externalId": "2"},
                    {"space": "s", "externalId": "3"},
                ],
            }
        },
        id="prefix with list of dicts",
    )
    yield pytest.param(
        f.Prefix,
        {
            "prefix": {
                "property": ["cdf_cdm", "CogniteAsset/v1", "path"],
                "value": [
                    {"space": "s", "externalId": "0"},
                    {"space": "s", "externalId": "1"},
                    {"space": "s", "externalId": "2"},
                    {"space": "s", "externalId": "3"},
                ],
            }
        },
        id="prefix with list of objects",
    )
    yield pytest.param(
        f.InAssetSubtree,
        {
            "inAssetSubtree": {
                "property": ["assetIds"],
                "values": [11, 22],
            }
        },
        id="InAssetSubtree with list of asset ids",
    )


@pytest.mark.parametrize("expected_filter_cls, raw_data", list(load_and_dump_equals_data()))
def test_load_and_dump_equals(expected_filter_cls: type[Filter], raw_data: dict) -> None:
    parsed = Filter.load(raw_data)
    assert isinstance(parsed, expected_filter_cls)
    dumped = parsed.dump(camel_case_property=False)
    assert dumped == raw_data


def dump_filter_test_data() -> Iterator[ParameterSet]:
    user_filter = f.Or(
        f.Equals(property=["person", "name"], value=["Quentin", "Tarantino"]),
        f.ContainsAny(property=["person", "name"], values=[["Quentin", "Tarantino"]]),
    )
    expected: dict[str, Any] = {
        "or": [
            {"equals": {"property": ["person", "name"], "value": ["Quentin", "Tarantino"]}},
            {"containsAny": {"property": ["person", "name"], "values": [["Quentin", "Tarantino"]]}},
        ]
    }

    yield pytest.param(user_filter, expected, id="Or with Equals and ContainsAny")

    complex_filter = f.And(
        f.Nested(scope=("space", "container", "prop"), filter=f.MatchAll()),
        f.Or(
            f.HasData(containers=[("space", "container")]),
            f.Overlaps(
                start_property=["space", "container", "prop1"],
                end_property=["space", "container", "prop2"],
                lt=f.ParameterValue("lt"),
            ),
        ),
    )
    expected = {
        "and": [
            {
                "nested": {
                    "scope": ("space", "container", "prop"),
                    "filter": {"matchAll": {}},
                }
            },
            {
                "or": [
                    {"hasData": [{"type": "container", "space": "space", "externalId": "container"}]},
                    {
                        "overlaps": {
                            "startProperty": ["space", "container", "prop1"],
                            "endProperty": ["space", "container", "prop2"],
                            "lt": {"parameter": "lt"},
                        }
                    },
                ]
            },
        ]
    }
    yield pytest.param(complex_filter, expected, id="And nested and Or with has data and overlaps")

    snake_cased_property = f.And(
        f.Range(
            property=ViewId("space", "viewExternalId", "v1").as_property_ref("start_time"),
            lte="2023-09-16T15:50:05.439",
        )
    )
    expected = {
        "and": [
            {
                "range": {
                    "property": ("space", "viewExternalId/v1", "start_time"),
                    "lte": "2023-09-16T15:50:05.439",
                }
            }
        ]
    }
    yield pytest.param(snake_cased_property, expected, id="And range filter with snake cased property")

    yield pytest.param(
        f.InvalidFilter([["some", "old", "prop"]], "overlaps"),
        {"invalid": {"previously_referenced_properties": [["some", "old", "prop"]], "filter_type": "overlaps"}},
    )

    property_ref = ["cdf_cdm", "CogniteAsset/v1", "path"]
    expected = {
        "prefix": {
            "property": property_ref,
            "value": [{"space": "s", "externalId": "0"}, {"space": "s", "externalId": "1"}],
        }
    }
    prop_list1 = f.Prefix(
        property_ref,
        [DirectRelationReference(space="s", external_id="0"), DirectRelationReference(space="s", external_id="1")],
    )
    prop_list2 = f.Prefix(
        property_ref,
        [{"space": "s", "externalId": "0"}, {"space": "s", "externalId": "1"}],
    )
    yield pytest.param(prop_list1, expected, id="Prefix filter with list property of objects")
    yield pytest.param(prop_list2, expected, id="Prefix filter with list property of dicts")
    overloaded_filter = f.Equals(property="name", value="bob") & f.HasData(
        containers=[("space", "container")]
    ) | ~f.Range(property="size", gt=0)
    expected = {
        "or": [
            {
                "and": [
                    {"equals": {"property": ["name"], "value": "bob"}},
                    {"hasData": [{"type": "container", "space": "space", "externalId": "container"}]},
                ]
            },
            {"not": {"range": {"property": ["size"], "gt": 0}}},
        ]
    }
    yield pytest.param(overloaded_filter, expected, id="Compound filter with overloaded operators")
    nested_overloaded_filter = (f.Equals("a", "b") & f.Equals("c", "d")) & (f.Equals("e", "f") & f.Equals("g", "h"))
    expected = {
        "and": [
            {"equals": {"property": ["a"], "value": "b"}},
            {"equals": {"property": ["c"], "value": "d"}},
            {"equals": {"property": ["e"], "value": "f"}},
            {"equals": {"property": ["g"], "value": "h"}},
        ]
    }
    yield pytest.param(nested_overloaded_filter, expected, id="Compound filter with nested overloaded and")


@pytest.mark.parametrize("user_filter, expected", list(dump_filter_test_data()))
def test_dump_filter(user_filter: Filter, expected: dict) -> None:
    actual = user_filter.dump()

    assert actual == expected


def test_unknown_filter_type() -> None:
    unknown = Filter.load({"unknown": {}})
    assert isinstance(unknown, f.UnknownFilter)
    assert unknown.dump() == {"unknown": {}}


@pytest.mark.parametrize("property_cls", filter(lambda cls: hasattr(cls, "metadata_key"), all_subclasses(EnumProperty)))
def test_user_given_metadata_keys_are_not_camel_cased(property_cls: type) -> None:
    # Bug prior to 6.32.4 would dump user given keys in camelCase
    flt = f.Equals(property_cls.metadata_key("key_foo_Bar_baz"), "value_foo Bar_baz")  # type: ignore [attr-defined]
    dumped = flt.dump(camel_case_property=True)["equals"]

    # property may contain more (static) values, so we just verify the end:
    assert dumped["property"][-2:] == ["metadata", "key_foo_Bar_baz"]
    assert dumped["value"] == "value_foo Bar_baz"


def test_not_filter_only_accepts_a_single_filter() -> None:
    # Bug prior to 7.63.10: Not-filter would accept and ignore all-but-the-first filter given:
    err_msg = re.escape("Not.__init__() takes 2 positional arguments but 3 were given")
    with pytest.raises(TypeError, match=f"^{err_msg}$"):
        f.Not(f.Exists("foo"), f.Exists("bar"))  # type: ignore [call-arg]


@pytest.mark.parametrize("compound_flt", [f.And, f.Or, f.Not])
def test_compound_filters_require_at_least_one_filter(compound_flt: type[f.CompoundFilter]) -> None:
    # Bug prior to 7.63.10: CompoundFilters (and/or/not) would accept no filters given:
    if compound_flt is f.Not:
        err_msg = re.escape("Not.__init__() missing 1 required positional argument: 'filter'")
    else:
        err_msg = "^At least one filter must be provided$"
    with pytest.raises(TypeError, match=err_msg):
        compound_flt()


@pytest.mark.parametrize("space", (None, "s1", ["s1", "s2"]))
@pytest.mark.parametrize("user_filter", (None, f.Exists("x1"), f.And(f.Exists("x1"), f.Exists("x2"))))
@pytest.mark.parametrize("pass_as_dict", (True, False))
def test_merge_space_into_filter(
    space: str | list[str] | None, user_filter: f.Filter | None, pass_as_dict: bool
) -> None:
    flt_used = user_filter.dump() if pass_as_dict and user_filter else user_filter
    res = InstancesAPI._merge_space_into_filter(
        instance_type="node",
        space=space,
        filter=flt_used,
    )
    if space is user_filter is None:
        assert res is None
    elif space is None:
        assert res == flt_used
    elif user_filter is None:
        assert isinstance(res, f.SpaceFilter)
    else:
        assert isinstance(res, f.And)
        f1, *fs = res._filters
        assert isinstance(f1, f.SpaceFilter)
        # Ensure that the user filter if passed as an And-filter, is merged, rather than nested:
        for flt in fs:
            assert isinstance(flt, f.Exists)


class TestSpaceFilter:
    @pytest.mark.parametrize(
        "inst_type, space, expected",
        (
            ("node", "myspace", {"equals": {"property": ["node", "space"], "value": "myspace"}}),
            (None, ["myspace"], {"equals": {"property": ["node", "space"], "value": "myspace"}}),
            ("edge", ["myspace"], {"equals": {"property": ["edge", "space"], "value": "myspace"}}),
            ("node", ["myspace", "another"], {"in": {"property": ["node", "space"], "values": ["myspace", "another"]}}),
            ("node", ("myspace", "another"), {"in": {"property": ["node", "space"], "values": ["myspace", "another"]}}),
        ),
    )
    def test_space_filter(
        self, inst_type: Literal["node", "edge"], space: str | list[str], expected: dict[str, Any]
    ) -> None:
        space_filter = f.SpaceFilter(space, inst_type) if inst_type else f.SpaceFilter(space)
        assert expected == space_filter.dump()

    def test_space_filter_passes_isinstance_checks(self) -> None:
        space_filter = f.SpaceFilter("myspace", "edge")
        assert isinstance(space_filter, Filter)

    @pytest.mark.parametrize(
        "body",
        (
            {"property": ["edge", "space"], "value": "myspace"},
            {"property": ["node", "space"], "values": ["myspace", "another"]},
        ),
    )
    def test_space_filter_loads_as_unknown(self, body: dict[str, str | list[str]]) -> None:
        # Space Filter is an SDK concept, so it should load as an UnknownFilter:
        dumped = {f.SpaceFilter._filter_name: body}
        loaded_flt = Filter.load(dumped)
        assert isinstance(loaded_flt, UnknownFilter)

    @pytest.mark.parametrize(
        "space_filter",
        [
            f.SpaceFilter("s1", "edge"),
            f.SpaceFilter(["s1"], "edge"),
            f.SpaceFilter(["s1", "s2"], "edge"),
        ],
    )
    def test_space_filter_passes_verification(self, cognite_client: CogniteClient, space_filter: f.SpaceFilter) -> None:
        cognite_client.data_modeling.instances._validate_filter(space_filter)
        assert True
