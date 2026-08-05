from __future__ import annotations

import pytest

from cognite.client.data_classes import filters
from cognite.client.data_classes.data_modeling.aggregates import (
    Aggregate,
    Average,
    Count,
    Filters,
    Max,
    Min,
    MovingFunction,
    MovingFunctions,
    NumberHistogram,
    Sum,
    TimeHistogram,
    UniqueValues,
    UnknownAggregate,
)
from cognite.client.utils._text import to_camel_case


class TestAggregateBuilders:
    """Dump-only aggregate request builders.

    Aggregate has no load/_load (aggregates are request-only bodies), so the dump output is
    the contract worth pinning down. Each test asserts a builder serializes to the exact request
    body the API expects.
    """

    def test_metric_aggregates_dump_name_and_property(self) -> None:
        prop = ["sp", "c", "temp"]
        assert Average(prop).dump() == {"avg": {"property": prop}}
        assert Min(prop).dump() == {"min": {"property": prop}}
        assert Max(prop).dump() == {"max": {"property": prop}}
        assert Sum(prop).dump() == {"sum": {"property": prop}}

    def test_metric_aggregate_accepts_tuple_property(self) -> None:
        assert Average(("sp", "c", "temp")).dump() == {"avg": {"property": ["sp", "c", "temp"]}}

    def test_count_without_property_dumps_empty_body(self) -> None:
        assert Count().dump() == {"count": {}}

    def test_count_with_property(self) -> None:
        assert Count(["sp", "c", "temp"]).dump() == {"count": {"property": ["sp", "c", "temp"]}}

    def test_unique_values_minimal(self) -> None:
        assert UniqueValues(["sp", "c", "region"]).dump() == {"uniqueValues": {"property": ["sp", "c", "region"]}}

    def test_unique_values_with_size_and_nested_aggregates(self) -> None:
        agg = UniqueValues(
            ["sp", "c", "region"],
            aggregates={"max_temp": Max(["sp", "c", "temp"])},
            size=5,
        )
        assert agg.dump() == {
            "uniqueValues": {
                "property": ["sp", "c", "region"],
                "aggregates": {"max_temp": {"max": {"property": ["sp", "c", "temp"]}}},
                "size": 5,
            }
        }

    def test_number_histogram_minimal(self) -> None:
        assert NumberHistogram(["sp", "c", "salary"], interval=1000).dump() == {
            "numberHistogram": {"property": ["sp", "c", "salary"], "interval": 1000}
        }

    def test_number_histogram_with_bounds_and_nested_aggregates(self) -> None:
        agg = NumberHistogram(
            ["sp", "c", "salary"],
            interval=1000,
            aggregates={"sum_salary": Sum(["sp", "c", "salary"])},
            hard_bounds={"min": 0, "max": 10000},
        )
        assert agg.dump() == {
            "numberHistogram": {
                "property": ["sp", "c", "salary"],
                "interval": 1000,
                "aggregates": {"sum_salary": {"sum": {"property": ["sp", "c", "salary"]}}},
                "hardBounds": {"min": 0, "max": 10000},
            }
        }

    def test_time_histogram_calendar_interval(self) -> None:
        assert TimeHistogram(["sp", "c", "ts"], calendar_interval="1d").dump() == {
            "timeHistogram": {"property": ["sp", "c", "ts"], "calendarInterval": "1d"}
        }

    def test_time_histogram_fixed_interval_and_bounds(self) -> None:
        agg = TimeHistogram(
            ["sp", "c", "ts"],
            fixed_interval="12h",
            hard_bounds={"min": "2024-01-01", "max": "2024-02-01"},
        )
        assert agg.dump() == {
            "timeHistogram": {
                "property": ["sp", "c", "ts"],
                "fixedInterval": "12h",
                "hardBounds": {"min": "2024-01-01", "max": "2024-02-01"},
            }
        }

    def test_time_histogram_requires_exactly_one_interval(self) -> None:
        match = "Exactly one of calendar_interval or fixed_interval"
        with pytest.raises(ValueError, match=match):
            TimeHistogram(["sp", "c", "ts"])  # neither
        with pytest.raises(ValueError, match=match):
            TimeHistogram(["sp", "c", "ts"], calendar_interval="1d", fixed_interval="12h")  # both

    def test_filters_with_filter_objects_and_raw_dicts(self) -> None:
        agg = Filters(
            filters=[filters.Range(["createdTime"], gte=1), {"matchAll": {}}],
            aggregates={"total": Count()},
        )
        assert agg.dump() == {
            "filters": {
                "filters": [
                    {"range": {"property": ["createdTime"], "gte": 1}},
                    {"matchAll": {}},
                ],
                "aggregates": {"total": {"count": {}}},
            }
        }

    def test_moving_function(self) -> None:
        agg = MovingFunction(buckets_path="_count", window=3, function=MovingFunctions.UNWEIGHTED_AVG)
        assert agg.dump() == {
            "movingFunction": {
                "bucketsPath": "_count",
                "window": 3,
                "function": "MovingFunctions.unweightedAvg",
            }
        }

    def test_moving_function_coerces_raw_wire_string(self) -> None:
        # `function` is typed as the enum, but MovingFunctions is a str enum, so an untyped caller
        # passing the wire string still lands on the same enum member rather than a bare str.
        from_enum = MovingFunction("_count", 3, function=MovingFunctions.UNWEIGHTED_AVG)
        from_str = MovingFunction("_count", 3, function="MovingFunctions.unweightedAvg")  # type: ignore[arg-type]
        assert from_str.function is MovingFunctions.UNWEIGHTED_AVG
        assert from_enum.dump() == from_str.dump()
        assert from_enum.dump()["movingFunction"]["function"] == "MovingFunctions.unweightedAvg"

    def test_moving_function_rejects_unknown_function(self) -> None:
        with pytest.raises(ValueError):
            MovingFunction("_count", 3, function="MovingFunctions.median")  # type: ignore[arg-type]

    def test_nested_aggregates_accept_raw_dicts(self) -> None:
        # Values under `aggregates` may be typed builders or raw dicts; both dump through.
        agg = UniqueValues(["sp", "c", "region"], aggregates={"raw": {"count": {}}})
        assert agg.dump() == {
            "uniqueValues": {
                "property": ["sp", "c", "region"],
                "aggregates": {"raw": {"count": {}}},
            }
        }

    def test_dump_is_always_camel_case(self) -> None:
        # Request builders always emit camelCase keys; the camel_case flag is a no-op for them.
        agg = TimeHistogram(["sp", "c", "ts"], calendar_interval="1d")
        expected = {"timeHistogram": {"property": ["sp", "c", "ts"], "calendarInterval": "1d"}}
        assert agg.dump(camel_case=True) == expected
        assert agg.dump(camel_case=False) == expected

    def test_eq(self) -> None:
        assert Average(["sp", "c", "temp"]) == Average(["sp", "c", "temp"])
        assert Average(["sp", "c", "temp"]) != Average(["sp", "c", "other"])
        assert Average(["sp", "c", "temp"]) != Max(["sp", "c", "temp"])

    def test_load_roundtrips_every_builder(self) -> None:
        # v7 makes load public so request builders round-trip through config files: the reloaded
        # object is the same type and dumps identically to the original.
        prop = ["sp", "c", "temp"]
        builders: list[Aggregate] = [
            Average(prop),
            Min(prop),
            Max(prop),
            Sum(prop),
            Count(),
            Count(prop),
            UniqueValues(["sp", "c", "region"], aggregates={"m": Max(prop)}, size=5),
            NumberHistogram(prop, interval=10, hard_bounds={"min": 0, "max": 100}),
            TimeHistogram(["sp", "c", "ts"], calendar_interval="1d"),
            TimeHistogram(["sp", "c", "ts"], fixed_interval="12h"),
            Filters(filters=[filters.Range(["createdTime"], gte=1)], aggregates={"n": Count()}),
            MovingFunction(buckets_path="games", window=7, function=MovingFunctions.UNWEIGHTED_AVG),
        ]
        for builder in builders:
            reloaded = Aggregate.load(builder.dump())
            assert type(reloaded) is type(builder)
            assert reloaded.dump() == builder.dump()

    def test_load_unknown_aggregate_falls_back_to_unknown_aggregate(self) -> None:
        # Unknown/newer aggregate types round-trip via UnknownAggregate (a real Aggregate)
        # rather than crashing, so the SDK can lag the API.
        payload = {"futureAggregate": {"property": ["sp", "c", "x"]}}
        loaded = Aggregate.load(payload)
        assert isinstance(loaded, UnknownAggregate)
        assert isinstance(loaded, Aggregate)
        assert loaded.dump() == payload


class TestMovingFunctionsEnum:
    """The wire values are spelled out per member, so nothing but a test catches a typo in one.

    A misspelled value is not caught by the type checker (both sides are ``str``) and not caught by
    any dump assertion either, since ``dump()`` just echoes ``.value`` — it would only surface as a
    400 from the API. Hence pinning both halves of every value.
    """

    def test_every_value_carries_the_namespace_prefix(self) -> None:
        for member in MovingFunctions:
            assert member.value.startswith("MovingFunctions."), f"{member.name} is missing the prefix"

    def test_every_value_matches_its_member_name(self) -> None:
        # Guards the half the prefix check cannot: MovingFunctions.unweigthedAvg would pass above.
        for member in MovingFunctions:
            _, _, suffix = member.value.partition(".")
            assert suffix == to_camel_case(member.name.lower()), f"{member.name} does not match {suffix!r}"

    def test_members_cover_the_documented_functions(self) -> None:
        assert {member.value for member in MovingFunctions} == {
            "MovingFunctions.max",
            "MovingFunctions.min",
            "MovingFunctions.sum",
            "MovingFunctions.unweightedAvg",
            "MovingFunctions.linearWeightedAvg",
        }

    def test_unprefixed_suffix_also_resolves(self) -> None:
        # _missing_ lets callers skip the "MovingFunctions." namespace prefix.
        assert MovingFunctions("sum") is MovingFunctions.SUM
        assert MovingFunctions("unweightedAvg") is MovingFunctions.UNWEIGHTED_AVG

    def test_unprefixed_unknown_suffix_still_raises(self) -> None:
        with pytest.raises(ValueError):
            MovingFunctions("median")


class TestCoexistenceWithTheAggregationsModule:
    """``data_classes.aggregations`` defines its own ``Average``, ``Count``, ``Min``, ``Max`` and
    ``Sum`` for ``instances.aggregate``, with different property semantics.

    Sharing those names across the two modules is only safe as long as neither leaks into a namespace
    holding the other, so these tests pin the reachability of both.
    """

    def test_dm_aggregations_attribute_is_the_instances_module(self) -> None:
        # Guards against anyone adding cognite/client/data_classes/data_modeling/aggregations.py:
        # importing such a submodule would rebind this attribute and break `dm.aggregations.Average`.
        from cognite.client.data_classes import aggregations
        from cognite.client.data_classes import data_modeling as dm
        from cognite.client.data_classes.data_modeling import instances

        assert dm.aggregations is aggregations
        assert dm.Aggregation is aggregations.Aggregation
        assert dm.AggregatedValue is aggregations.AggregatedValue
        # An incidental re-export rather than a declared one, hence getattr - but it is public today
        # and must keep pointing at the same class.
        assert getattr(instances, "AggregatedNumberedValue") is aggregations.AggregatedNumberedValue

    def test_aggregates_are_not_exported_into_namespaces(self) -> None:
        # This module is deliberately reachable only by its own path, so the names it shares with
        # data_classes.aggregations can never collide.
        from cognite.client import data_classes as dc
        from cognite.client.data_classes import data_modeling as dm

        for name in ("Aggregate", "Average", "Count", "Min", "Max", "Sum", "UniqueValues", "MovingFunction"):
            assert not hasattr(dm, name), f"{name} leaked into cognite.client.data_classes.data_modeling"
            assert not hasattr(dc, name), f"{name} leaked into cognite.client.data_classes"

    def test_the_two_aggregate_families_are_disjoint(self) -> None:
        # Both families dispatch on the same wire keys, so load() must stay family-scoped.
        from cognite.client.data_classes import aggregations

        assert not issubclass(Aggregate, aggregations.Aggregation)
        assert not issubclass(aggregations.Aggregation, Aggregate)
        assert type(aggregations.Aggregation.load({"avg": {"property": "height"}})) is aggregations.Average
        assert type(Aggregate.load({"avg": {"property": ["sp", "c", "temp"]}})) is Average
