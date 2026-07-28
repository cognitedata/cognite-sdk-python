from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from itertools import zip_longest
from typing import Any

import pytest

from cognite.client.utils._auxiliary import (
    exactly_one_is_not_none,
    find_duplicates,
    load_resource_to_dict,
    remove_duplicates_keep_order,
    split_into_chunks,
    split_into_n_parts,
)
from cognite.client.utils.useful_types import SequenceNotStr


class TestSplitIntoChunks:
    @pytest.mark.parametrize(
        "input, chunk_size, expected_output",
        [
            (["a", "b", "c"], 1, [["a"], ["b"], ["c"]]),
            (["a", "b", "c"], 2, [["a", "b"], ["c"]]),
            ([], 1000, []),
            (["a", "b", "c"], 3, [["a", "b", "c"]]),
            (["a", "b", "c"], 10, [["a", "b", "c"]]),
            ({"a": 1, "b": 2}, 1, [{"a": 1}, {"b": 2}]),
            ({"a": 1, "b": 2}, 2, [{"a": 1, "b": 2}]),
            ({"a": 1, "b": 2}, 3, [{"a": 1, "b": 2}]),
            ({}, 1, []),
        ],
    )
    def test_split_into_chunks(self, input: SequenceNotStr, chunk_size: int, expected_output: Sequence) -> None:
        actual_output = split_into_chunks(input, chunk_size)
        assert len(actual_output) == len(expected_output)
        for element in expected_output:
            assert element in actual_output


class TestRemoveDuplicatesKeepOrder:
    @pytest.mark.parametrize(
        "inp, expected",
        (
            ([], []),
            ((1, 1, 2, 1), [1, 2]),
            ("abccba", ["a", "b", "c"]),
        ),
    )
    def test_no_duplicates(self, inp: Iterable, expected: list) -> None:
        assert expected == remove_duplicates_keep_order(inp)


class TestFindDuplicates:
    @pytest.mark.parametrize("inp", ("abc", (1, 2, 3), [1.0, 1.1, 2], range(3), {1: 2, 2: 3}, {1, 1, 1}))
    def test_no_duplicates(self, inp: Iterable) -> None:
        assert set() == find_duplicates(inp)

    @pytest.mark.parametrize(
        "inp, exp_duplicate",
        (
            ("abca", {"a"}),
            ("x" * 20, {"x"}),
            ((1, 2, 2.0, 3), {2}),
            ([-0, 0.0, 1.0, 1.1], {0}),
            ([math.nan, math.nan], {math.nan}),  # Hmmm
            ([None, int, print, lambda s: s, print], {print}),  # Hmmm again
            ([frozenset((1,)), frozenset((1,)), frozenset((1, 3))], {frozenset((1,))}),
        ),
    )
    def test_has_duplicates(self, inp: Iterable, exp_duplicate: set) -> None:
        assert exp_duplicate == find_duplicates(inp)

    @pytest.mark.parametrize(
        "inp",
        (
            ([1], [1], [1, 2]),
            [{1}, {1}, {1, 3}],
            [{1: 2}, {1: 2}, {1: 2, 2: 3}],
        ),
    )
    def test_raises_not_hashable(self, inp: Iterable) -> None:
        with pytest.raises(TypeError, match="unhashable type:"):
            find_duplicates(inp)


class TestSplitIntoNParts:
    @pytest.mark.parametrize(
        "inp, n, exp_out",
        (
            ("abcd", 2, ("ac", "bd")),
            ("abcd", 3, ("ad", "b", "c")),
            (list("abcd"), 1, (list("abcd"),)),
            (list("abcd"), 2, (["a", "c"], ["b", "d"])),
            ((1, None, "a"), 2, ((1, "a"), (None,))),
            (range(10), 3, (range(0, 10, 3), range(1, 10, 3), range(2, 10, 3))),
        ),
    )
    def test_normal_split(self, inp: Sequence, n: int, exp_out: Sequence) -> None:
        exp_type = type(inp)
        res = split_into_n_parts(inp, n=n)
        for r, res_exp in zip_longest(res, exp_out, fillvalue=math.nan):
            assert type(r) is exp_type
            assert r == res_exp

    @pytest.mark.parametrize(
        "inp, n, exp_out",
        (
            ("abc", 4, ("a", "b", "c", "")),
            (list("abc"), 4, (["a"], ["b"], ["c"], [])),
            ((1, None), 5, ((1,), (None,), (), (), ())),
            (range(1), 3, (range(0, 1, 3), range(1, 1, 3), range(2, 1, 3))),
        ),
    )
    def test_split_into_too_many_pieces(self, inp: Sequence, n: int, exp_out: Sequence) -> None:
        exp_type = type(inp)
        res = split_into_n_parts(inp, n=n)
        for r, res_exp in zip_longest(res, exp_out, fillvalue=math.nan):
            assert type(r) is exp_type
            assert r == res_exp

    @pytest.mark.parametrize("inp", (set(range(5)), None))
    def test_raises_not_subscriptable(self, inp: set[int]) -> None:
        res = split_into_n_parts(inp, n=2)  # type: ignore[call-overload]
        with pytest.raises(TypeError, match="object is not subscriptable"):
            next(res)


class TestExactlyOneIsNotNone:
    @pytest.mark.parametrize(
        "inp, expected",
        (
            ((None, None), False),
            ((1, "123"), False),
            ((None, 1), True),
            (("123", None), True),
            ((None, None, 123), True),
            ((1, 2, None), False),
            ((1,), True),
            ((None,), False),
        ),
    )
    def test_exactly_one_is_not_none(self, inp: tuple, expected: bool) -> None:
        assert exactly_one_is_not_none(*inp) == expected


class TestLoadDictOrStr:
    @pytest.mark.parametrize(
        "input, expected",
        (
            ({"foo": "bar"}, {"foo": "bar"}),
            ({"foo": None}, {"foo": None}),
            ('{"foo": "bar"}', {"foo": "bar"}),
            ('{"foo": null}', {"foo": None}),
            ("foo: bar", {"foo": "bar"}),
            ('{"foo": {"bar": "thing"}}', {"foo": {"bar": "thing"}}),
        ),
    )
    def test_load_resource_to_dict(self, input: str | dict, expected: dict) -> None:
        assert expected == load_resource_to_dict(input)

    @pytest.mark.parametrize("input", ("foo", 100))
    def test_load_resource_to_dict_raises(self, input: Any) -> None:
        with pytest.raises(TypeError, match="Resource must be json or yaml str, or dict, not"):
            load_resource_to_dict(input)
