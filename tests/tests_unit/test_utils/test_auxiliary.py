import math
import re
import warnings
from itertools import zip_longest

import pytest

from cognite.client.data_classes._base import CogniteResource
from cognite.client.utils._auxiliary import (
    exactly_one_is_not_none,
    fast_dict_load,
    find_duplicates,
    get_accepted_params,
    handle_deprecated_camel_case_argument,
    interpolate_and_url_encode,
    load_resource_to_dict,
    remove_duplicates_keep_order,
    split_into_chunks,
    split_into_n_parts,
)


@pytest.mark.parametrize(
    "new_arg, old_arg_name, fn_name, kw_dct, expected",
    (
        ("Ceci n'est pas une pipe", "extractionPipeline", "f", {}, "Ceci n'est pas une pipe"),
        (None, "extractionPipeline", "f", {"extractionPipeline": "Ceci n'est pas une pipe"}, "Ceci n'est pas une pipe"),
        (42, "raw_geoLocation", "f", {}, 42),
        (None, "raw_geoLocation", "f", {"raw_geoLocation": 42}, 42),
    ),
)
def test_handle_deprecated_camel_case_argument__expected(new_arg, old_arg_name, fn_name, kw_dct, expected):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        value = handle_deprecated_camel_case_argument(new_arg, old_arg_name, fn_name, kw_dct)
    assert value == expected


@pytest.mark.parametrize(
    "new_arg, old_arg_name, fn_name, kw_dct, err_msg",
    (
        (
            "Ceci n'est pas une pipe",
            "extractionPipeline",
            "f",
            {"owner": "René Magritte"},
            "Got unexpected keyword argument(s): ['owner']",
        ),
        (
            None,
            "extractionPipeline",
            "fun_func",
            {"extractionPipeline": None},
            "fun_func() missing 1 required positional argument: 'extraction_pipeline'",
        ),
        (
            "what",
            "extractionPipeline",
            "f",
            {"extractionPipeline": "what"},
            "Pass either 'extraction_pipeline' or 'extractionPipeline' (deprecated), not both",
        ),
    ),
)
def test_handle_deprecated_camel_case_argument__raises(new_arg, old_arg_name, fn_name, kw_dct, err_msg):
    with pytest.raises(TypeError, match=re.escape(err_msg)), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        handle_deprecated_camel_case_argument(new_arg, old_arg_name, fn_name, kw_dct)


class TestUrlEncode:
    def test_url_encode(self):
        assert "/bla/yes%2Fno/bla" == interpolate_and_url_encode("/bla/{}/bla", "yes/no")
        assert "/bla/123/bla/456" == interpolate_and_url_encode("/bla/{}/bla/{}", "123", "456")


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
    def test_split_into_chunks(self, input, chunk_size, expected_output):
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
    def test_no_duplicates(self, inp, expected):
        assert expected == remove_duplicates_keep_order(inp)


class TestFindDuplicates:
    @pytest.mark.parametrize("inp", ("abc", (1, 2, 3), [1.0, 1.1, 2], range(3), {1: 2, 2: 3}, {1, 1, 1}))
    def test_no_duplicates(self, inp):
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
    def test_has_duplicates(self, inp, exp_duplicate):
        assert exp_duplicate == find_duplicates(inp)

    @pytest.mark.parametrize(
        "inp",
        (
            ([1], [1], [1, 2]),
            [{1}, {1}, {1, 3}],
            [{1: 2}, {1: 2}, {1: 2, 2: 3}],
        ),
    )
    def test_raises_not_hashable(self, inp):
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
    def test_normal_split(self, inp, n, exp_out):
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
    def test_split_into_too_many_pieces(self, inp, n, exp_out):
        exp_type = type(inp)
        res = split_into_n_parts(inp, n=n)
        for r, res_exp in zip_longest(res, exp_out, fillvalue=math.nan):
            assert type(r) is exp_type
            assert r == res_exp

    @pytest.mark.parametrize("inp", (set(range(5)), None))
    def test_raises_not_subscriptable(self, inp):
        res = split_into_n_parts(inp, n=2)
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
    def test_exactly_one_is_not_none(self, inp, expected):
        assert exactly_one_is_not_none(*inp) == expected


class MyTestResource(CogniteResource):
    # Test resource for fast_dict_load below
    def __init__(self, foo=None, foo_bar=None, foo_bar_baz=None, cognite_client=None):
        self.foo = foo
        self.foo_bar = foo_bar
        self.foo_bar_baz = foo_bar_baz
        self._cognite_client = cognite_client

    def load(*a, **kw):
        raise NotImplementedError


class TestFastDictLoad:
    @pytest.mark.parametrize(
        "item, expected",
        (
            # Simple load test for all keys:
            ({"foo": "a", "fooBar": "b", "fooBarBaz": "c"}, MyTestResource(*"abc")),
            # Ensure unknown keys are skipped silently:
            ({"f": "a", "foot": "b", "fooBarBaz": "c"}, MyTestResource(foo_bar_baz="c")),
            # Ensure keys must be camel cased:
            ({"foo": "a", "foo_bar": "b", "foo_bar_baz": "c"}, MyTestResource(foo="a")),
        ),
    )
    def test_load(self, item, expected):
        get_accepted_params.cache_clear()  # For good measure
        assert expected == fast_dict_load(MyTestResource, item, cognite_client=None)


class TestLoadDictOrStr:
    @pytest.mark.parametrize(
        "input, expected",
        (
            ({"foo": "bar"}, {"foo": "bar"}),
            ('{"foo": "bar"}', {"foo": "bar"}),
            ("foo: bar", {"foo": "bar"}),
            ('{"foo": {"bar": "thing"}}', {"foo": {"bar": "thing"}}),
        ),
    )
    def test_load_resource_to_dict(self, input, expected):
        assert expected == load_resource_to_dict(input)

    @pytest.mark.parametrize(
        "input",
        ("foo", 100),
    )
    def test_load_resource_to_dict_raises(self, input):
        with pytest.raises(TypeError, match="Resource must be json or yaml str, or dict, not"):
            load_resource_to_dict(input)
