import json
import math
from decimal import Decimal
from itertools import zip_longest

import pytest

from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._auxiliary import (
    assert_type,
    find_duplicates,
    interpolate_and_url_encode,
    json_dump_default,
    local_import,
    split_into_chunks,
    split_into_n_parts,
    to_camel_case,
    to_snake_case,
)


class TestCaseConversion:
    def test_to_camel_case(self):
        assert "camelCase" == to_camel_case("camel_case")
        assert "camelCase" == to_camel_case("camelCase")
        assert "a" == to_camel_case("a")

    def test_to_snake_case(self):
        assert "snake_case" == to_snake_case("snakeCase")
        assert "snake_case" == to_snake_case("snake_case")
        assert "a" == to_snake_case("a")


class TestLocalImport:
    @pytest.mark.dsl
    def test_local_import_single_ok(self):
        import pandas

        assert pandas == local_import("pandas")

    @pytest.mark.dsl
    def test_local_import_multiple_ok(self):
        import numpy
        import pandas

        assert (pandas, numpy) == local_import("pandas", "numpy")

    def test_local_import_single_fail(self):
        with pytest.raises(CogniteImportError, match="requires 'not-a-module' to be installed"):
            local_import("not-a-module")

    @pytest.mark.dsl
    def test_local_import_multiple_fail(self):
        with pytest.raises(CogniteImportError, match="requires 'not-a-module' to be installed"):
            local_import("pandas", "not-a-module")

    @pytest.mark.coredeps
    def test_dsl_deps_not_installed(self):
        for dep in ["geopandas", "pandas", "shapely", "sympy"]:
            with pytest.raises(CogniteImportError, match=dep):
                local_import(dep)


class TestUrlEncode:
    def test_url_encode(self):
        assert "/bla/yes%2Fno/bla" == interpolate_and_url_encode("/bla/{}/bla", "yes/no")
        assert "/bla/123/bla/456" == interpolate_and_url_encode("/bla/{}/bla/{}", "123", "456")


class TestJsonDumpDefault:
    def test_json_serializable_Decimal(self):
        with pytest.raises(TypeError):
            json.dumps(Decimal(1))

        assert json.dumps(Decimal(1), default=json_dump_default)

    def test_json_not_serializable_sets(self):
        with pytest.raises(TypeError):
            json.dumps({1, 2})
        with pytest.raises(TypeError):
            json.dumps({1, 2})

    @pytest.mark.dsl
    def test_json_serializable_numpy(self):
        np = local_import("numpy")
        arr = np.array([1.2, 3.4], dtype=np.float32)
        with pytest.raises(TypeError):
            json.dumps(arr)
        with pytest.raises(TypeError):
            json.dumps(arr[0])
        with pytest.raises(TypeError):  # core sdk makes it hard to serialize np.ndarray
            assert json.dumps(arr, default=json_dump_default)
        assert json.dumps(arr[0], default=json_dump_default)

    def test_json_serializable_object(self):
        class Obj:
            def __init__(self):
                self.x = 1

        with pytest.raises(TypeError):
            json.dumps(Obj())

        assert json.dumps({"x": 1}) == json.dumps(Obj(), default=json_dump_default)

    @pytest.mark.dsl
    def test_json_serialiable_numpy_integer(self):
        import numpy as np

        inputs = [np.int32(1), np.int64(1)]
        for input in inputs:
            assert json.dumps(input, default=json_dump_default)


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


class TestAssertions:
    @pytest.mark.parametrize("var, var_name, types", [(1, "var1", [int]), ("1", "var2", [int, str])])
    def test_assert_type_ok(self, var, var_name, types):
        assert_type(var, var_name, types=types)

    @pytest.mark.parametrize("var, var_name, types", [("1", "var", [int, float]), ((1,), "var2", [dict, list])])
    def test_assert_type_fail(self, var, var_name, types):
        with pytest.raises(TypeError, match=str(types)):
            assert_type(var, var_name, types)


class TestFindDuplicates:
    @pytest.mark.parametrize("inp", ("abc", (1, 2, 3), [1.0, 1.1, 2], range(3), {1: 2, 2: 3}, set([1, 1, 1])))
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
            [set((1,)), set((1,)), set((1, 3))],
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
        res = split_into_n_parts(inp, n)
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
        res = split_into_n_parts(inp, n)
        for r, res_exp in zip_longest(res, exp_out, fillvalue=math.nan):
            assert type(r) is exp_type
            assert r == res_exp

    @pytest.mark.parametrize("inp", (set(range(5)), None))
    def test_raises_not_subscriptable(self, inp):
        res = split_into_n_parts(inp, n=2)
        with pytest.raises(TypeError, match="object is not subscriptable"):
            next(res)
