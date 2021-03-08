import json
from decimal import Decimal

import pytest

from cognite.client import utils
from cognite.client.exceptions import CogniteImportError


class TestCaseConversion:
    def test_to_camel_case(self):
        assert "camelCase" == utils._auxiliary.to_camel_case("camel_case")
        assert "camelCase" == utils._auxiliary.to_camel_case("camelCase")
        assert "a" == utils._auxiliary.to_camel_case("a")

    def test_to_snake_case(self):
        assert "snake_case" == utils._auxiliary.to_snake_case("snakeCase")
        assert "snake_case" == utils._auxiliary.to_snake_case("snake_case")
        assert "a" == utils._auxiliary.to_snake_case("a")


class TestLocalImport:
    @pytest.mark.dsl
    def test_local_import_single_ok(self):
        import pandas

        assert pandas == utils._auxiliary.local_import("pandas")

    @pytest.mark.dsl
    def test_local_import_multiple_ok(self):
        import numpy
        import pandas

        assert (pandas, numpy) == utils._auxiliary.local_import("pandas", "numpy")

    def test_local_import_single_fail(self):
        with pytest.raises(CogniteImportError, match="requires 'not-a-module' to be installed"):
            utils._auxiliary.local_import("not-a-module")

    @pytest.mark.dsl
    def test_local_import_multiple_fail(self):
        with pytest.raises(CogniteImportError, match="requires 'not-a-module' to be installed"):
            utils._auxiliary.local_import("pandas", "not-a-module")

    @pytest.mark.coredeps
    def test_dsl_deps_not_installed(self):
        for dep in ["numpy", "pandas"]:
            with pytest.raises(CogniteImportError, match=dep):
                utils._auxiliary.local_import(dep)


class TestUrlEncode:
    def test_url_encode(self):
        assert "/bla/yes%2Fno/bla" == utils._auxiliary.interpolate_and_url_encode("/bla/{}/bla", "yes/no")
        assert "/bla/123/bla/456" == utils._auxiliary.interpolate_and_url_encode("/bla/{}/bla/{}", "123", "456")


class TestJsonDumpDefault:
    def test_json_serializable_Decimal(self):
        with pytest.raises(TypeError):
            json.dumps(Decimal(1))

        assert json.dumps(Decimal(1), default=utils._auxiliary.json_dump_default)

    def test_json_not_serializable_sets(self):
        with pytest.raises(TypeError):
            json.dumps({1, 2})
        with pytest.raises(TypeError):
            json.dumps({1, 2})

    @pytest.mark.dsl
    def test_json_serializable_numpy(self):
        np = utils._auxiliary.local_import("numpy")
        arr = np.array([1.2, 3.4]).astype(np.float32)
        with pytest.raises(TypeError):
            json.dumps(arr)
        with pytest.raises(TypeError):
            json.dumps(arr[0])
        with pytest.raises(TypeError):  # core sdk makes it hard to serialize np.ndarray
            assert json.dumps(arr, default=utils._auxiliary.json_dump_default)
        assert json.dumps(arr[0], default=utils._auxiliary.json_dump_default)

    def test_json_serializable_object(self):
        class Obj:
            def __init__(self):
                self.x = 1

        with pytest.raises(TypeError):
            json.dumps(Obj())

        assert json.dumps({"x": 1}) == json.dumps(Obj(), default=utils._auxiliary.json_dump_default)

    @pytest.mark.dsl
    def test_json_serialiable_numpy_integer(self):
        import numpy as np

        inputs = [np.int32(1), np.int64(1)]
        for input in inputs:
            assert json.dumps(input, default=utils._auxiliary.json_dump_default)


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
        actual_output = utils._auxiliary.split_into_chunks(input, chunk_size)
        assert len(actual_output) == len(expected_output)
        for element in expected_output:
            assert element in actual_output


class TestAssertions:
    @pytest.mark.parametrize("var, var_name, types", [(1, "var1", [int]), ("1", "var2", [int, str])])
    def test_assert_type_ok(self, var, var_name, types):
        utils._auxiliary.assert_type(var, var_name, types=types)

    @pytest.mark.parametrize("var, var_name, types", [("1", "var", [int, float]), ((1,), "var2", [dict, list])])
    def test_assert_type_fail(self, var, var_name, types):
        with pytest.raises(TypeError, match=str(types)):
            utils._auxiliary.assert_type(var, var_name, types)

    def test_assert_exactly_one_of_id_and_external_id(self):
        with pytest.raises(AssertionError):
            utils._auxiliary.assert_exactly_one_of_id_or_external_id(1, "1")
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(1, None)
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(None, "1")
