import json

import pytest

from cognite.client._utils.resource_base import CogniteResource


class MyResource(CogniteResource):
    def __init__(self, var_a=None, var_b=None):
        self.var_a = var_a
        self.var_b = var_b


class TestCogniteResource:
    def test_dump(self):
        assert {"var_a": 1} == MyResource(1).dump()
        assert {"var_a": 1} == MyResource(1).dump(camel_case=False)

    def test_dump_camel_case(self):
        assert {"varA": 1} == MyResource(1).dump(camel_case=True)

    def test_to_camel_case(self):
        assert "camelCase" == MyResource._to_camel_case("camel_case")
        assert "camelCase" == MyResource._to_camel_case("camelCase")
        assert "a" == MyResource._to_camel_case("a")

    def test_to_snake_case(self):
        assert "snake_case" == MyResource._to_snake_case("snakeCase")
        assert "snake_case" == MyResource._to_snake_case("snake_case")
        assert "a" == MyResource._to_snake_case("a")

    def test_load(self):
        assert MyResource(1).dump() == MyResource._load({"varA": 1}).dump()
        assert MyResource(1, 2).dump() == MyResource._load({"var_a": 1, "var_b": 2}).dump()
        with pytest.raises(AttributeError, match="'var_c' does not exist"):
            MyResource._load({"var_a": 1, "var_c": 1}).dump()

    def test_load_object_attr(self):
        assert {"var_a": 1, "var_b": {"camelCase": 1}} == MyResource._load({"varA": 1, "varB": {"camelCase": 1}}).dump()

    def test_eq(self):
        assert MyResource(1, "s") == MyResource(1, "s")
        assert MyResource() == MyResource()
        assert MyResource(1, "s") != MyResource(1)
        assert MyResource(1, "s") != MyResource(2, "t")

    def test_repr(self):
        assert json.dumps({"var_a": 1}, indent=4) == MyResource(1).__repr__()

    def test_str_repr(self):
        assert json.dumps({"var_a": 1}, indent=4) == MyResource(1).__str__()

    def test_to_pandas(self):
        with pytest.raises(NotImplementedError):
            MyResource(1).to_pandas()
