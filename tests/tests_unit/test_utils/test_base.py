import json

import pytest

from cognite.client._utils.resource_base import CogniteResource, CogniteResourceList, CogniteResponse


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

    def test_load(self):
        assert MyResource(1).dump() == MyResource._load({"varA": 1}).dump()
        assert MyResource(1, 2).dump() == MyResource._load({"var_a": 1, "var_b": 2}).dump()
        with pytest.raises(AttributeError, match="'var_c' does not exist"):
            MyResource._load({"var_a": 1, "var_c": 1}).dump()

    def test_load_unknown_attribute(self):
        with pytest.raises(AttributeError, match="var_c"):
            MyResource._load({"varA": 1, "varB": 2, "varC": 3})

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


class MyResourceList(CogniteResourceList):
    _RESOURCE = MyResource


class TestCogniteResourceList:
    def test_dump(self):
        assert [{"var_a": 1, "var_b": 2}, {"var_a": 2, "var_b": 3}] == MyResourceList(
            [MyResource(1, 2), MyResource(2, 3)]
        ).dump()

    def test_load(self):
        resource_list = MyResourceList._load([{"varA": 1, "varB": 2}, {"varA": 2, "varB": 3}, {"varA": 3}])

        assert {"var_a": 1, "var_b": 2} == resource_list[0].dump()
        assert [{"var_a": 1, "var_b": 2}, {"var_a": 2, "var_b": 3}, {"var_a": 3}] == resource_list.dump()

    def test_load_unknown_attribute(self):
        with pytest.raises(AttributeError, match="var_c"):
            MyResourceList._load([{"varA": 1, "varB": 2, "varC": 3}])

    def test_indexing(self):
        resource_list = MyResourceList([MyResource(1, 2), MyResource(2, 3)])
        assert MyResource(1, 2) == resource_list[0]
        assert MyResource(2, 3) == resource_list[1]
        assert [MyResource(1, 2), MyResource(2, 3)] == resource_list[:]

    def test_len(self):
        resource_list = MyResourceList([MyResource(1, 2), MyResource(2, 3)])
        assert 2 == len(resource_list)

    def test_eq(self):
        assert MyResourceList([MyResource(1, 2), MyResource(2, 3)]) == MyResourceList(
            [MyResource(1, 2), MyResource(2, 3)]
        )
        assert MyResourceList([MyResource(1, 2), MyResource(2, 3)]) != MyResourceList(
            [MyResource(2, 3), MyResource(1, 2)]
        )
        assert MyResourceList([MyResource(1, 2), MyResource(2, 3)]) != MyResourceList(
            [MyResource(2, 3), MyResource(1, 4)]
        )
        assert MyResourceList([MyResource(1, 2), MyResource(2, 3)]) != MyResourceList([MyResource(1, 2)])

    def test_iter(self):
        resource_list = MyResourceList([MyResource(1, 2), MyResource(2, 3)])
        counter = 0
        for resource in resource_list:
            counter += 1
            assert resource in [MyResource(1, 2), MyResource(2, 3)]
        assert 2 == counter

    def test_constructor_bad_type(self):
        with pytest.raises(TypeError, match="must be of type MyResource"):
            MyResourceList([1, 2, 3])


class MyResponse(CogniteResponse):
    def __init__(self, x):
        self.x = x


class TestCogniteResponse:
    def test_load(self):
        res = MyResponse.load({"data": {"items": []}})
