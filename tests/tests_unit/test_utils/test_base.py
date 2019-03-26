import json
from typing import Dict, List

import pytest

from cognite.client._utils.resource_base import (
    CogniteFilter,
    CogniteResource,
    CogniteResourceList,
    CogniteResponse,
    CogniteUpdate,
)


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


class MyFilter(CogniteFilter):
    def __init__(self, var_a=None, var_b=None):
        self.var_a = var_a
        self.var_b = var_b


class TestCogniteFilter:
    def test_dump(self):
        assert MyFilter(1, 2).dump() == {"var_a": 1, "var_b": 2}
        assert MyFilter(1, 2).dump(camel_case=True) == {"varA": 1, "varB": 2}

    def test_eq(self):
        assert MyFilter(1, 2) == MyFilter(1, 2)
        assert MyFilter(1) != MyFilter(1, 2)
        assert MyFilter() == MyFilter()

    def test_str(self):
        assert json.dumps({"var_a": 1}, indent=4) == MyFilter(1).__str__()

    def test_repr(self):
        assert json.dumps({"var_a": 1}, indent=4) == MyFilter(1).__repr__()


class MyUpdate(CogniteUpdate):
    def __init__(self, id=None, external_id=None):
        self.id = id
        self.external_id = external_id
        self._update_object = {}

    def string_set(self, value: str):
        if value is None:
            self._update_object["string"] = {"setNull": True}
            return self
        self._update_object["string"] = {"set": value}
        return self

    def list_set(self, value: List):
        if value is None:
            self._update_object["list"] = {"setNull": True}
            return self
        self._update_object["list"] = {"set": value}
        return self

    def list_add(self, value: List):
        self._update_object["list"] = {"add": value}
        return self

    def list_remove(self, value: List):
        self._update_object["list"] = {"remove": value}
        return self

    def object_set(self, value: Dict):
        if value is None:
            self._update_object["object"] = {"setNull": True}
            return self
        self._update_object["object"] = {"set": value}
        return self


class TestCogniteUpdate:
    def test_dump_id(self):
        assert {"id": 1, "update": {}} == MyUpdate(id=1).dump()

    def test_dump_external_id(self):
        assert {"externalId": 1, "update": {}} == MyUpdate(external_id=1).dump()

    def test_eq(self):
        assert MyUpdate() == MyUpdate()
        assert MyUpdate(1) == MyUpdate(1)
        assert MyUpdate(1).string_set("1") == MyUpdate(1).string_set("1")
        assert MyUpdate(1) != MyUpdate(2)
        assert MyUpdate(1) != MyUpdate(1).string_set("1")

    def test_str(self):
        assert json.dumps(MyUpdate(1).dump(), indent=4, sort_keys=True) == MyUpdate(1).__str__()
        assert (
            json.dumps(MyUpdate(1).string_set("1").dump(), indent=4, sort_keys=True)
            == MyUpdate(1).string_set("1").__str__()
        )

    def test_set_string(self):
        assert {"id": 1, "update": {"string": {"set": "bla"}}} == MyUpdate(1).string_set("bla").dump()

    def test_add_to_list(self):
        assert {"id": 1, "update": {"list": {"add": [1, 2, 3]}}} == MyUpdate(1).list_add([1, 2, 3]).dump()

    def test_set_list(self):
        assert {"id": 1, "update": {"list": {"set": [1, 2, 3]}}} == MyUpdate(1).list_set([1, 2, 3]).dump()

    def test_remove_from_list(self):
        assert {"id": 1, "update": {"list": {"remove": [1, 2, 3]}}} == MyUpdate(1).list_remove([1, 2, 3]).dump()

    def test_set_object(self):
        assert {"id": 1, "update": {"object": {"set": {"key": "value"}}}} == MyUpdate(1).object_set(
            {"key": "value"}
        ).dump()

    def test_set_string_null(self):
        assert {"externalId": 1, "update": {"string": {"setNull": True}}} == MyUpdate(external_id=1).string_set(
            None
        ).dump()

    def test_set_list_null(self):
        assert {"id": 1, "update": {"list": {"setNull": True}}} == MyUpdate(id=1).list_set(None).dump()

    def test_set_object_null(self):
        assert {"id": 1, "update": {"object": {"setNull": True}}} == MyUpdate(id=1).object_set(None).dump()

    def test_chain_setters(self):
        assert {"id": 1, "update": {"object": {"setNull": True}, "string": {"set": "bla"}}} == MyUpdate(
            id=1
        ).object_set(None).string_set("bla").dump()


class MyResponse(CogniteResponse):
    def __init__(self, x):
        self.x = x


class TestCogniteResponse:
    def test_load(self):
        res = MyResponse.load({"data": {"items": []}})
