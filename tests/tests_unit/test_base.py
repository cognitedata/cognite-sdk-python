from __future__ import annotations

import json
import re
from dataclasses import is_dataclass
from decimal import Decimal
from inspect import getsource, signature
from typing import Any, Dict, List
from unittest import mock

import pytest

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import Token
from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteResponse,
    CogniteUpdate,
)
from cognite.client.exceptions import CogniteMissingClientError
from tests.utils import all_subclasses


@pytest.fixture
def simple_mock_client():
    # We allow the mock to pass isinstance checks
    (client := mock.MagicMock()).__class__ = CogniteClient
    return client


class MyResource(CogniteResource):
    def __init__(self, var_a=None, var_b=None, id=None, external_id=None, cognite_client=None):
        self.var_a = var_a
        self.var_b = var_b
        self.id = id
        self.external_id = external_id
        self._cognite_client = cognite_client

    def use(self):
        return self._cognite_client


class MyUpdate(CogniteUpdate):
    @property
    def string(self):
        return PrimitiveUpdate(self, "string")

    @property
    def list(self):
        return ListUpdate(self, "list")

    @property
    def object(self):
        return ObjectUpdate(self, "object")

    @property
    def labels(self):
        return LabelUpdate(self, "labels")

    @property
    def columns(self):
        # Not really a PrimitiveUpdate, but we have this to ensure it is skipped from updates
        return PrimitiveUpdate(self, "columns")


class PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> MyUpdate:
        return self._set(value)


class ObjectUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> MyUpdate:
        return self._set(value)

    def add(self, value: Dict) -> MyUpdate:
        return self._add(value)

    def remove(self, value: List) -> MyUpdate:
        return self._remove(value)


class ListUpdate(CogniteListUpdate):
    def set(self, value: List) -> MyUpdate:
        return self._set(value)

    def add(self, value: List) -> MyUpdate:
        return self._add(value)

    def remove(self, value: List) -> MyUpdate:
        return self._remove(value)


class LabelUpdate(CogniteLabelUpdate):
    def add(self, value: List) -> MyUpdate:
        return self._add(value)

    def remove(self, value: List) -> MyUpdate:
        return self._remove(value)


class MyFilter(CogniteFilter):
    def __init__(self, var_a=None, var_b=None):
        self.var_a = var_a
        self.var_b = var_b


class MyResourceList(CogniteResourceList):
    _RESOURCE = MyResource

    def use(self):
        return self._cognite_client


class MyResponse(CogniteResponse):
    def __init__(self, var_a=None):
        self.var_a = var_a

    @classmethod
    def _load(cls, api_response):
        data = api_response["data"]
        return cls(data["varA"])


class TestVerifyAllCogniteSubclasses:
    @pytest.mark.parametrize("subclass", all_subclasses(CogniteResource))
    def test_all_cognite_resource_subclasses(self, subclass):
        if subclass.__init__ is CogniteResource.__init__:
            return  # all good, no method override

        src, accepts_client = "", False
        if is_dataclass(subclass):
            if hasattr(subclass, "__post_init__"):
                src = getsource(subclass.__post_init__)
                accepts_client = "cognite_client" in signature(subclass.__post_init__).parameters

        elif subclass.__init__ is not object.__init__:
            src = getsource(subclass.__init__)
            accepts_client = "cognite_client" in signature(subclass.__init__).parameters

        # TODO(haakonvt): All classes that do not accept cognite_client should most likely be CogniteResponse
        if accepts_client:
            # Make sure all subclasses set cognite_client:
            if "self._cognite_client" not in src:
                # Passing to super().__init__ is fine:
                match = re.search(
                    r"super\(\)\.__init__\(.*cognite_client=cognite_client.*\)", "".join(src.splitlines())
                )
                assert match is not None

    @pytest.mark.parametrize("subclass", all_subclasses(CogniteResponse))
    def test_all_cognite_response_subclasses(self, subclass):
        assert "cognite_client" not in signature(subclass.__init__).parameters

    @pytest.mark.parametrize("subclass", all_subclasses(CogniteFilter))
    def test_all_cognite_filter_subclasses(self, subclass):
        assert "cognite_client" not in signature(subclass.__init__).parameters


class TestCogniteResource:
    def test_dump(self):
        assert {"var_a": 1} == MyResource(1).dump()
        assert {"var_a": 1} == MyResource(1).dump(camel_case=False)

    def test_dump_camel_case(self):
        assert {"varA": 1} == MyResource(1).dump(camel_case=True)

    def test_load(self):
        assert MyResource(1).dump() == MyResource._load({"varA": 1}, cognite_client=None).dump()
        assert MyResource(1, 2).dump() == MyResource._load({"varA": 1, "varB": 2}, cognite_client=None).dump()
        assert {"var_a": 1} == MyResource._load({"varA": 1, "varC": 1}, cognite_client=None).dump()

    def test_load_unknown_attribute(self):
        resource = MyResource._load({"varA": 1, "varB": 2, "varC": 3}, cognite_client=None).dump()
        assert resource == {"var_a": 1, "var_b": 2}

    def test_load_object_attr(self):
        resource = MyResource._load({"varA": 1, "varB": {"camelCase": 1}}, cognite_client=None).dump()
        assert resource == {"var_a": 1, "var_b": {"camelCase": 1}}

    def test_eq(self, simple_mock_client):
        assert MyResource(1, "s") == MyResource(1, "s")
        assert MyResource(1, "s") == MyResource(1, "s", cognite_client=simple_mock_client)
        assert MyResource() == MyResource()
        assert MyResource(1, "s") != MyResource(1)
        assert MyResource(1, "s") != MyResource(2, "t")

    @pytest.mark.parametrize("client", (None, CogniteClient(ClientConfig("client_name", "project", "credentials"))))
    def test_accepted_values_for_cognite_client(self, client):
        MyResource(cognite_client=client)

    @pytest.mark.parametrize("client", ("no", object(), mock.MagicMock()))
    def test_raises_if_given_bad_cognite_client(self, client):
        with pytest.raises(AttributeError):
            MyResource(cognite_client=client)

    def test_str_repr(self):
        assert json.dumps({"var_a": 1}, indent=4) == str(MyResource(1))
        assert json.dumps({"var_a": 1.0}, indent=4) == str(MyResource(Decimal(1)))

    @pytest.mark.dsl
    def test_to_pandas(self):
        import pandas as pd

        class SomeResource(CogniteResource):
            def __init__(self, a_list, ob, ob_expand, ob_ignore, prim, prim_ignore):
                self.a_list = a_list
                self.ob = ob
                self.ob_expand = ob_expand
                self.ob_ignore = ob_ignore
                self.prim = prim
                self.prim_ignore = prim_ignore

        expected_df = pd.DataFrame(columns=["value"])
        expected_df.loc["prim"] = ["abc"]
        expected_df.loc["aList"] = [[1, 2, 3]]
        expected_df.loc["ob"] = [{"x": "y"}]
        expected_df.loc["md_key"] = ["md_value"]

        res = SomeResource([1, 2, 3], {"x": "y"}, {"md_key": "md_value"}, {"bla": "bla"}, "abc", 1)
        actual_df = res.to_pandas(expand=["obExpand"], ignore=["primIgnore", "obIgnore"], camel_case=True)
        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)
        res.to_pandas()

    @pytest.mark.dsl
    def test_to_pandas_no_camels(self):
        import pandas as pd

        class SomeResource(CogniteResource):
            def __init__(self):
                self.snakes_are_better_anyway = 42

        expected_df = pd.DataFrame(columns=["value"])
        expected_df.loc["snakes_are_better_anyway"] = [42]

        actual_df = SomeResource().to_pandas(camel_case=False)
        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True)

    def test_resource_client_correct(self):
        c = CogniteClient(ClientConfig(client_name="bla", project="bla", credentials=Token("bla")))
        with pytest.raises(CogniteMissingClientError):
            MyResource(1)._cognite_client
        assert MyResource(1, cognite_client=c)._cognite_client == c

    def test_use_method_which_requires_cognite_client__client_not_set(self):
        mr = MyResource()
        with pytest.raises(CogniteMissingClientError):
            mr.use()


class TestCogniteResourceList:
    def test_dump(self):
        assert [{"var_a": 1, "var_b": 2}, {"var_a": 2, "var_b": 3}] == MyResourceList(
            [MyResource(1, 2), MyResource(2, 3)]
        ).dump()

    @pytest.mark.dsl
    def test_to_pandas(self):
        import pandas as pd

        resource_list = MyResourceList([MyResource(1), MyResource(2, 3)])
        expected_df = pd.DataFrame({"varA": [1, 2], "varB": [None, 3]})
        pd.testing.assert_frame_equal(resource_list.to_pandas(camel_case=True), expected_df)

    @pytest.mark.dsl
    def test_to_pandas_no_camels(self):
        import pandas as pd

        resource_list = MyResourceList([MyResource(1), MyResource(2, 3)])
        expected_df = pd.DataFrame({"var_a": [1, 2], "var_b": [None, 3]})
        pd.testing.assert_frame_equal(resource_list.to_pandas(camel_case=False), expected_df)

    def test_load(self):
        resource_list = MyResourceList._load(
            [{"varA": 1, "varB": 2}, {"varA": 2, "varB": 3}, {"varA": 3}],
            cognite_client=None,
        )

        assert {"var_a": 1, "var_b": 2} == resource_list[0].dump()
        assert [{"var_a": 1, "var_b": 2}, {"var_a": 2, "var_b": 3}, {"var_a": 3}] == resource_list.dump()

    def test_load_unknown_attribute(self):
        assert [{"var_a": 1, "var_b": 2}] == MyResourceList._load([{"varA": 1, "varB": 2, "varC": 3}]).dump()

    def test_indexing(self):
        resource_list = MyResourceList([MyResource(1, 2), MyResource(2, 3)])
        assert MyResource(1, 2) == resource_list[0]
        assert MyResource(2, 3) == resource_list[1]
        assert MyResourceList([MyResource(1, 2), MyResource(2, 3)]) == resource_list[:]
        assert isinstance(resource_list[:], MyResourceList)

    def test_slice_list_client_remains(self, simple_mock_client):
        rl = MyResourceList([MyResource(1, 2)], cognite_client=simple_mock_client)
        rl_sliced = rl[:]
        assert rl._cognite_client == rl_sliced._cognite_client

    def test_extend__no_identifiers(self):
        resource_list = MyResourceList([MyResource(1, 2), MyResource(2, 3)])
        another_resource_list = MyResourceList([MyResource(4, 5), MyResource(6, 7)])
        resource_list.extend(another_resource_list)

        expected = MyResourceList([MyResource(1, 2), MyResource(2, 3), MyResource(4, 5), MyResource(6, 7)])
        assert expected == resource_list
        assert resource_list._id_to_item == {}
        assert resource_list._external_id_to_item == {}

    def test_extend__with_identifiers(self):
        resource_list = MyResourceList([MyResource(id=1, external_id="2"), MyResource(id=2, external_id="3")])
        another_resource_list = MyResourceList([MyResource(id=4, external_id="5"), MyResource(id=6, external_id="7")])
        resource_list.extend(another_resource_list)

        expected = MyResourceList(
            [
                MyResource(id=1, external_id="2"),
                MyResource(id=2, external_id="3"),
                MyResource(id=4, external_id="5"),
                MyResource(id=6, external_id="7"),
            ]
        )
        assert expected == resource_list
        assert expected._id_to_item == resource_list._id_to_item
        assert expected._external_id_to_item == resource_list._external_id_to_item

    def test_extend__fails_with_overlapping_identifiers(self):
        resource_list = MyResourceList([MyResource(id=1), MyResource(id=2)])
        another_resource_list = MyResourceList([MyResource(id=2), MyResource(id=6)])
        with pytest.raises(ValueError, match="^Unable to extend as this would introduce duplicates$"):
            resource_list.extend(another_resource_list)

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

    def test_get_item_by_id(self):
        resource_list = MyResourceList([MyResource(id=1, external_id="1"), MyResource(id=2, external_id="2")])
        assert MyResource(id=1, external_id="1") == resource_list.get(id=1)
        assert MyResource(id=2, external_id="2") == resource_list.get(id=2)

    def test_str_repr(self):
        assert json.dumps([{"var_a": 1}], indent=4) == str(MyResourceList([MyResource(1)]))
        assert json.dumps([{"var_a": 1.0}], indent=4) == str(MyResourceList([MyResource(Decimal(1))]))

    def test_get_item_by_external_id(self):
        resource_list = MyResourceList([MyResource(id=1, external_id="1"), MyResource(id=2, external_id="2")])
        assert MyResource(id=1, external_id="1") == resource_list.get(external_id="1")
        assert MyResource(id=2, external_id="2") == resource_list.get(external_id="2")

    def test_constructor_bad_type(self):
        with pytest.raises(TypeError, match="must be of type 'MyResource'"):
            MyResourceList([1, 2, 3])

    def test_resource_list_client_correct(self):
        c = CogniteClient(ClientConfig(client_name="bla", project="bla", credentials=Token("bla")))
        with pytest.raises(CogniteMissingClientError):
            MyResource(1)._cognite_client
        assert MyResource(1, cognite_client=c)._cognite_client == c

    def test_use_method_which_requires_cognite_client__client_not_set(self):
        mr = MyResourceList([])
        with pytest.raises(CogniteMissingClientError):
            mr.use()


class TestCogniteFilter:
    def test_dump(self):
        assert MyFilter(1, 2).dump() == {"var_a": 1, "var_b": 2}
        assert MyFilter(1, 2).dump(camel_case=True) == {"varA": 1, "varB": 2}

    def test_eq(self):
        assert MyFilter(1, 2) == MyFilter(1, 2)
        assert MyFilter(1) != MyFilter(1, 2)
        assert MyFilter() == MyFilter()

    def test_raises_if_given_cognite_client(self):
        with pytest.raises(TypeError, match=r"^__init__\(\) got an unexpected keyword argument 'cognite_client'$"):
            MyFilter(1, 2, cognite_client=mock.MagicMock())

    def test_str(self):
        assert json.dumps({"var_a": 1}, indent=4) == str(MyFilter(1))
        assert json.dumps({"var_a": 1.0}, indent=4) == str(MyFilter(Decimal(1)))

    def test_repr(self):
        assert json.dumps({"var_a": 1}, indent=4) == repr(MyFilter(1))

    def test_filter_no_cogclient_ref(self):
        # CogniteResponse does not have a reference to the cognite client:
        with pytest.raises(AttributeError):
            MyFilter()._cognite_client


class TestCogniteUpdate:
    def test_dump_id(self):
        assert {"id": 1, "update": {}} == MyUpdate(id=1).dump()

    def test_dump_external_id(self):
        assert {"externalId": "1", "update": {}} == MyUpdate(external_id="1").dump()

    def test_dump_both_ids_set(self):
        assert {"id": 1, "update": {}} == MyUpdate(id=1, external_id="1").dump()

    def test_eq(self):
        assert MyUpdate() == MyUpdate()
        assert MyUpdate(1) == MyUpdate(1)
        assert MyUpdate(1).string.set("1") == MyUpdate(1).string.set("1")
        assert MyUpdate(1) != MyUpdate(2)
        assert MyUpdate(1) != MyUpdate(1).string.set("1")

    def test_str(self):
        assert json.dumps(MyUpdate(1).dump(), indent=4) == str(MyUpdate(1))
        assert json.dumps(MyUpdate(1.0).dump(), indent=4) == str(MyUpdate(Decimal(1)))
        assert json.dumps(MyUpdate(1).string.set("1").dump(), indent=4) == str(MyUpdate(1).string.set("1"))

    def test_set_string(self):
        assert {"id": 1, "update": {"string": {"set": "bla"}}} == MyUpdate(1).string.set("bla").dump()

    def test_add_to_list(self):
        assert {"id": 1, "update": {"list": {"add": [1, 2, 3]}}} == MyUpdate(1).list.add([1, 2, 3]).dump()

    def test_set_list(self):
        assert {"id": 1, "update": {"list": {"set": [1, 2, 3]}}} == MyUpdate(1).list.set([1, 2, 3]).dump()

    def test_remove_from_list(self):
        assert {"id": 1, "update": {"list": {"remove": [1, 2, 3]}}} == MyUpdate(1).list.remove([1, 2, 3]).dump()

    def test_set_object(self):
        assert {"id": 1, "update": {"object": {"set": {"key": "value"}}}} == MyUpdate(1).object.set(
            {"key": "value"}
        ).dump()

    def test_add_object(self):
        assert {"id": 1, "update": {"object": {"add": {"key": "value"}}}} == MyUpdate(1).object.add(
            {"key": "value"}
        ).dump()

    def test_add_or_remove_after_set_raises_error(self):
        update = MyUpdate(1).object.set({"key": "value"})
        with pytest.raises(AssertionError):
            update.object.add({"key2": "value2"})
        with pytest.raises(AssertionError):
            update.object.remove(["key2"])

    def test_set_after_add_or_removeraises_error(self):
        update = MyUpdate(1).object.add({"key": "value"})
        with pytest.raises(AssertionError):
            update.object.set({"key2": "value2"})

    def test_add_object_and_remove(self):
        update = MyUpdate(1).object.add({"key": "value"})
        update.object.remove(["key2"])
        assert {"id": 1, "update": {"object": {"add": {"key": "value"}, "remove": ["key2"]}}} == update.dump()
        with pytest.raises(AssertionError):
            update.object.add({"key": "overwrite"})
        with pytest.raises(AssertionError):
            update.object.remove(["key2", "key4"])

    def test_remove_object(self):
        assert {"id": 1, "update": {"object": {"remove": ["value"]}}} == MyUpdate(1).object.remove(["value"]).dump()

    def test_set_string_null(self):
        assert {"externalId": "1", "update": {"string": {"setNull": True}}} == MyUpdate(external_id="1").string.set(
            None
        ).dump()

    def test_chain_setters(self):
        assert {"id": 1, "update": {"object": {"set": {"bla": "bla"}}, "string": {"set": "bla"}}} == MyUpdate(
            id=1
        ).object.set({"bla": "bla"}).string.set("bla").dump()

    def test_get_update_properties(self):
        props = MyUpdate._get_update_properties()
        assert hasattr(MyUpdate, "columns") and "columns" not in props
        assert {"string", "list", "object", "labels"} == set(props)


class TestCogniteResponse:
    def test_load(self):
        res = MyResponse._load({"data": {"varA": 1}})
        assert 1 == res.var_a

    def test_dump(self):
        assert {"var_a": 1} == MyResponse(1).dump()
        assert {"varA": 1} == MyResponse(1).dump(camel_case=True)
        assert {} == MyResponse().dump()

    def test_str(self):
        assert json.dumps(MyResponse(1).dump(), indent=4, sort_keys=True) == str(MyResponse(1))
        assert json.dumps(MyResponse(1.0).dump(), indent=4, sort_keys=True) == str(MyResponse(Decimal(1)))

    def test_repr(self):
        assert json.dumps(MyResponse(1).dump(), indent=4, sort_keys=True) == repr(MyResponse(1))

    def test_eq(self):
        assert MyResponse(1) == MyResponse(1)
        assert MyResponse(1) != MyResponse(2)
        assert MyResponse(1) != MyResponse()

    def test_raises_if_given_cognite_client(self):
        with pytest.raises(TypeError, match=r"^__init__\(\) got an unexpected keyword argument 'cognite_client'$"):
            MyResponse(1, cognite_client=mock.MagicMock())

    def test_response_no_cogclient_ref(self):
        # CogniteResponse does not have a reference to the cognite client:
        with pytest.raises(AttributeError):
            MyResponse(1)._cognite_client
