from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

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
    PropertySpec,
)
from cognite.client.data_classes.events import Event, EventList
from cognite.client.exceptions import CogniteMissingClientError
from tests.utils import all_subclasses


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

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("string", is_nullable=False),
            PropertySpec("list", is_container=True),
            PropertySpec("object", is_container=True),
            PropertySpec("labels", is_container=True),
            # Columns are not supported
            # PropertySpec("columns", is_nullable=False),
        ]


class PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> MyUpdate:
        return self._set(value)


class ObjectUpdate(CogniteObjectUpdate):
    def set(self, value: dict) -> MyUpdate:
        return self._set(value)

    def add(self, value: dict) -> MyUpdate:
        return self._add(value)

    def remove(self, value: list) -> MyUpdate:
        return self._remove(value)


class ListUpdate(CogniteListUpdate):
    def set(self, value: list) -> MyUpdate:
        return self._set(value)

    def add(self, value: list) -> MyUpdate:
        return self._add(value)

    def remove(self, value: list) -> MyUpdate:
        return self._remove(value)


class LabelUpdate(CogniteLabelUpdate):
    def add(self, value: list) -> MyUpdate:
        return self._add(value)

    def remove(self, value: list) -> MyUpdate:
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


class TestCogniteResource:
    def test_dump(self):
        assert {"var_a": 1} == MyResource(1).dump()
        assert {"var_a": 1} == MyResource(1).dump(camel_case=False)

    def test_dump_camel_case(self):
        assert {"varA": 1} == MyResource(1).dump(camel_case=True)

    def test_load(self):
        assert MyResource(1).dump() == MyResource._load({"varA": 1}).dump()
        assert MyResource().dump() == MyResource._load({"var_a": 1, "var_b": 2}).dump()
        assert {"var_a": 1} == MyResource._load({"varA": 1, "varC": 1}).dump()

    def test_load_unknown_attribute(self):
        assert {"var_a": 1, "var_b": 2} == MyResource._load({"varA": 1, "varB": 2, "varC": 3}).dump()

    def test_load_object_attr(self):
        assert {"var_a": 1, "var_b": {"camelCase": 1}} == MyResource._load({"varA": 1, "varB": {"camelCase": 1}}).dump()

    def test_eq(self):
        assert MyResource(1, "s") == MyResource(1, "s")
        assert MyResource(1, "s") == MyResource(1, "s", cognite_client=MagicMock(spec=CogniteClient))
        assert MyResource() == MyResource()
        assert MyResource(1, "s") != MyResource(1)
        assert MyResource(1, "s") != MyResource(2, "t")

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

    @pytest.mark.dsl
    def test_to_pandas_metadata(self):
        import pandas as pd

        event_list = EventList(
            [
                Event(external_id="ev1", metadata={"value1": 1, "value2": "hello"}),
                Event(external_id="ev2", metadata={"value1": 2, "value2": "world"}),
            ]
        )

        expected_df = pd.DataFrame(
            data={"external_id": ["ev1", "ev2"], "metadata.value1": [1, 2], "metadata.value2": ["hello", "world"]},
        )

        actual_df = event_list.to_pandas(expand_metadata=True)
        pd.testing.assert_frame_equal(expected_df, actual_df, check_like=False)

    @pytest.mark.dsl
    def test_to_pandas_metadata_some_nulls(self):
        import pandas as pd

        event_list = EventList(
            [Event(external_id="ev1", metadata={"val1": 1}), Event(external_id="ev2", metadata={"val2": 2})]
        )
        expected_df = pd.DataFrame(
            data={
                "external_id": ["ev1", "ev2"],
                "metadata.val1": [1, None],
                "metadata.val2": [None, 2],
            }
        )

        actual_df = event_list.to_pandas(expand_metadata=True)
        pd.testing.assert_frame_equal(expected_df, actual_df)

    def test_load(self):
        resource_list = MyResourceList._load([{"varA": 1, "varB": 2}, {"varA": 2, "varB": 3}, {"varA": 3}])

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

    def test_slice_list_client_remains(self):
        rl = MyResourceList([MyResource(1, 2)], cognite_client=MagicMock(spec=CogniteClient))
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

    def test_str(self):
        assert json.dumps({"var_a": 1}, indent=4) == str(MyFilter(1))
        assert json.dumps({"var_a": 1.0}, indent=4) == str(MyFilter(Decimal(1)))

    def test_repr(self):
        assert json.dumps({"var_a": 1}, indent=4) == repr(MyFilter(1))


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
        props = {prop.name for prop in MyUpdate._get_update_properties()}
        assert hasattr(MyUpdate, "columns") and "columns" not in props
        assert {"string", "list", "object", "labels"} == set(props)

    @pytest.mark.parametrize("cognite_update_subclass", all_subclasses(CogniteUpdate))
    def test_correct_implementation_get_update_properties(self, cognite_update_subclass: CogniteUpdate):
        # Arrange
        expected = sorted(
            key
            for key in cognite_update_subclass.__dict__
            if not key.startswith("_") and key not in {"columns", "dump"}
        )

        # Act
        actual = sorted(prop.name for prop in cognite_update_subclass._get_update_properties())

        # Assert
        assert expected == actual


class TestCogniteResponse:
    def test_load(self):
        # No base implementation of _load for CogniteResponse subclasses
        with pytest.raises(NotImplementedError):
            MyResponse._load({"varA": 1})

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

    def test_response_client_correct(self):
        c = CogniteClient(ClientConfig(client_name="bla", project="bla", credentials=Token("bla")))
        with pytest.raises(CogniteMissingClientError):
            MyResource(1)._cognite_client
        assert MyResource(1, cognite_client=c)._cognite_client == c

    def test_response_no_cogclient_ref(self):
        # CogniteResponse does not have a reference to the cognite client:
        with pytest.raises(AttributeError):
            MyResponse(1)._cognite_client
