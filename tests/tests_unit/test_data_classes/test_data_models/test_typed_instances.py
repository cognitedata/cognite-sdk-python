from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pytest

from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    NodeList,
    NodeOrEdgeData,
    ViewId,
)
from cognite.client.data_classes.data_modeling.cdm.v1 import (
    CogniteAsset,
    CogniteAssetApply,
    CogniteDescribableEdgeApply,
)
from cognite.client.data_classes.data_modeling.instances import (
    Properties,
    PropertyOptions,
    TypedEdge,
    TypedEdgeApply,
    TypedNode,
    TypedNodeApply,
)


class PersonProperties:
    birth_date = PropertyOptions(identifier="birthDate")


class Person(TypedNodeApply, PersonProperties):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str,
        birth_date: date,
        email: str,
        siblings: list[DirectRelationReference] | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            type=DirectRelationReference("sp_model_space", "person"),
            sources=[
                NodeOrEdgeData(
                    source=self.get_source(),
                    properties={"birthDate": birth_date, "email": email, "name": name, "siblings": siblings or []},
                )
            ],
        )
        self.name = name
        self.birth_date = birth_date
        self.email = email
        self.siblings = siblings

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_model_space", "view_id", "1")


class FlowProperties:
    flow_rate = PropertyOptions(identifier="flowRate")


class FlowWrite(TypedEdgeApply, FlowProperties):
    def __init__(
        self,
        external_id: str,
        start_node: DirectRelationReference | tuple[str, str],
        end_node: DirectRelationReference | tuple[str, str],
        flow_rate: float,
    ) -> None:
        super().__init__(
            "sp_my_fixed_space",
            external_id,
            DirectRelationReference("sp_model_space", "Flow"),
            start_node,
            end_node,
            sources=[NodeOrEdgeData(source=self.get_source(), properties={"flowRate": flow_rate})],
        )
        self.flow_rate = flow_rate

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_model_space", "flow", "1")


class Flow(TypedEdge, FlowProperties):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        type: DirectRelationReference,
        last_updated_time: int,
        created_time: int,
        flow_rate: float,
        start_node: DirectRelationReference,
        end_node: DirectRelationReference,
        deleted_time: int | None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            type,
            last_updated_time,
            created_time,
            start_node,
            end_node,
            deleted_time,
            properties=Properties({self.get_source(): {"flowRate": flow_rate}}),
        )
        self.flow_rate = flow_rate

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_model_space", "flow", "1")


class PersonRead(TypedNode, PersonProperties):
    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        name: str,
        birth_date: date,
        email: str,
        siblings: list[DirectRelationReference] | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
        deleted_time: int | None = None,
    ) -> None:
        super().__init__(
            space,
            external_id,
            version,
            last_updated_time,
            created_time,
            deleted_time,
            properties=Properties.from_typed_properties(
                self.get_source(),
                {
                    "name": name,
                    "birthDate": birth_date,
                    "email": email,
                    "siblings": siblings,
                },
            ),
            type=type,
        )
        self.name = name
        self.birth_date = birth_date
        self.email = email
        self.siblings = siblings

    def as_write(self) -> Person:
        return Person(self.space, self.external_id, self.name, self.birth_date, self.email, self.siblings)

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_model_space", "view_id", "1")


class Asset(TypedNodeApply):
    type_ = PropertyOptions(identifier="type")

    def __init__(self, external_id: str, name: str, type_: str) -> None:
        super().__init__(
            "sp_my_fixed_space",
            external_id,
            type=DirectRelationReference("sp_model_space", "asset"),
            sources=[NodeOrEdgeData(source=self.get_source(), properties={"name": name, "type": type_})],
        )
        self.name = name
        self.type_ = type_

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("sp_model_space", "Asset", "1")


class TestTypedNodeApply:
    def test_dump_person(self) -> None:
        person = Person("sp_my_fixed_space", "my_external_id", "John Doe", date(1990, 1, 1), "example@cognite.com")
        expected = {
            "space": "sp_my_fixed_space",
            "externalId": "my_external_id",
            "instanceType": "node",
            "type": {"space": "sp_model_space", "externalId": "person"},
            "sources": [
                {
                    "source": {"space": "sp_model_space", "externalId": "view_id", "version": "1", "type": "view"},
                    "properties": {
                        "name": "John Doe",
                        "birthDate": "1990-01-01",
                        "email": "example@cognite.com",
                        "siblings": [],
                    },
                }
            ],
        }

        assert person.dump() == expected
        loaded = Person.load(expected)
        assert person.dump() == loaded.dump()

    def test_dump_load_asset(self) -> None:
        asset = Asset("my_external_id", "My Asset", "Pump")
        expected = {
            "space": "sp_my_fixed_space",
            "externalId": "my_external_id",
            "instanceType": "node",
            "type": {"space": "sp_model_space", "externalId": "asset"},
            "sources": [
                {
                    "source": {"space": "sp_model_space", "externalId": "Asset", "version": "1", "type": "view"},
                    "properties": {"name": "My Asset", "type": "Pump"},
                }
            ],
        }

        assert asset.dump() == expected
        loaded = Asset.load(expected)
        assert asset.dump() == loaded.dump()


class TestTypedEdgeApply:
    def test_dump_load_flow_write(self) -> None:
        flow = FlowWrite("my_external_id", ("sp_my_data_space", "start_node"), ("sp_my_data_space", "end_node"), 42.0)
        expected = {
            "space": "sp_my_fixed_space",
            "externalId": "my_external_id",
            "instanceType": "edge",
            "type": {"space": "sp_model_space", "externalId": "Flow"},
            "startNode": {"space": "sp_my_data_space", "externalId": "start_node"},
            "endNode": {"space": "sp_my_data_space", "externalId": "end_node"},
            "sources": [
                {
                    "source": {"space": "sp_model_space", "externalId": "flow", "version": "1", "type": "view"},
                    "properties": {"flowRate": 42.0},
                }
            ],
        }

        assert flow.dump() == expected
        loaded = FlowWrite.load(expected)
        assert flow.dump() == loaded.dump()


class TestTypedNode:
    def test_dump_load_person(self) -> None:
        person = PersonRead(
            "sp_my_fixed_space",
            "my_external_id",
            1,
            0,
            0,
            "John Doe",
            date(1990, 1, 1),
            "example@email.com",
            siblings=[
                DirectRelationReference("sp_data_space", "brother"),
                DirectRelationReference("sp_data_space", "sister"),
            ],
        )
        expected = {
            "space": "sp_my_fixed_space",
            "externalId": "my_external_id",
            "instanceType": "node",
            "version": 1,
            "lastUpdatedTime": 0,
            "createdTime": 0,
            "properties": {
                "sp_model_space": {
                    "view_id/1": {
                        "name": "John Doe",
                        "birthDate": "1990-01-01",
                        "email": "example@email.com",
                        "siblings": [
                            {"space": "sp_data_space", "externalId": "brother"},
                            {"space": "sp_data_space", "externalId": "sister"},
                        ],
                    },
                }
            },
        }

        actual = person.dump()
        assert actual == expected
        loaded = PersonRead.load(expected)
        assert person == loaded
        assert isinstance(loaded.birth_date, date)
        assert all(isinstance(sibling, DirectRelationReference) for sibling in loaded.siblings or [])

    @pytest.mark.dsl
    def test_to_pandas(self) -> None:
        import pandas as pd

        person = PersonRead(
            "sp_my_fixed_space", "my_external_id", 1, 0, 0, "John Doe", date(1990, 1, 1), "john@doe.com"
        )
        df = person.to_pandas(expand_properties=True)
        expected_df = pd.Series(
            {
                "space": "sp_my_fixed_space",
                "external_id": "my_external_id",
                "version": 1,
                "last_updated_time": pd.Timestamp("1970-01-01"),
                "created_time": pd.Timestamp("1970-01-01"),
                "instance_type": "node",
                "name": "John Doe",
                "birthDate": "1990-01-01",
                "email": "john@doe.com",
                "siblings": None,
            },
        ).to_frame(name="value")
        pd.testing.assert_frame_equal(df, expected_df)

    @pytest.mark.dsl
    def test_to_pandas_list(self) -> None:
        import pandas as pd

        person = NodeList[PersonRead](
            [PersonRead("sp_my_fixed_space", "my_external_id", 1, 0, 0, "John Doe", date(1990, 1, 1), "john@doe.com")]
        )

        df = person.to_pandas(expand_properties=True)

        pd.testing.assert_frame_equal(
            df,
            pd.DataFrame(
                {
                    "space": ["sp_my_fixed_space"],
                    "external_id": ["my_external_id"],
                    "version": [1],
                    "last_updated_time": [pd.Timestamp("1970-01-01 00:00:00")],
                    "created_time": [pd.Timestamp("1970-01-01 00:00:00")],
                    "instance_type": ["node"],
                    "name": ["John Doe"],
                    "birthDate": ["1990-01-01"],
                    "email": ["john@doe.com"],
                    "siblings": None,
                }
            ),
        )


class TestTypedEdge:
    def test_dump_load_flow(self) -> None:
        flow = Flow(
            "sp_my_fixed_space",
            "my_external_id",
            1,
            DirectRelationReference("sp_model_space", "Flow"),
            0,
            0,
            42.0,
            DirectRelationReference("sp_my_fixed_space", "start_node"),
            DirectRelationReference("sp_my_fixed_space", "end_node"),
            None,
        )
        expected = {
            "space": "sp_my_fixed_space",
            "externalId": "my_external_id",
            "instanceType": "edge",
            "version": 1,
            "lastUpdatedTime": 0,
            "createdTime": 0,
            "type": {"space": "sp_model_space", "externalId": "Flow"},
            "properties": {"sp_model_space": {"flow/1": {"flowRate": 42.0}}},
            "startNode": {"space": "sp_my_fixed_space", "externalId": "start_node"},
            "endNode": {"space": "sp_my_fixed_space", "externalId": "end_node"},
        }

        assert flow.dump() == expected
        loaded = Flow.load(expected)
        assert flow.dump() == loaded.dump()


@pytest.fixture
def cognite_asset_kwargs() -> dict[str, Any]:
    return dict(
        space="my-space",
        external_id="my-xid",
        type=DirectRelationReference("should-be", "at-root"),
        asset_type=DirectRelationReference("should-be", "in-properties"),
        source_created_time=datetime(2020, 1, 1),
    )


class TestCDMv1Classes:
    @pytest.mark.parametrize("camel_case", (True, False))
    @pytest.mark.parametrize("use_attribute_name", (True, False))
    def test_cognite_asset_read_and_write(
        self, cognite_asset_kwargs: dict[str, Any], camel_case: bool, use_attribute_name: bool
    ) -> None:
        asset_write = CogniteAssetApply(**cognite_asset_kwargs)
        asset_read = CogniteAsset(**cognite_asset_kwargs, version=1, last_updated_time=10, created_time=5)

        for is_write, asset in zip([True, False], [asset_write, asset_read]):
            dumped = asset.dump(camel_case=camel_case, use_attribute_name=use_attribute_name)  # type: ignore [call-arg]
            xid = "externalId" if camel_case else "external_id"
            assert xid in dumped
            # Check that type/type_ are correctly dumped:
            # - not starting with double underscore
            # - not ending with single underscore
            assert dumped["type"] == {"space": "should-be", xid: "at-root"}
            if is_write:
                properties = dumped["sources"][0]["properties"]
            else:
                properties = dumped["properties"]["cdf_cdm"]["CogniteAsset/v1"]
            assert properties["type"] == {"space": "should-be", xid: "in-properties"}
            # Check that properties are cased according to use_attribute_name:
            assert ("source_created_time" if use_attribute_name else "sourceCreatedTime") in properties

    @pytest.mark.dsl
    @pytest.mark.parametrize("camel_case", (True, False))
    def test_cognite_asset_read_and_write__to_pandas(
        self, cognite_asset_kwargs: dict[str, Any], camel_case: bool
    ) -> None:
        import pandas as pd

        # When calling to_pandas, `use_attribute_name = not camel_case` due to how we expect
        # attributes to be snake cased in python (in general).
        asset_write = CogniteAssetApply(**cognite_asset_kwargs)
        asset_read = CogniteAsset(**cognite_asset_kwargs, version=1, last_updated_time=10, created_time=5)

        # TODO: Implement expand_properties=True for Apply classes?
        write_df = asset_write.to_pandas(camel_case=camel_case)
        read_df = asset_read.to_pandas(expand_properties=False, camel_case=camel_case)
        read_expanded_df = asset_read.to_pandas(expand_properties=True, camel_case=camel_case)

        xid = "externalId" if camel_case else "external_id"
        for df in [write_df, read_df]:
            assert df.index.is_unique
            assert df.index[1] == xid
            assert df.at["type", "value"] == {"space": "should-be", xid: "at-root"}

        assert not read_expanded_df.index.is_unique  # because 'type' is repeated
        expected_type_df = pd.DataFrame(
            [
                ({"space": "should-be", xid: "at-root"},),
                ({"space": "should-be", xid: "in-properties"},),
            ],
            columns=["value"],
            index=["type", "type"],
        )
        pd.testing.assert_frame_equal(read_expanded_df.loc["type"], expected_type_df)
        assert ("sourceCreatedTime" if camel_case else "source_created_time") in read_expanded_df.index


@pytest.mark.parametrize(
    "name, instance",
    (
        (
            "CogniteAssetApply",
            CogniteAssetApply(
                space="foo",
                external_id="child",
                parent=("foo", "I-am-root"),
            ),
        ),
        (
            "CogniteDescribableEdgeApply",
            CogniteDescribableEdgeApply(
                space="foo",
                external_id="indescribable",
                type=DirectRelationReference("foo", "yo"),
                start_node=DirectRelationReference("foo", "yo2"),
                end_node=DirectRelationReference("foo", "yo3"),
            ),
        ),
    ),
)
def test_typed_instances_overrides_inherited_methods_from_instance_cls(
    name: str, instance: TypedNode | TypedEdge
) -> None:
    with pytest.raises(AttributeError, match=f"{name!r} object has no attribute 'get'"):
        instance.get("space")

    with pytest.raises(TypeError, match=f"{name!r} object is not subscriptable"):
        instance["foo"]

    with pytest.raises(TypeError, match=f"{name!r} object does not support item assignment"):
        instance["foo"] = "bar"

    with pytest.raises(TypeError, match=f"{name!r} object does not support item deletion"):
        del instance["external_id"]

    with pytest.raises(TypeError, match=f"argument of type {name!r} is not iterable"):
        "foo" in instance
