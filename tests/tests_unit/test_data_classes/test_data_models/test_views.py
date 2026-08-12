from __future__ import annotations

import re

import pytest

from cognite.client.data_classes._base import UnknownCogniteResource
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    MappedPropertyApply,
    RecordViewApply,
    View,
    ViewApply,
    ViewFilter,
    ViewId,
)
from cognite.client.data_classes.data_modeling.containers import PropertyConstraintState
from cognite.client.data_classes.data_modeling.views import MappedProperty, ViewProperty, ViewPropertyApply


def make_record_view_apply(stream_id: str | list[str] = "my-stream") -> RecordViewApply:
    return RecordViewApply(
        space="sp",
        external_id="rv",
        version="v1",
        stream_id=stream_id,
        properties={
            "title": MappedPropertyApply(
                container=ContainerId("sp", "recordContainer"), container_property_identifier="title"
            )
        },
    )


def make_test_view(space: str, external_id: str, version: str, created_time: int = 1) -> View:
    return View(
        space,
        external_id,
        version,
        created_time=created_time,
        properties={},
        last_updated_time=2,
        description="",
        name="",
        filter=None,
        implements=None,
        writable=False,
        used_for="all",
        is_global=False,
    )


class TestRecordViewApplyDataClass:
    @pytest.mark.parametrize(
        "stream_id",
        [
            pytest.param("my-stream", id="singular"),
            pytest.param(["my-stream"], id="sequence"),
        ],
    )
    def test_accepts_singular_or_sequence_stream_id(self, stream_id: str | list[str]) -> None:
        view = make_record_view_apply(stream_id=stream_id)
        assert view.stream_id == ["my-stream"]

        dumped = view.dump()
        assert dumped["streamId"] == ["my-stream"]

    def test_view_apply_load_rejects_record_view_payload(self) -> None:
        dumped = make_record_view_apply().dump()
        with pytest.raises(ValueError, match=re.escape("RecordViewApply.load")):
            ViewApply._load(dumped)


class TestViewRecordFields:
    def test_load_dump_round_trip_with_stream_id(self) -> None:
        payload = {
            "space": "sp",
            "externalId": "rv",
            "version": "v1",
            "streamId": ["my-stream"],
            "createdTime": 1,
            "lastUpdatedTime": 2,
            "writable": True,
            "usedFor": "record",
            "isGlobal": False,
            "properties": {},
        }
        view = View._load(payload)
        assert view.stream_id == ["my-stream"]
        assert view.used_for == "record"
        assert view.is_record_view is True
        assert view.dump() == {**payload, "implements": []}

    def test_load_without_stream_id_leaves_it_none(self) -> None:
        view = make_test_view("sp", "v", "v1")
        assert view.stream_id is None
        assert view.is_record_view is False
        assert "streamId" not in view.dump()

    def test_as_apply_raises_when_used_for_record(self) -> None:
        view = make_test_view("sp", "rv", "v1")
        view.used_for = "record"

        with pytest.raises(ValueError, match="as_record_view_apply"):
            view.as_apply()

    def test_as_record_view_apply_returns_record_view_apply(self) -> None:
        view = make_test_view("sp", "rv", "v1")
        view.stream_id = ["my-stream"]
        view.used_for = "record"

        result = view.as_record_view_apply()

        assert isinstance(result, RecordViewApply)
        assert result.stream_id == ["my-stream"]
        assert result.space == "sp"
        assert result.external_id == "rv"

    def test_as_record_view_apply_raises_when_stream_id_not_set(self) -> None:
        view = make_test_view("sp", "v", "v1")

        with pytest.raises(ValueError, match="not a record view"):
            view.as_record_view_apply()


class TestViewFilterUsedFor:
    def test_used_for_omitted_by_default(self) -> None:
        assert "usedFor" not in ViewFilter().dump()

    def test_used_for_singular_str(self) -> None:
        assert ViewFilter(used_for="record").dump()["usedFor"] == ["record"]

    def test_used_for_sequence(self) -> None:
        assert ViewFilter(used_for=["node", "record"]).dump()["usedFor"] == ["node", "record"]

    def test_used_for_rejects_invalid_type(self) -> None:
        with pytest.raises(TypeError, match="Invalid value for 'used_for'"):
            ViewFilter(used_for=123)  # type: ignore[arg-type]


class TestView:
    def test_as_property_ref(self) -> None:
        params = dict(
            space="spa",
            externalId="de",
            version="69",
            lastUpdatedTime=123,
            createdTime=12,
            writable=False,
            usedFor="node",
            isGlobal=False,
        )
        cont = View.load(params)
        cont_apply = ViewApply.load(params)

        assert cont.as_property_ref("bar") == ("spa", "de/69", "bar")
        assert cont_apply.as_property_ref("bar") == ("spa", "de/69", "bar")


class TestViewPropertyDefinition:
    def test_load_dumped_mapped_property_for_read(self) -> None:
        input = {
            "type": {
                "type": "direct",
                "source": {"type": "view", "space": "mySpace", "externalId": "myExternalId", "version": "myVersion"},
                "list": False,
            },
            "container": {"space": "mySpace", "externalId": "myExternalId", "type": "container"},
            "containerPropertyIdentifier": "name",
            "description": None,
            "name": "fullName",
            "nullable": False,
            "autoIncrement": False,
            "defaultValue": None,
            "immutable": False,
            "constraintState": {"nullability": "current"},
        }
        actual = ViewProperty.load(input)
        assert isinstance(actual, MappedProperty)
        assert actual.source == ViewId(space="mySpace", external_id="myExternalId", version="myVersion")
        assert actual.constraint_state == PropertyConstraintState(nullability="current")

        assert actual.dump(camel_case=False) == {
            "auto_increment": False,
            "container": {"external_id": "myExternalId", "space": "mySpace"},
            "container_property_identifier": "name",
            "name": "fullName",
            "nullable": False,
            "immutable": False,
            "type": {
                "type": "direct",
                "source": {"external_id": "myExternalId", "space": "mySpace", "version": "myVersion"},
                "list": False,
            },
            "constraint_state": {"nullability": "current"},
        }

    def test_load_dumped_mapped_property_for_apply(self) -> None:
        input = {
            "container": {"space": "mySpace", "externalId": "myExternalId", "type": "container"},
            "containerPropertyIdentifier": "name",
            "description": None,
            "name": "fullName",
            "source": {"type": "view", "space": "mySpace", "externalId": "myExternalId", "version": "myVersion"},
        }
        actual = ViewPropertyApply.load(input)

        assert actual.dump(camel_case=False) == {
            "container": {"external_id": "myExternalId", "space": "mySpace", "type": "container"},
            "container_property_identifier": "name",
            "name": "fullName",
            "source": {"space": "mySpace", "external_id": "myExternalId", "version": "myVersion", "type": "view"},
        }

    def test_load_dump_single_reverse_direct_relation_property_with_container(self) -> None:
        input = {
            "connectionType": "single_reverse_direct_relation",
            "through": {
                "source": {"externalId": "myContainer", "space": "mySpace", "type": "container"},
                "identifier": "myIdentifier",
            },
            "source": {"type": "view", "space": "mySpace", "externalId": "mySourceView", "version": "myVersion"},
            "name": "fullName",
            "description": "my single reverse direct relation property",
        }
        actual = ViewProperty.load(input)

        assert actual.dump(camel_case=False) == {
            "connection_type": "single_reverse_direct_relation",
            "description": "my single reverse direct relation property",
            "name": "fullName",
            "source": {"external_id": "mySourceView", "space": "mySpace", "type": "view", "version": "myVersion"},
            "through": {
                "identifier": "myIdentifier",
                "source": {"external_id": "myContainer", "space": "mySpace", "type": "container"},
            },
        }

    def test_load_dump_single_reverse_direct_relation_property_with_container_for_apply(self) -> None:
        input = {
            "through": {
                "source": {"externalId": "myContainer", "space": "mySpace", "type": "container"},
                "identifier": "myIdentifier",
            },
            "source": {"type": "view", "space": "mySpace", "externalId": "mySourceView", "version": "myVersion"},
            "name": "fullName",
            "description": None,
            "connectionType": "single_reverse_direct_relation",
        }
        actual = ViewPropertyApply.load(input)

        assert actual.dump(camel_case=False) == {
            "name": "fullName",
            "source": {"external_id": "mySourceView", "space": "mySpace", "type": "view", "version": "myVersion"},
            "through": {
                "identifier": "myIdentifier",
                "source": {"external_id": "myContainer", "space": "mySpace", "type": "container"},
            },
            "connection_type": "single_reverse_direct_relation",
        }

    def test_load_dump_multi_reverse_direct_relation_property(self) -> None:
        input = {
            "connectionType": "multi_reverse_direct_relation",
            "through": {
                "source": {"externalId": "myContainer", "space": "mySpace", "type": "container"},
                "identifier": "myIdentifier",
            },
            "source": {"type": "view", "space": "mySpace", "externalId": "mySourceView", "version": "myVersion"},
            "name": "fullName",
            "description": "my multi reverse direct relation property",
        }
        actual = ViewProperty.load(input)

        assert actual.dump(camel_case=False) == {
            "connection_type": "multi_reverse_direct_relation",
            "description": "my multi reverse direct relation property",
            "name": "fullName",
            "source": {"external_id": "mySourceView", "space": "mySpace", "type": "view", "version": "myVersion"},
            "through": {
                "identifier": "myIdentifier",
                "source": {"external_id": "myContainer", "space": "mySpace", "type": "container"},
            },
        }

    def test_load_dump_multi_reverse_direct_relation_property_for_apply(self) -> None:
        input = {
            "through": {
                "source": {"externalId": "myContainer", "space": "mySpace", "type": "container"},
                "identifier": "myIdentifier",
            },
            "source": {"type": "view", "space": "mySpace", "externalId": "mySourceView", "version": "myVersion"},
            "name": "fullName",
            "description": None,
            "connectionType": "multi_reverse_direct_relation",
        }
        actual = ViewPropertyApply.load(input)

        assert actual.dump(camel_case=False) == {
            "name": "fullName",
            "source": {"external_id": "mySourceView", "space": "mySpace", "type": "view", "version": "myVersion"},
            "through": {
                "identifier": "myIdentifier",
                "source": {"external_id": "myContainer", "space": "mySpace", "type": "container"},
            },
            "connection_type": "multi_reverse_direct_relation",
        }

    def test_load_view_property_legacy(self) -> None:
        # Before the introduction of the `connectionType` field, the `source` field was used to determine the type of
        # the property. As of v8, this is now required:
        legacy_view = {
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
            "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "version": "2", "type": "view"},
            "name": "roles",
            "description": None,
            "edgeSource": None,
            "direction": "outwards",
        }
        with pytest.raises(ValueError, match=r"Connection Definition is missing field 'connectionType'"):
            ViewProperty._load(legacy_view)

    def test_load_view_property_apply_legacy(self) -> None:
        # Before the introduction of the `connectionType` field, the `source` field was used to determine the type of
        # the property. As of v8, this is now required:
        legacy_view = {
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
            "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "version": "2", "type": "view"},
            "name": "roles",
            "description": None,
            "edgeSource": None,
            "direction": "outwards",
        }

        with pytest.raises(ValueError, match=r"Connection Definition is missing field 'connectionType'"):
            ViewPropertyApply._load(legacy_view)

    def test_load_unknown_connection_type(self) -> None:
        # Before the introduction of the `connectionType` field, the `source` field was used to determine the type of
        # the property. This test ensures that the old format is still supported.
        input = {
            "whatever": "whatever",
            "connectionType": "UNKNOWN",
        }

        actual = ViewProperty.load(input)
        assert isinstance(actual, UnknownCogniteResource)
        assert actual.dump() == input

    def test_load_unknown_connection_type_apply(self) -> None:
        # Before the introduction of the `connectionType` field, the `source` field was used to determine the type of
        # the property. This test ensures that the old format is still supported.
        input = {
            "whatever": "whatever",
            "connectionType": "UNKNOWN",
        }

        actual = ViewPropertyApply.load(input)
        assert isinstance(actual, UnknownCogniteResource)
        assert actual.dump() == input
