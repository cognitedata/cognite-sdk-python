import pytest

from cognite.client.data_classes.data_modeling.containers import ContainerDirectNodeRelation
from cognite.client.data_classes.data_modeling.shared import (
    ContainerReference,
    DirectRelationReference,
    PropertyType,
    Reference,
    ViewReference,
)


class TestDirectRelationReference:
    def test_load_dump(self):
        data = {"space": "mySpace", "externalId": "myId"}

        assert data == DirectRelationReference.load(data).dump(camel_case=True)


class TestContainerReference:
    def test_load_dump(self):
        data = {"space": "mySpace", "externalId": "myId", "type": "container"}

        assert data == ContainerReference.load(data).dump(camel_case=True)


class TestViewReference:
    def test_load_dump(self):
        data = {"space": "mySpace", "externalId": "myId", "version": "myVersion", "type": "view"}

        assert data == ViewReference.load(data).dump(camel_case=True)


class TestReference:
    @pytest.mark.parametrize(
        "data",
        [
            {"space": "mySpace", "externalId": "myId", "type": "container"},
            {"space": "mySpace", "externalId": "myId", "version": "myVersion", "type": "view"},
        ],
    )
    def test_load_dump(self, data):
        assert data == Reference.load(data).dump(camel_case=True)


class TestPropertyType:
    @pytest.mark.parametrize(
        "data, direct_cls",
        [
            ({"type": "text", "collation": "ucs_basic", "list": False}, None),
            ({"type": "int32", "list": True}, None),
            ({"type": "timeseries", "list": False}, None),
            (
                {"type": "direct", "container": {"space": "mySpace", "externalId": "myId", "type": "container"}},
                ContainerDirectNodeRelation,
            ),
        ],
    )
    def test_load_dump(self, data: dict, direct_cls):
        actual = PropertyType.load(data, direct_type_cls=direct_cls).dump(camel_case=True)

        assert data == actual
