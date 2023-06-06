import pytest

from cognite.client.data_classes.data_modeling.containers import ContainerDirectRelation
from cognite.client.data_classes.data_modeling.shared import (
    ContainerReference,
    DirectRelationReference,
    PropertyType,
    Reference,
    ViewReference,
)
from cognite.client.data_classes.data_modeling.views import ViewDirectRelation


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
        "data",
        [
            {"type": "text", "collation": "ucs_basic", "list": False},
            {"type": "int32", "list": True},
            {"type": "timeseries", "list": False},
        ],
    )
    def test_load_dump(self, data: dict):
        actual = PropertyType.load(data).dump(camel_case=True)

        assert data == actual


class TestDirectRelation:
    def test_load_dump_container_direct_relation(self):
        data = {"type": "direct", "container": {"space": "mySpace", "externalId": "myId", "type": "container"}}

        assert data == ContainerDirectRelation.load(data).dump(camel_case=True)

    def test_load_dump_view_direct_relation(self):
        data = {
            "type": "direct",
            "source": {"space": "mySpace", "externalId": "myId", "version": "myVersion", "type": "view"},
        }

        assert data == ViewDirectRelation.load(data).dump(camel_case=True)
