from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling.views import MappedProperty, ViewProperty, ViewPropertyApply


class TestViewPropertyDefinition:
    def test_load_dumped_mapped_property_for_read(self) -> None:
        input = {
            "type": {
                "type": "direct",
                "source": {"type": "view", "space": "mySpace", "externalId": "myExternalId", "version": "myVersion"},
            },
            "container": {"space": "mySpace", "externalId": "myExternalId", "type": "container"},
            "containerPropertyIdentifier": "name",
            "description": None,
            "name": "fullName",
            "nullable": False,
            "autoIncrement": False,
            "defaultValue": None,
        }
        actual = ViewProperty.load(input)
        assert isinstance(actual, MappedProperty)
        assert actual.source == ViewId(space="mySpace", external_id="myExternalId", version="myVersion")

        assert actual.dump() == {
            "auto_increment": False,
            "container": {"external_id": "myExternalId", "space": "mySpace"},
            "container_property_identifier": "name",
            "default_value": None,
            "description": None,
            "name": "fullName",
            "nullable": False,
            "type": {
                "container": None,
                "type": "direct",
                "source": {"external_id": "myExternalId", "space": "mySpace", "version": "myVersion"},
            },
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

        assert actual.dump() == {
            "container": {"external_id": "myExternalId", "space": "mySpace", "type": "container"},
            "container_property_identifier": "name",
            "description": None,
            "name": "fullName",
            "source": {"space": "mySpace", "external_id": "myExternalId", "version": "myVersion"},
        }

    def test_load_dump_connection_property(self) -> None:
        input = {
            "type": {"space": "mySpace", "externalId": "myExternalId"},
            "source": {"type": "view", "space": "mySpace", "externalId": "myExternalId", "version": "myVersion"},
            "direction": "outwards",
            "name": "fullName",
            "edgeSource": None,
        }
        actual = ViewProperty.load(input)

        assert actual.dump() == {
            "description": None,
            "direction": "outwards",
            "edge_source": None,
            "name": "fullName",
            "source": {"external_id": "myExternalId", "space": "mySpace", "type": "view", "version": "myVersion"},
            "type": {"external_id": "myExternalId", "space": "mySpace"},
        }

    def test_load_dump_connection_property_for_apply(self) -> None:
        input = {
            "type": {"space": "mySpace", "externalId": "myExternalId"},
            "source": {"type": "view", "space": "mySpace", "externalId": "myExternalId", "version": "myVersion"},
            "direction": "outwards",
            "name": "fullName",
            "edgeSource": None,
        }
        actual = ViewPropertyApply.load(input)

        assert actual.dump() == {
            "description": None,
            "direction": "outwards",
            "edge_source": None,
            "name": "fullName",
            "source": {"external_id": "myExternalId", "space": "mySpace", "type": "view", "version": "myVersion"},
            "type": {"external_id": "myExternalId", "space": "mySpace"},
        }
