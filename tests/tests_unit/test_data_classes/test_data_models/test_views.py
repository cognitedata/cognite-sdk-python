from cognite.client.data_classes._base import UnknownCogniteObject
from cognite.client.data_classes.data_modeling import View, ViewApply, ViewId
from cognite.client.data_classes.data_modeling.views import MappedProperty, ViewProperty, ViewPropertyApply


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
        }
        actual = ViewProperty.load(input)
        assert isinstance(actual, MappedProperty)
        assert actual.source == ViewId(space="mySpace", external_id="myExternalId", version="myVersion")

        assert actual.dump(camel_case=False) == {
            "auto_increment": False,
            "container": {"external_id": "myExternalId", "space": "mySpace"},
            "container_property_identifier": "name",
            "default_value": None,
            "description": None,
            "name": "fullName",
            "nullable": False,
            "immutable": False,
            "type": {
                "type": "direct",
                "source": {"external_id": "myExternalId", "space": "mySpace", "version": "myVersion"},
                "list": False,
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
        # the property. This test ensures that the old format is still supported.
        legacy_view = {
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
            "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "version": "2", "type": "view"},
            "name": "roles",
            "description": None,
            "edgeSource": None,
            "direction": "outwards",
        }

        actual = ViewProperty._load(legacy_view)

        assert actual.dump() == {
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
            "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "version": "2", "type": "view"},
            "name": "roles",
            "description": None,
            "edgeSource": None,
            "direction": "outwards",
            "connectionType": "multi_edge_connection",
        }

    def test_load_view_property_apply_legacy(self) -> None:
        # Before the introduction of the `connectionType` field, the `source` field was used to determine the type of
        # the property. This test ensures that the old format is still supported.
        legacy_view = {
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
            "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "version": "2", "type": "view"},
            "name": "roles",
            "description": None,
            "edgeSource": None,
            "direction": "outwards",
        }

        actual = ViewPropertyApply._load(legacy_view)

        assert actual.dump() == {
            "type": {"space": "IntegrationTestsImmutable", "externalId": "Person.roles"},
            "source": {"space": "IntegrationTestsImmutable", "externalId": "Role", "version": "2", "type": "view"},
            "name": "roles",
            "direction": "outwards",
            "connectionType": "multi_edge_connection",
        }

    def test_load_unknown_connection_type(self) -> None:
        # Before the introduction of the `connectionType` field, the `source` field was used to determine the type of
        # the property. This test ensures that the old format is still supported.
        input = {
            "whatever": "whatever",
            "connectionType": "UNKNOWN",
        }

        actual = ViewProperty.load(input)
        assert isinstance(actual, UnknownCogniteObject)
        assert actual.dump() == input

    def test_load_unknown_connection_type_apply(self) -> None:
        # Before the introduction of the `connectionType` field, the `source` field was used to determine the type of
        # the property. This test ensures that the old format is still supported.
        input = {
            "whatever": "whatever",
            "connectionType": "UNKNOWN",
        }

        actual = ViewPropertyApply.load(input)
        assert isinstance(actual, UnknownCogniteObject)
        assert actual.dump() == input
