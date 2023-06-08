import pytest

from cognite.client.data_classes.data_modeling.views import ViewPropertyDefinition


class TestViewPropertyDefinition:
    @pytest.mark.parametrize(
        "data",
        [
            {
                "type": {"type": "text", "list": False, "collation": "ucs_basic"},
                "container": {"space": "mySpace", "externalId": "myExternalId", "type": "container"},
                "containerPropertyIdentifier": "name",
                "description": None,
                "name": "fullName",
                "nullable": False,
                "autoIncrement": False,
                "defaultValue": None,
            },
            {
                "type": {"space": "mySpace", "externalId": "myExternalId"},
                "source": {"type": "view", "space": "mySpace", "externalId": "myExternalId", "version": "myVersion"},
                "direction": "outwards",
                "description": None,
                "edgeSource": None,
                "name": "fullName",
            },
        ],
    )
    def test_load_dump(self, data: dict):
        actual = ViewPropertyDefinition.load(data).dump(camel_case=True)

        assert data == actual
