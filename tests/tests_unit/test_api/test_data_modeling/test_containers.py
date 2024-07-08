import re
from typing import Any

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ContainerApply, ContainerId, ContainerProperty, Text
from cognite.client.data_classes.data_modeling.containers import BTreeIndex, RequiresConstraint

EXAMPLE_CONTAINER = {
    "space": "testspace",
    "externalId": "container1",
    "name": "string",
    "description": "string",
    "usedFor": "node",
    "properties": {
        "prop1": {
            "immutable": False,
            "nullable": True,
            "autoIncrement": False,
            "defaultValue": "string",
            "description": "string",
            "name": "string",
            "type": {"type": "text", "list": False, "collation": "ucs_basic"},
        }
    },
    "constraints": {
        "constraint1": {
            "constraintType": "requires",
            "require": {"type": "container", "space": "string", "externalId": "string"},
        }
    },
    "indexes": {"index1": {"properties": ["prop1"], "indexType": "btree", "cursorable": False, "bySpace": False}},
    "createdTime": 123,
    "lastUpdatedTime": 123,
    "isGlobal": True,
}


@pytest.fixture
def mock_containers_response(rsps: Any, cognite_client: CogniteClient):
    response_body = {"items": [EXAMPLE_CONTAINER]}
    url_pattern = re.compile(
        re.escape(cognite_client.data_modeling.containers._get_base_url_with_base_path())
        + "/models/containers(/byids)?$"
    )
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


@pytest.fixture
def mock_delete_index_response(rsps: Any, cognite_client: CogniteClient):
    response_body = {
        "items": [
            {
                "space": EXAMPLE_CONTAINER["space"],
                "containerExternalId": EXAMPLE_CONTAINER["externalId"],
                "identifier": "index1",
            }
        ]
    }
    url_pattern = re.compile(
        re.escape(cognite_client.data_modeling.containers._get_base_url_with_base_path())
        + "/models/containers/indexes/delete$"
    )
    rsps.add(rsps.POST, url_pattern, status=200, json=response_body)
    yield rsps


class TestContainersApi:
    def test_apply_retrieve_and_delete_index(
        self, cognite_client: CogniteClient, mock_containers_response: Any, mock_delete_index_response: Any
    ) -> None:
        new_container = ContainerApply(
            space=EXAMPLE_CONTAINER["space"],
            external_id=EXAMPLE_CONTAINER["externalId"],
            properties={
                "prop1": ContainerProperty(type=Text(), default_value="string", description="string", name="string")
            },
            description="string",
            name="string",
            used_for="node",
            constraints={"constraint1": RequiresConstraint(ContainerId("string", "string"))},
            indexes={"index1": BTreeIndex(properties=["prop1"])},
        )
        created = cognite_client.data_modeling.containers.apply(new_container)
        retrieved = cognite_client.data_modeling.containers.retrieve(new_container.as_id())

        assert retrieved is not None
        assert created.created_time
        assert created.last_updated_time
        assert retrieved.as_apply().dump() == new_container.dump()

        deleted_indexes = cognite_client.data_modeling.containers.delete_indexes([(new_container.as_id(), "index1")])
        assert deleted_indexes == [(new_container.as_id(), "index1")]
