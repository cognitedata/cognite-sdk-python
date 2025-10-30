from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, cast

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ContainerApply, ContainerId, ContainerProperty, Text
from cognite.client.data_classes.data_modeling.containers import BTreeIndex, RequiresConstraint
from tests.utils import get_url

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient

EXAMPLE_CONTAINER = {
    "space": "testspace",
    "externalId": "container1",
    "name": "string",
    "description": "string",
    "usedFor": "node",
    "properties": {
        "prop1": {
            "immutable": False,
            "nullable": False,
            "autoIncrement": False,
            "defaultValue": "string",
            "description": "string",
            "name": "string",
            "type": {"type": "text", "list": False, "collation": "ucs_basic"},
            "constraintState": {"nullability": "current"},
        }
    },
    "constraints": {
        "constraint1": {
            "constraintType": "requires",
            "require": {"type": "container", "space": "string", "externalId": "string"},
            "state": "current",
        }
    },
    "indexes": {
        "index1": {
            "properties": ["prop1"],
            "indexType": "btree",
            "cursorable": False,
            "bySpace": False,
            "state": "current",
        }
    },
    "createdTime": 123,
    "lastUpdatedTime": 123,
    "isGlobal": True,
}


@pytest.fixture
def mock_containers_response(
    httpx_mock: Any, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
    response_body = {"items": [EXAMPLE_CONTAINER]}
    url_pattern = re.compile(re.escape(get_url(async_client.data_modeling.containers)) + "/models/containers$")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body)
    url_pattern = re.compile(re.escape(get_url(async_client.data_modeling.containers)) + "/models/containers/byids$")
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body)
    return httpx_mock


@pytest.fixture
def mock_delete_index_response(
    httpx_mock: Any, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> HTTPXMock:
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
        re.escape(get_url(async_client.data_modeling.containers)) + "/models/containers/indexes/delete$"
    )
    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body)
    return httpx_mock


class TestContainersApi:
    def test_apply_retrieve_and_delete_index(
        self, cognite_client: CogniteClient, mock_containers_response: Any, mock_delete_index_response: Any
    ) -> None:
        new_container = ContainerApply(
            space=cast(str, EXAMPLE_CONTAINER["space"]),
            external_id=cast(str, EXAMPLE_CONTAINER["externalId"]),
            properties={
                "prop1": ContainerProperty(
                    type=Text(), default_value="string", description="string", name="string", nullable=False
                )
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
