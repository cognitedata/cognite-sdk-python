import re
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes import Label, LabelDefinition, LabelDefinitionList, LabelDefinitionWrite
from tests.utils import get_url, jsgz_load


@pytest.fixture
def mock_labels_response(httpx_mock: HTTPXMock, cognite_client: CogniteClient) -> dict[str, Any]:
    response_body = {
        "items": [
            {"name": "Pump", "description": "guess", "externalId": "PUMP", "createdTime": 1575892259245, "dataSetId": 1}
        ]
    }
    url_pattern = re.compile(re.escape(get_url(cognite_client.labels)) + "/.+")

    httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body, is_optional=True)
    return response_body


class TestLabels:
    def test_list(
        self, cognite_client: CogniteClient, mock_labels_response: dict[str, Any], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.labels.list(external_id_prefix="P")
        assert "P" == jsgz_load(httpx_mock.get_requests()[0].content)["filter"]["externalIdPrefix"]
        assert mock_labels_response["items"] == res.dump(camel_case=True)

    def test_access_properties(self, cognite_client: CogniteClient, mock_labels_response: dict[str, Any]) -> None:
        res = cognite_client.labels.list(external_id_prefix="P")
        assert res[0].name == "Pump"
        assert res[0].description == "guess"
        assert res[0].external_id == "PUMP"
        assert res[0].created_time > 0

    def test_call(
        self, cognite_client: CogniteClient, mock_labels_response: dict[str, Any], httpx_mock: HTTPXMock
    ) -> None:
        list(cognite_client.labels(limit=10))
        calls = httpx_mock.get_requests()
        assert 1 == len(calls)
        assert {"limit": 10} == jsgz_load(calls[0].content)

    def test_create_single(
        self, cognite_client: CogniteClient, mock_labels_response: dict[str, Any], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.labels.create(LabelDefinitionWrite(external_id="1", name="my_label", description="text"))
        assert isinstance(res, LabelDefinition)
        assert mock_labels_response["items"][0] == res.dump(camel_case=True)
        assert {"items": [{"externalId": "1", "name": "my_label", "description": "text"}]} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )

    def test_create_multiple(
        self, cognite_client: CogniteClient, mock_labels_response: dict[str, Any], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.labels.create(
            [
                LabelDefinitionWrite(external_id="1", name="Rotating"),
                LabelDefinitionWrite(external_id="2", name="Positive Displacement"),
            ]
        )
        assert isinstance(res, LabelDefinitionList)
        assert mock_labels_response["items"] == res.dump(camel_case=True)
        assert {
            "items": [{"externalId": "1", "name": "Rotating"}, {"externalId": "2", "name": "Positive Displacement"}]
        } == jsgz_load(httpx_mock.get_requests()[0].content)

    def test_delete_single(
        self, cognite_client: CogniteClient, mock_labels_response: dict[str, Any], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.labels.delete(external_id="PUMP")
        assert {"items": [{"externalId": "PUMP"}]} == jsgz_load(httpx_mock.get_requests()[0].content)
        assert res is None

    def test_delete_multiple(
        self, cognite_client: CogniteClient, mock_labels_response: dict[str, Any], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.labels.delete(external_id=["PUMP", "VALVE"])
        assert {"items": [{"externalId": "PUMP"}, {"externalId": "VALVE"}]} == jsgz_load(
            httpx_mock.get_requests()[0].content
        )
        assert res is None

    def test_load_list(self) -> None:
        assert Label._load_list(None) is None
        labels: list[dict | str | Label | LabelDefinition] = [
            {"externalId": "a"},
            "b",
            Label("c"),
            LabelDefinition("d", name="bla", created_time=123, description=None, data_set_id=None, cognite_client=None),
        ]
        assert Label._load_list(labels) == [Label("a"), Label("b"), Label("c"), Label("d")]

    def test_list_with_dataset_ids(
        self, cognite_client: CogniteClient, mock_labels_response: dict[str, Any], httpx_mock: HTTPXMock
    ) -> None:
        res = cognite_client.labels.list(data_set_ids=[123], data_set_external_ids=["x"])
        assert res[0].data_set_id == 1
        ds_ids = jsgz_load(httpx_mock.get_requests()[0].content)["filter"]["dataSetIds"]
        assert [{"id": 123}, {"externalId": "x"}] == ds_ids
