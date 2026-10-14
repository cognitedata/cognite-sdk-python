from __future__ import annotations

import re
from typing import Any

import pytest
from pytest_httpx2 import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.transformations.externaldata import (
    ExternalDataSourceList,
    ExternalDataSourceRotatedKeys,
    ExternalDataSourceRotatedKeysList,
    ExternalDataSourceRotateKeys,
    OneLakeCredentialsWrite,
    OneLakeExternalDataSource,
    OneLakeExternalDataSourceWrite,
    OneLakeLocationDescription,
    OneLakeSettingsWrite,
    SnowflakeCredentialsWrite,
    SnowflakeExternalDataSource,
    SnowflakeExternalDataSourceWrite,
    SnowflakeLocationDescription,
    SnowflakeSettingsWrite,
)
from cognite.client.exceptions import CogniteDuplicatedError
from tests.utils import get_url, jsgz_load


@pytest.fixture
def external_data_url(async_client: AsyncCogniteClient) -> str:
    api = async_client.transformations.external_data_sources
    return get_url(api, api._RESOURCE_PATH)


@pytest.fixture
def onelake_read_item() -> dict[str, Any]:
    return {
        "externalId": "fabric-lakehouse-prod",
        "format": "one_lake",
        "name": "Production lakehouse",
        "dataSetId": 123456,
        "settings": {
            "credentials": {"clientId": "my-client-id", "tenantId": "my-tenant-id"},
            "locationDescription": {"workspaceId": "my-workspace-id", "containerId": "my-container-id"},
        },
        "createdTime": 1,
        "lastUpdatedTime": 2,
    }


@pytest.fixture
def snowflake_read_item() -> dict[str, Any]:
    return {
        "externalId": "snowflake-analytics-prod",
        "format": "snowflake",
        "settings": {
            "credentials": {
                "accountIdentifier": "org-account",
                "userName": "COGNITE_SVC",
                "roleName": "COGNITE_READER",
                "publicKey": "-----BEGIN PUBLIC KEY-----abc",
            },
            "locationDescription": {"warehouseName": "COMPUTE_WH"},
        },
        "expiryTime": 1700000000000,
        "createdTime": 1,
        "lastUpdatedTime": 2,
    }


def make_snowflake_write_source(external_id: str) -> SnowflakeExternalDataSourceWrite:
    return SnowflakeExternalDataSourceWrite(
        external_id=external_id,
        expiry_time=1700000000000,
        settings=SnowflakeSettingsWrite(
            credentials=SnowflakeCredentialsWrite(
                account_identifier="org-account", user_name="COGNITE_SVC", role_name="COGNITE_READER"
            ),
            location_description=SnowflakeLocationDescription(warehouse_name="COMPUTE_WH"),
        ),
    )


def make_write_source(external_id: str) -> OneLakeExternalDataSourceWrite:
    return OneLakeExternalDataSourceWrite(
        external_id=external_id,
        name="Production lakehouse",
        data_set_id=123456,
        settings=OneLakeSettingsWrite(
            credentials=OneLakeCredentialsWrite(
                client_id="my-client-id", tenant_id="my-tenant-id", client_secret="super-secret"
            ),
            location_description=OneLakeLocationDescription(
                workspace_id="my-workspace-id", container_id="my-container-id"
            ),
        ),
    )


class TestTransformationExternalDataSourcesAPI:
    def test_list(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
        onelake_read_item: dict[str, Any],
    ) -> None:
        httpx2_mock.add_response(
            method="GET", url=external_data_url + "?limit=5", status_code=200, json={"items": [onelake_read_item]}
        )

        result = cognite_client.transformations.external_data_sources.list(limit=5)

        assert isinstance(result, ExternalDataSourceList)
        assert len(result) == 1
        data_source = result[0]
        assert isinstance(data_source, OneLakeExternalDataSource)
        assert data_source.external_id == "fabric-lakehouse-prod"
        assert data_source.settings.credentials.client_id == "my-client-id"
        assert data_source.settings.location_description.workspace_id == "my-workspace-id"

    def test_call_yields_data_sources_one_by_one(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
        onelake_read_item: dict[str, Any],
    ) -> None:
        httpx2_mock.add_response(
            method="GET",
            url=re.compile(re.escape(external_data_url) + r"\?.+"),
            status_code=200,
            json={"items": [onelake_read_item]},
        )

        data_sources = list(cognite_client.transformations.external_data_sources())

        assert len(data_sources) == 1
        assert isinstance(data_sources[0], OneLakeExternalDataSource)

    def test_call_yields_chunks(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
        onelake_read_item: dict[str, Any],
    ) -> None:
        items = [{**onelake_read_item, "externalId": f"lakehouse-{no}"} for no in range(30)]
        httpx2_mock.add_response(
            method="GET",
            url=re.compile(re.escape(external_data_url) + r"\?.+"),
            status_code=200,
            json={"items": items},
        )

        chunks = list(cognite_client.transformations.external_data_sources(chunk_size=25))

        assert [len(chunk) for chunk in chunks] == [25, 5]
        assert all(isinstance(chunk, ExternalDataSourceList) for chunk in chunks)

    def test_create_single(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
        onelake_read_item: dict[str, Any],
    ) -> None:
        httpx2_mock.add_response(
            method="POST", url=external_data_url, status_code=201, json={"items": [onelake_read_item]}
        )

        result = cognite_client.transformations.external_data_sources.create(make_write_source("fabric-lakehouse-prod"))

        assert isinstance(result, OneLakeExternalDataSource)
        assert result.external_id == "fabric-lakehouse-prod"

        item = jsgz_load(httpx2_mock.get_requests()[-1].content)["items"][0]
        assert item["format"] == "one_lake"
        assert item["settings"] == {
            "credentials": {
                "clientId": "my-client-id",
                "tenantId": "my-tenant-id",
                "clientSecret": "super-secret",
            },
            "locationDescription": {"workspaceId": "my-workspace-id", "containerId": "my-container-id"},
        }

    def test_create_multiple(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
        onelake_read_item: dict[str, Any],
    ) -> None:
        second_item = {**onelake_read_item, "externalId": "fabric-lakehouse-staging"}
        httpx2_mock.add_response(
            method="POST", url=external_data_url, status_code=201, json={"items": [onelake_read_item, second_item]}
        )

        result = cognite_client.transformations.external_data_sources.create(
            [make_write_source("fabric-lakehouse-prod"), make_write_source("fabric-lakehouse-staging")]
        )

        assert isinstance(result, ExternalDataSourceList)
        assert result.as_external_ids() == ["fabric-lakehouse-prod", "fabric-lakehouse-staging"]
        assert len(jsgz_load(httpx2_mock.get_requests()[-1].content)["items"]) == 2

    def test_create_existing_external_id_raises_duplicated(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=external_data_url,
            status_code=409,
            json={
                "error": {
                    "code": 409,
                    "message": "Duplicated external ids",
                    "duplicated": [{"externalId": "fabric-lakehouse-prod"}],
                }
            },
        )

        with pytest.raises(CogniteDuplicatedError) as exc_info:
            cognite_client.transformations.external_data_sources.create(make_write_source("fabric-lakehouse-prod"))

        assert exc_info.value.duplicated == [{"externalId": "fabric-lakehouse-prod"}]

    def test_delete(self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock, external_data_url: str) -> None:
        httpx2_mock.add_response(method="POST", url=external_data_url + "/delete", status_code=200, json={})

        cognite_client.transformations.external_data_sources.delete(
            ["fabric-lakehouse-prod", "fabric-lakehouse-staging"]
        )

        # The API accepts no other fields than 'items' (notably, there is no 'ignoreUnknownIds'):
        assert jsgz_load(httpx2_mock.get_requests()[-1].content) == {
            "items": [{"externalId": "fabric-lakehouse-prod"}, {"externalId": "fabric-lakehouse-staging"}]
        }

    def test_verify_usability(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock, external_data_url: str
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=external_data_url + "/usability",
            status_code=200,
            json={
                "externalId": {"externalId": "fabric-lakehouse-prod"},
                "usableVersion": "550e8400-e29b-41d4-a716-446655440000",
            },
        )

        usability = cognite_client.transformations.external_data_sources.verify_usability("fabric-lakehouse-prod")

        assert usability.external_id == "fabric-lakehouse-prod"
        assert usability.usable_version == "550e8400-e29b-41d4-a716-446655440000"
        assert usability.is_usable is True
        # The request takes a flat external ID, only the response nests it:
        assert jsgz_load(httpx2_mock.get_requests()[-1].content) == {"externalId": "fabric-lakehouse-prod"}

    def test_verify_usability_not_usable(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock, external_data_url: str
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=external_data_url + "/usability",
            status_code=200,
            json={"externalId": {"externalId": "unknown-or-inaccessible"}},
        )

        usability = cognite_client.transformations.external_data_sources.verify_usability("unknown-or-inaccessible")

        assert usability.usable_version is None
        assert usability.is_usable is False

    def test_list_sends_format_filter(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
        snowflake_read_item: dict[str, Any],
    ) -> None:
        httpx2_mock.add_response(
            method="GET",
            url=external_data_url + "?format=snowflake&limit=5",
            status_code=200,
            json={"items": [snowflake_read_item]},
        )

        result = cognite_client.transformations.external_data_sources.list(limit=5, format="snowflake")

        assert isinstance(result[0], SnowflakeExternalDataSource)

    def test_call_sends_format_filter(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
        snowflake_read_item: dict[str, Any],
    ) -> None:
        httpx2_mock.add_response(
            method="GET",
            url=re.compile(re.escape(external_data_url) + r"\?.*format=snowflake"),
            status_code=200,
            json={"items": [snowflake_read_item]},
        )

        data_sources = list(cognite_client.transformations.external_data_sources(format="snowflake"))

        assert len(data_sources) == 1

    def test_retrieve(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
        snowflake_read_item: dict[str, Any],
    ) -> None:
        httpx2_mock.add_response(
            method="GET",
            url=external_data_url + "/byid?externalId=snowflake-analytics-prod",
            status_code=200,
            json=snowflake_read_item,
        )

        result = cognite_client.transformations.external_data_sources.retrieve("snowflake-analytics-prod")

        assert isinstance(result, SnowflakeExternalDataSource)
        assert result.settings.credentials.public_key == "-----BEGIN PUBLIC KEY-----abc"

    def test_retrieve_missing_returns_none(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock, external_data_url: str
    ) -> None:
        httpx2_mock.add_response(
            method="GET",
            url=external_data_url + "/byid?externalId=nope",
            status_code=400,
            json={"error": {"code": 400, "message": "Not found", "missing": [{"externalId": "nope"}]}},
        )

        assert cognite_client.transformations.external_data_sources.retrieve("nope") is None

    def test_create_snowflake_sends_no_keys(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        external_data_url: str,
        snowflake_read_item: dict[str, Any],
    ) -> None:
        httpx2_mock.add_response(
            method="POST", url=external_data_url, status_code=201, json={"items": [snowflake_read_item]}
        )

        result = cognite_client.transformations.external_data_sources.create(
            make_snowflake_write_source("snowflake-analytics-prod")
        )

        assert isinstance(result, SnowflakeExternalDataSource)
        item = jsgz_load(httpx2_mock.get_requests()[-1].content)["items"][0]
        assert item["format"] == "snowflake"
        assert item["expiryTime"] == 1700000000000
        assert "publicKey" not in item["settings"]["credentials"]

    def test_rotate_keys_single(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock, external_data_url: str
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=external_data_url + "/rotatekeys",
            status_code=200,
            json={"items": [{"externalId": "src", "publicKey": "pk", "expiryTime": 123}]},
        )

        result = cognite_client.transformations.external_data_sources.rotate_keys(
            ExternalDataSourceRotateKeys("src", 123)
        )

        assert isinstance(result, ExternalDataSourceRotatedKeys)
        assert result.public_key == "pk"
        assert jsgz_load(httpx2_mock.get_requests()[-1].content) == {
            "items": [{"externalId": "src", "expiryTime": 123}]
        }

    def test_rotate_keys_multiple(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock, external_data_url: str
    ) -> None:
        httpx2_mock.add_response(
            method="POST",
            url=external_data_url + "/rotatekeys",
            status_code=200,
            json={
                "items": [
                    {"externalId": "a", "publicKey": "pk-a", "expiryTime": 1},
                    {"externalId": "b", "publicKey": "pk-b", "expiryTime": 2},
                ]
            },
        )

        result = cognite_client.transformations.external_data_sources.rotate_keys(
            [ExternalDataSourceRotateKeys("a", 1), ExternalDataSourceRotateKeys("b", 2)]
        )

        assert isinstance(result, ExternalDataSourceRotatedKeysList)
        assert result.as_external_ids() == ["a", "b"]

    def test_rotate_keys_over_limit_raises_without_request(
        self, cognite_client: CogniteClient, httpx2_mock: HTTPXMock
    ) -> None:
        too_many = [ExternalDataSourceRotateKeys(f"src-{no}", 1) for no in range(11)]

        with pytest.raises(ValueError, match="At most 10"):
            cognite_client.transformations.external_data_sources.rotate_keys(too_many)

        assert httpx2_mock.get_requests() == []
