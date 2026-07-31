from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes._base import UnknownCogniteResource
from cognite.client.data_classes.transformations.externaldata import (
    ExternalDataSource,
    ExternalDataSourceList,
    ExternalDataSourceUsability,
    ExternalDataSourceWrite,
    OneLakeCredentialsWrite,
    OneLakeExternalDataSource,
)


@pytest.fixture
def onelake_read_resource() -> dict[str, Any]:
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


class TestExternalDataSourceDispatch:
    def test_load_known_format_returns_onelake_subclass(self, onelake_read_resource: dict[str, Any]) -> None:
        loaded = ExternalDataSource._load(onelake_read_resource)

        assert isinstance(loaded, OneLakeExternalDataSource)
        assert loaded.settings.credentials.client_id == "my-client-id"
        assert loaded.settings.location_description.workspace_id == "my-workspace-id"

    def test_load_unknown_format_returns_unknown_resource(self, onelake_read_resource: dict[str, Any]) -> None:
        resource = {**onelake_read_resource, "format": "some_future_format"}

        loaded = ExternalDataSource._load(resource)

        assert isinstance(loaded, UnknownCogniteResource)

    def test_write_load_unknown_format_raises(self, onelake_read_resource: dict[str, Any]) -> None:
        resource = {
            "externalId": "fabric-lakehouse-prod",
            "format": "some_future_format",
            "settings": onelake_read_resource["settings"],
        }

        with pytest.raises(TypeError, match="Unknown external data source format"):
            ExternalDataSourceWrite._load(resource)


class TestExternalDataSourceAsWrite:
    def test_onelake_as_write_raises(self, onelake_read_resource: dict[str, Any]) -> None:
        instance = ExternalDataSource._load(onelake_read_resource)

        with pytest.raises(TypeError, match="cannot be converted to write"):
            instance.as_write()

    def test_list_as_write_raises(self, onelake_read_resource: dict[str, Any]) -> None:
        instance = ExternalDataSource._load(onelake_read_resource)
        resource_list = ExternalDataSourceList([instance])

        with pytest.raises(TypeError, match="cannot be converted to write"):
            resource_list.as_write()


class TestOneLakeCredentialsWriteRedaction:
    def test_repr_and_str_redact_secret(self) -> None:
        credentials = OneLakeCredentialsWrite(
            client_id="my-client-id", tenant_id="my-tenant-id", client_secret="super-secret"
        )

        assert "<redacted>" in repr(credentials)
        assert "<redacted>" in str(credentials)
        assert "super-secret" not in repr(credentials)
        assert "super-secret" not in str(credentials)

    def test_dump_still_contains_secret(self) -> None:
        credentials = OneLakeCredentialsWrite(
            client_id="my-client-id", tenant_id="my-tenant-id", client_secret="super-secret"
        )

        assert credentials.dump()["clientSecret"] == "super-secret"


class TestExternalDataSourceUsability:
    def test_is_usable_true_when_usable_version_present(self) -> None:
        usability = ExternalDataSourceUsability._load(
            {"externalId": {"externalId": "my-source"}, "usableVersion": "550e8400-e29b-41d4-a716-446655440000"}
        )

        assert usability.is_usable is True
        assert usability.external_id == "my-source"

    def test_is_usable_false_when_usable_version_absent(self) -> None:
        usability = ExternalDataSourceUsability._load({"externalId": {"externalId": "unknown-or-inaccessible"}})

        assert usability.is_usable is False

    def test_dump_wraps_external_id(self) -> None:
        usability = ExternalDataSourceUsability(external_id="my-source", usable_version="v1")

        assert usability.dump() == {"externalId": {"externalId": "my-source"}, "usableVersion": "v1"}
