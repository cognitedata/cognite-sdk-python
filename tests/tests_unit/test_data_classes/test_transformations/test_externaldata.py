from __future__ import annotations

from operator import attrgetter
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
    SnowflakeCredentialsWrite,
    SnowflakeExternalDataSource,
    SnowflakeExternalDataSourceWrite,
    SnowflakeLocationDescription,
    SnowflakeSettingsWrite,
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


@pytest.fixture
def snowflake_read_resource() -> dict[str, Any]:
    return {
        "externalId": "snowflake-analytics-prod",
        "format": "snowflake",
        "name": "Production warehouse",
        "dataSetId": 123456,
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


def _snowflake_write(**kwargs: Any) -> SnowflakeExternalDataSourceWrite:
    return SnowflakeExternalDataSourceWrite(
        external_id="snowflake-analytics-prod",
        settings=SnowflakeSettingsWrite(
            credentials=SnowflakeCredentialsWrite(
                account_identifier="org-account", user_name="COGNITE_SVC", role_name="COGNITE_READER"
            ),
            location_description=SnowflakeLocationDescription(warehouse_name="COMPUTE_WH"),
        ),
        **{"expiry_time": 5, **kwargs},
    )


class TestSnowflake:
    def test_write_dump_shape(self) -> None:
        assert _snowflake_write(name="n").dump() == {
            "externalId": "snowflake-analytics-prod",
            "name": "n",
            "format": "snowflake",
            "expiryTime": 5,
            "settings": {
                "credentials": {
                    "accountIdentifier": "org-account",
                    "userName": "COGNITE_SVC",
                    "roleName": "COGNITE_READER",
                },
                "locationDescription": {"warehouseName": "COMPUTE_WH"},
            },
        }

    def test_write_requires_expiry_time(self) -> None:
        with pytest.raises(TypeError, match="expiry_time"):
            SnowflakeExternalDataSourceWrite(  # type: ignore[call-arg]
                external_id="x",
                settings=_snowflake_write().settings,
            )

    def test_write_load_requires_expiry_time(self) -> None:
        dumped = _snowflake_write().dump()
        del dumped["expiryTime"]

        with pytest.raises(KeyError, match="expiryTime"):
            ExternalDataSourceWrite._load(dumped)

    def test_write_load_roundtrip(self) -> None:
        dumped = _snowflake_write(name="n", data_set_id=1).dump()

        loaded = ExternalDataSourceWrite._load(dumped)

        assert isinstance(loaded, SnowflakeExternalDataSourceWrite)
        assert loaded.dump() == dumped


class TestExternalDataSourceDispatch:
    @pytest.mark.parametrize(
        "resource_fixture, expected_cls, expected_attrs",
        [
            pytest.param(
                "onelake_read_resource",
                OneLakeExternalDataSource,
                {
                    "settings.credentials.client_id": "my-client-id",
                    "settings.location_description.workspace_id": "my-workspace-id",
                },
                id="one_lake",
            ),
            pytest.param(
                "snowflake_read_resource",
                SnowflakeExternalDataSource,
                {
                    "settings.credentials.public_key": "-----BEGIN PUBLIC KEY-----abc",
                    "settings.location_description.warehouse_name": "COMPUTE_WH",
                    "expiry_time": 1700000000000,
                },
                id="snowflake",
            ),
        ],
    )
    def test_load_known_format_returns_matching_subclass(
        self,
        resource_fixture: str,
        expected_cls: type[ExternalDataSource],
        expected_attrs: dict[str, Any],
        request: pytest.FixtureRequest,
    ) -> None:
        resource = request.getfixturevalue(resource_fixture)

        loaded = ExternalDataSource._load(resource)

        assert isinstance(loaded, expected_cls)
        assert {path: attrgetter(path)(loaded) for path in expected_attrs} == expected_attrs
        assert loaded.dump() == resource

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

    def test_load_without_format_on_base_class_raises(self, snowflake_read_resource: dict[str, Any]) -> None:
        write_resource = {k: v for k, v in _snowflake_write().dump().items() if k != "format"}
        read_resource = {k: v for k, v in snowflake_read_resource.items() if k != "format"}

        with pytest.raises(KeyError, match="format"):
            ExternalDataSourceWrite._load(write_resource)
        with pytest.raises(KeyError, match="format"):
            ExternalDataSource._load(read_resource)

    def test_load_without_format_falls_back_to_subclass_format(self, snowflake_read_resource: dict[str, Any]) -> None:
        write_resource = {k: v for k, v in _snowflake_write().dump().items() if k != "format"}
        read_resource = {k: v for k, v in snowflake_read_resource.items() if k != "format"}

        assert isinstance(SnowflakeExternalDataSourceWrite._load(write_resource), SnowflakeExternalDataSourceWrite)
        assert isinstance(SnowflakeExternalDataSource._load(read_resource), SnowflakeExternalDataSource)


class TestExternalDataSourceAsWrite:
    @pytest.mark.parametrize("resource_fixture", ["onelake_read_resource", "snowflake_read_resource"])
    def test_as_write_raises(self, resource_fixture: str, request: pytest.FixtureRequest) -> None:
        instance = ExternalDataSource._load(request.getfixturevalue(resource_fixture))

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
