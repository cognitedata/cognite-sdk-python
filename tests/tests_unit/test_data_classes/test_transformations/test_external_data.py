from __future__ import annotations

import pytest

from cognite.client.data_classes.transformations.external_data import (
    ExternalDataSource,
    OneLakeCredentialsWrite,
    OneLakeExternalDataSource,
    OneLakeExternalDataSourceWrite,
    UnknownExternalDataSource,
)


def test_onelake_write_init_structure() -> None:
    source = OneLakeExternalDataSourceWrite(
        external_id="x",
        client_id="cid",
        tenant_id="tid",
        client_secret="sec",
        workspace_name="ws",
        container_name="cn",
    )

    assert source.external_id == "x"
    assert source.settings is not None
    assert source.settings.credentials is not None
    assert source.settings.credentials.client_id == "cid"
    assert source.settings.credentials.client_secret == "sec"
    assert source.settings.location_description is not None
    assert source.settings.location_description.workspace_name == "ws"


def test_write_dump_always_includes_format() -> None:
    source = OneLakeExternalDataSourceWrite(
        external_id="x",
        client_id="cid",
        tenant_id="tid",
        client_secret="sec",
        workspace_name="ws",
        container_name="cn",
    )
    dumped = source.dump(camel_case=True)

    assert "format" in dumped
    assert dumped["format"] == "one_lake"
    assert dumped["settings"]["credentials"]["clientId"] == "cid"


def test_base_write_has_no_settings_field() -> None:
    source = OneLakeExternalDataSourceWrite.with_settings(external_id="x", settings=None)
    dumped = source.dump(camel_case=True)

    assert dumped["externalId"] == "x"
    assert "settings" not in dumped or dumped.get("settings") is None


def test_one_lake_load_returns_subclass() -> None:
    raw = {"externalId": "x", "format": "one_lake", "settings": {}}
    source = ExternalDataSource._load(raw)

    assert isinstance(source, OneLakeExternalDataSource)
    assert source.format == "one_lake"


def test_read_load_unknown_format_returns_unknown_subclass() -> None:
    raw = {"externalId": "x", "format": "delta_sharing", "settings": {}}
    source = ExternalDataSource._load(raw)

    assert isinstance(source, UnknownExternalDataSource)
    assert source.external_id == "x"
    assert source.format == "delta_sharing"
    assert not hasattr(source, "settings") or getattr(source, "settings", None) is None
    assert source.dump(camel_case=True) == raw


def test_read_load_requires_format() -> None:
    raw = {"externalId": "x", "settings": {}}

    with pytest.raises(KeyError, match="format"):
        ExternalDataSource._load(raw)


def test_unknown_as_write_raises() -> None:
    source = UnknownExternalDataSource(external_id="x", format="delta_sharing")

    with pytest.raises(ValueError, match="unknown external data source format"):
        source.as_write()


def test_as_write_raises_without_client_secret() -> None:
    raw = {
        "externalId": "x",
        "format": "one_lake",
        "settings": {
            "credentials": {"clientId": "cid", "tenantId": "tid"},
            "locationDescription": {"workspaceName": "ws", "containerName": "cn"},
        },
    }
    read_source = ExternalDataSource._load(raw)
    assert isinstance(read_source, OneLakeExternalDataSource)

    with pytest.raises(ValueError, match="client_secret is required"):
        read_source.as_write()


def test_as_write_with_client_secret() -> None:
    raw = {
        "externalId": "x",
        "format": "one_lake",
        "settings": {
            "credentials": {"clientId": "cid", "tenantId": "tid"},
            "locationDescription": {"workspaceName": "ws", "containerName": "cn"},
        },
    }
    read_source = ExternalDataSource._load(raw)
    assert isinstance(read_source, OneLakeExternalDataSource)
    write_source = read_source.as_write(client_secret="new-secret")

    assert isinstance(write_source, OneLakeExternalDataSourceWrite)
    assert write_source.settings is not None
    assert write_source.settings.credentials is not None
    assert write_source.settings.credentials.client_secret == "new-secret"


def test_credentials_write_repr_masks_secret() -> None:
    creds = OneLakeCredentialsWrite("cid", "tid", "actual-secret")
    result = repr(creds)

    assert "actual-secret" not in result
    assert "<redacted>" in result


def test_credentials_write_str_masks_secret() -> None:
    creds = OneLakeCredentialsWrite("cid", "tid", "actual-secret")
    result = str(creds)

    assert "actual-secret" not in result
    assert "<redacted>" in result
