from __future__ import annotations

import pytest

from cognite.client.data_classes.transformations.external_data import (
    ExternalDataSource,
    ExternalDataSourceUsability,
    ExternalDataSourceWrite,
    OneLakeCredentialsWrite,
)


def test_onelake_factory_produces_valid_structure() -> None:
    source = ExternalDataSourceWrite.onelake(
        external_id="x",
        client_id="cid",
        tenant_id="tid",
        client_secret="sec",
        workspace_name="ws",
        container_name="cn",
    )
    dumped = source.dump(camel_case=True)

    assert dumped["format"] == "one_lake"
    assert dumped["externalId"] == "x"
    assert dumped["settings"]["credentials"]["clientId"] == "cid"
    assert dumped["settings"]["credentials"]["clientSecret"] == "sec"
    assert dumped["settings"]["locationDescription"]["workspaceName"] == "ws"


def test_write_dump_always_includes_format() -> None:
    source = ExternalDataSourceWrite(external_id="x")
    dumped = source.dump(camel_case=True)

    assert "format" in dumped
    assert dumped["format"] == "one_lake"


def test_read_load_parses_settings() -> None:
    raw = {
        "externalId": "x",
        "format": "one_lake",
        "settings": {
            "credentials": {"clientId": "cid", "tenantId": "tid"},
            "locationDescription": {"workspaceName": "ws", "containerName": "cn"},
        },
    }
    source = ExternalDataSource._load(raw)

    assert source.external_id == "x"
    assert source.format == "one_lake"
    assert source.settings is not None
    assert source.settings.credentials is not None
    assert source.settings.credentials.client_id == "cid"
    assert source.settings.credentials.tenant_id == "tid"
    assert source.settings.location_description is not None
    assert source.settings.location_description.workspace_name == "ws"
    assert source.settings.location_description.container_name == "cn"
    # Read model never carries client_secret
    assert not hasattr(source.settings.credentials, "client_secret")


def test_read_load_unknown_format_warns_not_raises() -> None:
    raw = {"externalId": "x", "format": "delta_sharing", "settings": {}}

    with pytest.warns(UserWarning, match="Unknown external data source format"):
        source = ExternalDataSource._load(raw)

    # Must not raise — the object is returned despite the unknown format
    assert source.external_id == "x"
    assert source.format == "delta_sharing"


def test_as_write_returns_write_with_none_secret() -> None:
    raw = {
        "externalId": "x",
        "format": "one_lake",
        "settings": {
            "credentials": {"clientId": "cid", "tenantId": "tid"},
            "locationDescription": {"workspaceName": "ws", "containerName": "cn"},
        },
    }
    read_source = ExternalDataSource._load(raw)
    write_source = read_source.as_write()

    assert isinstance(write_source, ExternalDataSourceWrite)
    assert write_source.settings is not None
    assert write_source.settings.credentials is not None
    assert write_source.settings.credentials.client_secret is None


def test_credentials_write_repr_masks_secret() -> None:
    creds = OneLakeCredentialsWrite("cid", "tid", "actual-secret")
    result = repr(creds)

    assert "actual-secret" not in result
    assert "***" in result


def test_usability_load_with_version() -> None:
    usability = ExternalDataSourceUsability._load({"externalId": "x", "usableVersion": "some-uuid"})

    assert usability.external_id == "x"
    assert usability.usable_version == "some-uuid"


def test_usability_load_with_null_version() -> None:
    usability = ExternalDataSourceUsability._load({"externalId": "x", "usableVersion": None})

    assert usability.external_id == "x"
    assert usability.usable_version is None
