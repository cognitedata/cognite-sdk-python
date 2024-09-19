from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
import yaml

from cognite.client.data_classes import TimeSeries
from cognite.client.testing import monkeypatch_cognite_client

# Files to exclude test directories or modules
collect_ignore = ["conf.py"]


@pytest.fixture
def set_envs(monkeypatch):
    env_vars = {
        "MY_CLUSTER": "api",
        "MY_TENANT_ID": "my-tenant-id",
        "MY_CLIENT_ID": "my-client-id",
        "MY_CLIENT_SECRET": "my-client-secret",
    }

    monkeypatch.setattr(os, "environ", env_vars)


@pytest.fixture
def client_data() -> dict[str, Any]:
    return {
        "client": {
            "project": "my-project",
            "client_name": "my-special-client",
            "base_url": "https://${MY_CLUSTER}.cognitedata.com",
            "credentials": {
                "client_credentials": {
                    "token_url": "https://login.microsoftonline.com/${MY_TENANT_ID}/oauth2/v2.0/token",
                    "client_id": "${MY_CLIENT_ID}",
                    "client_secret": "${MY_CLIENT_SECRET}",
                    "scopes": ["https://api.cognitedata.com/.default"],
                },
            },
        },
        "global": {
            "max_retries": 10,
            "max_retry_backoff": 10,
        },
    }


@pytest.fixture
def quickstart_client_config_file(monkeypatch, client_data):
    def read_text(*args, **kwargs):
        return yaml.dump(client_data)

    monkeypatch.setattr(Path, "read_text", read_text)


@pytest.fixture()
def appendix_update_patch() -> None:
    with monkeypatch_cognite_client() as client:
        client.time_series.update.return_value = TimeSeries(
            external_id="new_ts",
            name="New TS",
            description="Updated description",
            metadata={"key": "value", "new": "entry"},
        )
        yield None


@pytest.fixture()
def appendix_update_replace() -> None:
    with monkeypatch_cognite_client() as client:
        client.time_series.update.return_value = TimeSeries(
            external_id="new_ts",
            description="Updated description",
            metadata={"new": "entry"},
        )
        yield None


@pytest.fixture()
def appendix_update_replace_ignore_null() -> None:
    with monkeypatch_cognite_client() as client:
        client.time_series.update.return_value = TimeSeries(
            external_id="new_ts",
            name="New TS",
            description="Updated description",
            metadata={"new": "entry"},
        )
        yield None


@pytest.fixture()
def appendix_update_replace_ignore_null2() -> None:
    with monkeypatch_cognite_client() as client:
        client.time_series.update.return_value = TimeSeries(
            external_id="new_ts",
            name="New TS",
            description="Updated description",
            metadata={"key": "value"},
        )
        yield None
