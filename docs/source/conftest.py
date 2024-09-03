import os
from pathlib import Path

import pytest
import yaml

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
def quickstart_client_config_file(monkeypatch):
    data = {
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

    def read_text(*args, **kwargs):
        return yaml.dump(data)

    monkeypatch.setattr(Path, "read_text", read_text)
