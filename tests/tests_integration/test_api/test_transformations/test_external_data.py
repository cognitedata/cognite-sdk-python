from __future__ import annotations

import os
from pathlib import Path

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.transformations.external_data import (
    ExternalDataSourceList,
    ExternalDataSourceUsability,
    OneLakeExternalDataSource,
    OneLakeExternalDataSourceWrite,
)

_JETFIRE_ENV = Path(__file__).parents[5] / "jetfire-backend" / ".env"
_SKIP_REASON = f"Fabric integration env not available ({_JETFIRE_ENV})"


def _load_jetfire_env() -> None:
    """Parse key=value pairs from the jetfire-backend .env file into os.environ."""
    for line in _JETFIRE_ENV.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


@pytest.fixture(scope="module", autouse=True)
def load_fabric_env() -> None:
    if _JETFIRE_ENV.exists():
        _load_jetfire_env()


_FABRIC_ENV_VARS = (
    "FABRIC_CLIENT_ID",
    "FABRIC_TENANT_ID",
    "FABRIC_CLIENT_SECRET",
    "FABRIC_WORKSPACE",
    "FABRIC_LAKEHOUSE",
)


def _fabric_ci_available() -> bool:
    if all(os.environ.get(key) for key in _FABRIC_ENV_VARS):
        return True
    if not _JETFIRE_ENV.exists():
        return False
    _load_jetfire_env()
    return all(os.environ.get(key) for key in _FABRIC_ENV_VARS)


@pytest.mark.skipif(not _fabric_ci_available(), reason=_SKIP_REASON)
class TestExternalDataSourcesIntegration:
    """End-to-end lifecycle test: upsert → list → verify_usability → delete."""

    _EXTERNAL_ID = "sdk-integration-test-fabric-onelake"

    def test_lifecycle(self, cognite_client: CogniteClient) -> None:
        client_id = os.environ["FABRIC_CLIENT_ID"]
        tenant_id = os.environ["FABRIC_TENANT_ID"]
        client_secret = os.environ["FABRIC_CLIENT_SECRET"]
        workspace_name = os.environ["FABRIC_WORKSPACE"]
        container_name = os.environ["FABRIC_LAKEHOUSE"]

        source = OneLakeExternalDataSourceWrite(
            external_id=self._EXTERNAL_ID,
            client_id=client_id,
            tenant_id=tenant_id,
            client_secret=client_secret,
            workspace_name=workspace_name,
            container_name=container_name,
        )

        created = False
        try:
            # Upsert
            upserted = cognite_client.transformations.external_data_sources.upsert(source)
            created = True
            assert isinstance(upserted, OneLakeExternalDataSource)
            assert upserted.external_id == self._EXTERNAL_ID
            assert upserted.format == "one_lake"

            # List — verify the upserted source is present
            all_sources = cognite_client.transformations.external_data_sources.list()
            assert isinstance(all_sources, ExternalDataSourceList)
            external_ids = {s.external_id for s in all_sources}
            assert self._EXTERNAL_ID in external_ids

            # Verify usability — expects valid credentials and reachable workspace
            usability = cognite_client.transformations.external_data_sources.verify_usability(self._EXTERNAL_ID)
            assert isinstance(usability, ExternalDataSourceUsability)
            # usable_version is a UUID when the source is accessible; None when credentials are invalid
            assert usability.usable_version is not None, (
                "verify_usability returned None — credentials may be invalid or workspace unreachable"
            )

        finally:
            # Always delete to avoid leaving stale test resources
            if created:
                cognite_client.transformations.external_data_sources.delete(self._EXTERNAL_ID)
