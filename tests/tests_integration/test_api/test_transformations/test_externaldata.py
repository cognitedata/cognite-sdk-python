from __future__ import annotations

import os
import string

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.transformations.externaldata import (
    OneLakeCredentialsWrite,
    OneLakeExternalDataSource,
    OneLakeExternalDataSourceWrite,
    OneLakeLocationDescription,
    OneLakeSettingsWrite,
)
from cognite.client.utils._text import random_string

# The OneLake data source is verified against a live Fabric lakehouse, so these tests only run when
# credentials for one are available:
REQUIRED_ENV_VARS = (
    "ONELAKE_CLIENT_ID",
    "ONELAKE_TENANT_ID",
    "ONELAKE_CLIENT_SECRET",
    "ONELAKE_WORKSPACE_ID",
    "ONELAKE_CONTAINER_ID",
)
missing_env_vars = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]

pytestmark = pytest.mark.skipif(
    bool(missing_env_vars), reason=f"Fabric OneLake credentials not available, missing: {missing_env_vars}"
)


class TestExternalDataSources:
    def test_upsert_list_verify_usability_delete(self, cognite_client: CogniteClient) -> None:
        api = cognite_client.transformations.external_data_sources
        external_id = f"sdk-integration-test-{random_string(6, string.ascii_letters)}"
        data_source = OneLakeExternalDataSourceWrite(
            external_id=external_id,
            name="SDK integration test lakehouse",
            settings=OneLakeSettingsWrite(
                credentials=OneLakeCredentialsWrite(
                    client_id=os.environ["ONELAKE_CLIENT_ID"],
                    tenant_id=os.environ["ONELAKE_TENANT_ID"],
                    client_secret=os.environ["ONELAKE_CLIENT_SECRET"],
                ),
                location_description=OneLakeLocationDescription(
                    workspace_id=os.environ["ONELAKE_WORKSPACE_ID"],
                    container_id=os.environ["ONELAKE_CONTAINER_ID"],
                ),
            ),
        )
        created = False
        try:
            upserted = api.upsert(data_source)
            created = True
            assert isinstance(upserted, OneLakeExternalDataSource)
            assert upserted.external_id == external_id
            assert upserted.settings.credentials.client_id == os.environ["ONELAKE_CLIENT_ID"]

            assert external_id in api.list(limit=-1).as_external_ids()

            usability = api.verify_usability(external_id)
            assert usability.external_id == external_id
            assert usability.is_usable, "Fabric credentials invalid or the lakehouse is unreachable"
        finally:
            if created:
                api.delete(external_id)
