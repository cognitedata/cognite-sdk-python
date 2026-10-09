from __future__ import annotations

from collections.abc import Iterator

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.integrations import Extractor, IntegrationErrorList, IntegrationWrite
from cognite.client.utils._text import random_string

BETA_HEADERS = {"cdf-version": "20230101-beta"}


@pytest.fixture
def integration_external_id(cognite_client: CogniteClient) -> Iterator[str]:
    external_id = f"sdk-test-integration-errors-{random_string(10)}"
    cognite_client.integrations.create(
        IntegrationWrite(
            external_id=external_id,
            extractor=Extractor(external_id="cognite-simple-influxdb-extractor", version="1.0.0"),
            name="SDK integration errors test",
        )
    )
    try:
        yield external_id
    finally:
        cognite_client.integrations.delete(external_id, ignore_unknown_ids=True)


class TestIntegrationErrors:
    def test_list_reported_error(self, cognite_client: CogniteClient, integration_external_id: str) -> None:
        # Errors are reported by extractors through the checkin endpoint, which the SDK does not wrap.
        error_external_id = f"sdk-test-error-{random_string(10)}"
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/integrations/checkin",
            json={
                "externalId": integration_external_id,
                "errors": [
                    {
                        "externalId": error_external_id,
                        "level": "error",
                        "description": "Something went wrong",
                        "startTime": 1_700_000_000_000,
                    }
                ],
            },
            headers=BETA_HEADERS,
        )

        res = cognite_client.integrations.errors.list(external_id=integration_external_id)

        assert isinstance(res, IntegrationErrorList)
        assert len(res) == 1
        error = res[0]
        assert error.external_id == error_external_id
        assert error.level == "error"
        assert error.description == "Something went wrong"
        assert error.start_time == 1_700_000_000_000
        assert error.type == "general"
        assert error.task is None
