import time

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.integrations import Extractor, IntegrationWrite
from cognite.client.utils._text import random_string

BETA_HEADERS = {"cdf-version": "20230101-beta"}


@pytest.fixture(scope="session")
def integration_external_id(cognite_client: CogniteClient) -> str:
    external_id = "sdk-test-integration-errors"
    if cognite_client.integrations.retrieve(external_id, ignore_unknown_ids=True) is None:
        cognite_client.integrations.create(
            IntegrationWrite(
                external_id=external_id,
                extractor=Extractor(external_id="sdk-test-extractor", version="1.0.0"),
                name="SDK integration errors test",
            )
        )
    return external_id


class TestIntegrationErrors:
    @pytest.mark.allow_no_semaphore(
        "Errors are reported via the top-level client.post to /integrations/checkin, which the SDK does not wrap."
    )
    def test_list_reported_error(self, cognite_client: CogniteClient, integration_external_id: str) -> None:
        error_external_id = f"sdk-test-error-{random_string(10)}"
        start_time = int(time.time() * 1000)
        cognite_client.post(
            f"/api/v1/projects/{cognite_client.config.project}/integrations/checkin",
            json={
                "externalId": integration_external_id,
                "errors": [
                    {
                        "externalId": error_external_id,
                        "level": "error",
                        "description": "Something went wrong",
                        "startTime": start_time,
                    }
                ],
            },
            headers=BETA_HEADERS,
        )

        # The integration is shared across runs, so pick out the error reported by this run:
        res = cognite_client.integrations.errors.list(
            external_id=integration_external_id, min_start_time=start_time, limit=-1
        )
        (error,) = [e for e in res if e.external_id == error_external_id]

        assert error.dump() == {
            "externalId": error_external_id,
            "level": "error",
            "description": "Something went wrong",
            "startTime": start_time,
            "type": "general",
        }
