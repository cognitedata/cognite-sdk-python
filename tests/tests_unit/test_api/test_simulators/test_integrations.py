import re

from responses import RequestsMock

from cognite.client import CogniteClient
from tests.utils import jsgz_load


class TestIntegrations:
    def test_delete_integrations(
        self,
        cognite_client: CogniteClient,
        rsps: RequestsMock,
    ) -> None:
        # Arrange
        request_body = {"items": [{"externalId": "test"}]}
        rsps.add(
            "POST",
            url=re.compile(
                re.escape(cognite_client.simulators._get_base_url_with_base_path() + "/simulators/integrations/delete")
            ),
            json={},
            status=200,
        )

        # Act
        cognite_client.simulators.integrations.delete(external_ids="test")

        # Assert
        assert request_body == jsgz_load(rsps.calls[0].request.body)
