from cognite.client import CogniteClient


class TestDatapointSubscriptions:
    def test_list_subscriptions(self, cognite_client: CogniteClient):
        subscriptions = cognite_client.time_series.subscriptions.list(limit=5)

        assert len(subscriptions) > 0, "Add at least one subscription to the test environment to run this test"
