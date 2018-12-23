from cognite.client.experimental.analytics import AnalyticsClient
from cognite.client.experimental.datapoints import DatapointsClient
from cognite.client.experimental.sequences import SequencesClient
from cognite.client.experimental.time_series import TimeSeriesClient


class ExperimentalClient:
    def __init__(self, client_factory):
        self._client_factory = client_factory

    @property
    def analytics(self) -> AnalyticsClient:
        return AnalyticsClient(client_factory=self._client_factory, version="0.6")

    @property
    def datapoints(self) -> DatapointsClient:
        return self._client_factory(DatapointsClient, "0.6")

    @property
    def sequences(self) -> SequencesClient:
        return self._client_factory(SequencesClient, "0.6")

    @property
    def time_series(self) -> TimeSeriesClient:
        return self._client_factory(TimeSeriesClient, "0.6")
