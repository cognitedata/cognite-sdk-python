from cognite.client.experimental.model_hosting import ModelHostingClient
from cognite.client.experimental.datapoints import DatapointsClient
from cognite.client.experimental.sequences import SequencesClient
from cognite.client.experimental.time_series import TimeSeriesClient


class ExperimentalClient:
    def __init__(self, client_factory):
        self._client_factory = client_factory

    @property
    def model_hosting(self) -> ModelHostingClient:
        return ModelHostingClient(client_factory=self._client_factory)

    @property
    def datapoints(self) -> DatapointsClient:
        return self._client_factory(DatapointsClient)

    @property
    def sequences(self) -> SequencesClient:
        return self._client_factory(SequencesClient)

    @property
    def time_series(self) -> TimeSeriesClient:
        return self._client_factory(TimeSeriesClient)
