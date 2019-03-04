from cognite.client.experimental.model_hosting import ModelHostingClient
from cognite.client.experimental.datapoints import DatapointsClient
from cognite.client.experimental.sequences import SequencesClient
from cognite.client.experimental.time_series import TimeSeriesClient


class ExperimentalClient:
    def __init__(self, client_factory):
        self._client_factory = client_factory
        self._model_hosting_client = ModelHostingClient(client_factory=self._client_factory)
        self._datapoints_client = self._client_factory(DatapointsClient)
        self._sequences_client = self._client_factory(SequencesClient)
        self._time_series_client = self._client_factory(TimeSeriesClient)

    @property
    def model_hosting(self) -> ModelHostingClient:
        return self._model_hosting_client

    @property
    def datapoints(self) -> DatapointsClient:
        return self._datapoints_client

    @property
    def sequences(self) -> SequencesClient:
        return self._sequences_client

    @property
    def time_series(self) -> TimeSeriesClient:
        return self._time_series_client
