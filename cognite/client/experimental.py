from cognite.client._api.datapoints import DatapointsAPI
from cognite.client.beta import CogniteClient as Client
from cognite.client.utils._experimental_warning import experimental_api


class ExperimentalDatapointsApi(DatapointsAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


@experimental_api
class CogniteClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
