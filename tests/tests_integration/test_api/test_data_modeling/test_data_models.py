import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import DataModelList


@pytest.fixture(scope="function")
def cdf_data_models(cognite_client: CogniteClient):
    data_models = cognite_client.data_modeling.data_models.list(limit=-1)
    assert len(data_models) > 0, "Please create at least one data model in CDF."
    yield data_models


class TestDataModelsAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_data_models: DataModelList):
        actual_data_models_in_cdf = cognite_client.data_modeling.spaces.list(limit=-1)

        assert actual_data_models_in_cdf == cdf_data_models
