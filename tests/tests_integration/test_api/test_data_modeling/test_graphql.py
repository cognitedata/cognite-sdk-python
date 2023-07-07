import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import DataModel, DataModelApply, Space
from cognite.client.exceptions import CogniteGraphQLError


@pytest.fixture(scope="session")
def data_model(cognite_client: CogniteClient, integration_test_space: Space) -> DataModel:
    data_model = cognite_client.data_modeling.data_models.apply(
        DataModelApply(integration_test_space.space, "DataModelForDmlTest", "v1")
    )
    return data_model


class TestDataModelingGraphQLAPI:
    def test_apply_dml(self, cognite_client: CogniteClient, data_model: DataModel) -> None:
        cognite_client.data_modeling.graphql.apply_dml(data_model.as_id(), "type SomeType { someProp: String! }")

    def test_apply_dml_invalid(self, cognite_client: CogniteClient, data_model: DataModel) -> None:
        with pytest.raises(CogniteGraphQLError) as exc:
            cognite_client.data_modeling.graphql.apply_dml(data_model.as_id(), "typ SomeType { someProp: String! }")
        assert exc.value.errors[0].message == "Invalid syntax in provided GraphQL schema"
        assert exc.value.errors[0].locations == [{"column": 17, "line": 3}, {"column": 1, "line": 1}]
