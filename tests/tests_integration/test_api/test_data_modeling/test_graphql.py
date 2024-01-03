import textwrap

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    DataModel,
    DataModelApply,
    DataModelId,
    Space,
)
from cognite.client.exceptions import CogniteGraphQLError


@pytest.fixture(scope="session")
def data_model(cognite_client: CogniteClient, integration_test_space: Space) -> DataModel:
    return cognite_client.data_modeling.data_models.apply(
        DataModelApply(integration_test_space.space, "DataModelForDmlTest", "1")
    )


@pytest.fixture(scope="session")
def data_model_for_query_test(cognite_client: CogniteClient, integration_test_space: Space) -> DataModel:
    data_model = cognite_client.data_modeling.data_models.apply(
        DataModelApply(integration_test_space.space, "DataModelForGraphQlQueryTest", "1")
    )
    cognite_client.data_modeling.graphql.apply_dml(data_model.as_id(), "type Thing { someProp: String! }")
    return data_model


class TestDataModelingGraphQLAPI:
    def test_apply_dml(self, cognite_client: CogniteClient, data_model: DataModel) -> None:
        dml = "type SomeType { someProp: String! } type AnotherType { anotherProp: String! }"
        res = cognite_client.data_modeling.graphql.apply_dml(data_model.as_id(), dml)
        assert res.version == "1"

    def test_apply_dml_invalid(self, cognite_client: CogniteClient, data_model: DataModel) -> None:
        with pytest.raises(CogniteGraphQLError) as exc:
            cognite_client.data_modeling.graphql.apply_dml(data_model.as_id(), "typ SomeType { someProp: String! }")
        assert exc.value.errors[0].message == "Invalid syntax in provided GraphQL schema"
        assert exc.value.errors[0].locations == [{"column": 5, "line": 3}, {"column": 1, "line": 1}]

    def test_apply_dml_invalid_error_passed_inside_response(
        self, cognite_client: CogniteClient, data_model: DataModel
    ) -> None:
        with pytest.raises(CogniteGraphQLError) as exc:
            cognite_client.data_modeling.graphql.apply_dml(data_model.as_id(), "type FailType { someProp: String! }")

        assert len(exc.value.errors) == 2
        err1, err2 = exc.value.errors
        assert err1.kind == "DIFF_ERROR"
        assert err1.message == "Can not remove view 'SomeType' from the data model definition"
        assert err2.hint == "Please publish a new data model version."

    def test_apply_dm_raise_top_level_error(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        # Arrange
        # Invalid as it is a missing version, which will raise an exception
        invalid_data_model_id = DataModelId(integration_test_space.space, "raiseTopLevelError")

        with pytest.raises(CogniteGraphQLError) as exc:
            cognite_client.data_modeling.graphql.apply_dml(
                invalid_data_model_id, "type ScenarioInstance { start: Timestamp }"
            )

        exception_message = str(exc.value)
        assert "version" in exception_message
        assert "invalid value" in exception_message

    def test_wipe_and_regenerate_dml(self, cognite_client: CogniteClient, data_model: DataModel) -> None:
        res = cognite_client.data_modeling.graphql._unsafely_wipe_and_regenerate_dml(data_model.as_id())
        expected = """
            type SomeType @view(version: "31e24bb338352b") {
              someProp: String!
            }

            type AnotherType @view(version: "855f080c9de22f") {
              anotherProp: String!
            }
        """
        assert res.strip() == textwrap.dedent(expected).strip()

    def test_query(self, cognite_client: CogniteClient, data_model_for_query_test: DataModel) -> None:
        query = """
            {
                listThing {
                    items {
                        externalId
                        space
                        someProp
                    }
                }
            }
        """
        res = cognite_client.data_modeling.graphql.query(data_model_for_query_test.as_id(), query)
        assert res == {"listThing": {"items": []}}

    def test_query_with_intent(self, cognite_client: CogniteClient, data_model_for_query_test: DataModel) -> None:
        query = """
            query MyQuery {
                listThing {
                    items {
                        externalId
                        space
                        someProp
                    }
                }
            }
        """
        res = cognite_client.data_modeling.graphql.query(data_model_for_query_test.as_id(), query)
        assert res == {"listThing": {"items": []}}

    def test_query_with_variables(self, cognite_client: CogniteClient, data_model_for_query_test: DataModel) -> None:
        query = """
            query MyQuery($first: Int) {
                listThing(first: $first) {
                    items {
                        externalId
                        space
                        someProp
                    }
                }
            }
        """
        res = cognite_client.data_modeling.graphql.query(
            data_model_for_query_test.as_id(), query, variables={"first": 10}
        )
        assert res == {"listThing": {"items": []}}

    def test_query_with_error(self, cognite_client: CogniteClient, data_model_for_query_test: DataModel) -> None:
        query = """
            {
                listThing {
                    items {
                        i_dont_exist
                    }
                }
            }
        """

        with pytest.raises(CogniteGraphQLError, match="Field 'i_dont_exist' in type 'Thing' is undefined"):
            cognite_client.data_modeling.graphql.query(data_model_for_query_test.as_id(), query)
