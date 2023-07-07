from typing import Optional

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.client.exceptions import CogniteGraphQLError, GraphQLErrorSpec


class DataModelingGraphQLAPI(APIClient):
    def _post_graphql(self, url_path: str, query: str) -> None:
        res = self._post(url_path=url_path, json={"query": query})
        if (errors := res.json().get("errors")) is not None:
            raise CogniteGraphQLError([GraphQLErrorSpec.load(error) for error in errors])

    def apply_dml(
        self,
        id: DataModelIdentifier,
        dml: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        previous_version: Optional[str] = None,
    ) -> None:
        """Apply the DML for a given data model.

        Args:
            id (DataModelIdentifier): The data model to apply DML to.
            dml (str): The DML to apply.
            previous_version (Optional[str]): The previous version of the data model. Specify to reuse view versions from
                previous data model version.
        """
        data_model_id = DataModelId.load(id)
        graphql_body = f"""
            mutation UpsertGraphQlDmlVersion{{
                upsertGraphQlDmlVersion(
                    graphQlDmlVersion: {{
                        space: "{data_model_id.space}",
                        externalId: "{data_model_id.external_id}",
                        version: "{data_model_id.version}",
                        graphQlDml: "{dml}",
                        previousVersion: "{previous_version}",
                        name: "{name}",
                        description: "{description}"
                    }}
                ) {{
                    result {{
                        graphQlDml
                        externalId
                        version
                    }}
                }}
            }}
        """
        self._post_graphql(url_path="/dml/graphql", query=graphql_body)
