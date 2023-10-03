from __future__ import annotations

import textwrap
from typing import Any

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.data_classes.data_modeling.graphql import DMLApplyResult
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.client.exceptions import CogniteGraphQLError, GraphQLErrorSpec


class DataModelingGraphQLAPI(APIClient):
    def _post_graphql(self, url_path: str, json: dict) -> dict[str, Any]:
        res = self._post(url_path=url_path, json=json).json()
        # Errors can be passed both at top level and nested in the response:
        errors = res.get("errors", []) + (
            (res.get("data", {}).get("upsertGraphQlDmlVersion") or {}).get("errors") or []
        )
        if errors:
            raise CogniteGraphQLError([GraphQLErrorSpec.load(error) for error in errors])
        return res["data"]

    def apply_dml(
        self,
        id: DataModelIdentifier,
        dml: str,
        name: str | None = None,
        description: str | None = None,
        previous_version: str | None = None,
    ) -> DMLApplyResult:
        """Apply the DML for a given data model.

        Args:
            id (DataModelIdentifier): The data model to apply DML to.
            dml (str): The DML to apply.
            name (str | None): The name of the data model.
            description (str | None): The description of the data model.
            previous_version (str | None): The previous version of the data model. Specify to reuse view versions from previous data model version.

        Returns:
            DMLApplyResult: The id of the updated data model.

        Examples:

            Apply DML::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_modeling.graphql.apply_dml(
                ...     id=("mySpaceExternalId", "myModelExternalId", "1"),
                ...     dml="type MyType { id: String! }",
                ...     name="My model name",
                ...     description="My model description"
                ... )
        """
        graphql_body = """
            mutation UpsertGraphQlDmlVersion($dmCreate: GraphQlDmlVersionUpsert!) {
                upsertGraphQlDmlVersion(graphQlDmlVersion: $dmCreate) {
                    errors {
                        kind
                        message
                        hint
                        location {
                            start {
                                line
                                column
                            }
                        }
                    }
                    result {
                        space
                        externalId
                        version
                        name
                        description
                        graphQlDml
                        createdTime
                        lastUpdatedTime
                    }
                }
            }
        """
        data_model_id = DataModelId.load(id)
        payload = {
            "query": textwrap.dedent(graphql_body),
            "variables": {
                "dmCreate": {
                    "space": data_model_id.space,
                    "externalId": data_model_id.external_id,
                    "version": data_model_id.version,
                    "previousVersion": previous_version,
                    "graphQlDml": dml,
                    "name": name,
                    "description": description,
                }
            },
        }
        res = self._post_graphql(url_path="/dml/graphql", json=payload)
        return DMLApplyResult.load(res["upsertGraphQlDmlVersion"]["result"])
