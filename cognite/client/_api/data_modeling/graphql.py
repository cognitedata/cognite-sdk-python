from __future__ import annotations

import textwrap
from typing import Any

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.data_classes.data_modeling.graphql import DMLApplyResult
from cognite.client.data_classes.data_modeling.ids import DataModelId
from cognite.client.exceptions import CogniteGraphQLError, GraphQLErrorSpec
from cognite.client.utils._auxiliary import interpolate_and_url_encode


class DataModelingGraphQLAPI(APIClient):
    def _post_graphql(self, url_path: str, query_name: str, json: dict) -> dict[str, Any]:
        res = self._post(url_path=url_path, json=json).json()
        # Errors can be passed both at top level and nested in the response:
        errors = res.get("errors", []) + ((res.get("data", {}).get(query_name) or {}).get("errors") or [])
        if errors:
            raise CogniteGraphQLError([GraphQLErrorSpec.load(error) for error in errors])
        return res["data"]

    def _unsafely_wipe_and_regenerate_dml(self, id: DataModelIdentifier) -> str:
        """Wipe and regenerate the DML for a given data model.

        Note:
            This removes all comments from the DML.

        Args:
            id (DataModelIdentifier): The data model to apply DML to.

        Returns:
            str: The new DML
        """
        graphql_body = """
            query WipeAndRegenerateDml($space: String!, $externalId: String!, $version: String!) {
                unsafelyWipeAndRegenerateDmlBasedOnDataModel(space: $space, externalId: $externalId, version: $version) {
                    items {
                        graphQlDml
                    }
                }
            }
        """
        data_model_id = DataModelId.load(id)
        payload = {
            "query": textwrap.dedent(graphql_body),
            "variables": {
                "space": data_model_id.space,
                "externalId": data_model_id.external_id,
                "version": data_model_id.version,
            },
        }

        query_name = "unsafelyWipeAndRegenerateDmlBasedOnDataModel"
        res = self._post_graphql(url_path="/dml/graphql", query_name=query_name, json=payload)
        return res[query_name]["items"][0]["graphQlDml"]

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
                >>> client = CogniteClient()
                >>> res = client.data_modeling.graphql.apply_dml(
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

        query_name = "upsertGraphQlDmlVersion"
        res = self._post_graphql(url_path="/dml/graphql", query_name=query_name, json=payload)
        return DMLApplyResult.load(res[query_name]["result"])

    def query(self, id: DataModelIdentifier, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQl query against a given data model.

        Args:
            id (DataModelIdentifier): The data model to query.
            query (str): The query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.

        Returns:
            dict[str, Any]: The query result

        Examples:

            Execute a graphql query against a given data model::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.graphql.query(
                ...     id=("mySpace", "myDataModel", "v1"),
                ...     query="listThings { items { thingProperty } }",
                ... )
        """
        dm_id = DataModelId.load(id)
        endpoint = interpolate_and_url_encode(
            "/userapis/spaces/{}/datamodels/{}/versions/{}/graphql", dm_id.space, dm_id.external_id, dm_id.version
        )
        res = self._post_graphql(url_path=endpoint, query_name="", json={"query": query, "variables": variables})
        return res
