"""
===============================================================================
1cddc46d83897d442813705f021107c0
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import Any

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling import DataModelIdentifier
from cognite.client.data_classes.data_modeling.graphql import DMLApplyResult
from cognite.client.utils._async_helpers import run_sync


class SyncDataModelingGraphQLAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def _unsafely_wipe_and_regenerate_dml(self, id: DataModelIdentifier) -> str:
        """
        Wipe and regenerate the DML for a given data model.

        Note:
            This removes all comments from the DML.

        Args:
            id: The data model to apply DML to.

        Returns:
            The new DML
        """
        return run_sync(self.__async_client.data_modeling.graphql._unsafely_wipe_and_regenerate_dml(id=id))

    def apply_dml(
        self,
        id: DataModelIdentifier,
        dml: str,
        name: str | None = None,
        description: str | None = None,
        previous_version: str | None = None,
    ) -> DMLApplyResult:
        """
        Apply the DML for a given data model.

        Args:
            id: The data model to apply DML to.
            dml: The DML to apply.
            name: The name of the data model.
            description: The description of the data model.
            previous_version: The previous version of the data model. Specify to reuse view versions from previous data model version.

        Returns:
            The id of the updated data model.

        Examples:

            Apply DML:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.graphql.apply_dml(
                ...     id=("mySpaceExternalId", "myModelExternalId", "1"),
                ...     dml="type MyType { id: String! }",
                ...     name="My model name",
                ...     description="My model description"
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.graphql.apply_dml(
                id=id, dml=dml, name=name, description=description, previous_version=previous_version
            )
        )

    def query(self, id: DataModelIdentifier, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Execute a GraphQl query against a given data model.

        Args:
            id: The data model to query.
            query: The query to issue.
            variables: An optional dict of variables to pass to the query.

        Returns:
            The query result

        Examples:

            Execute a graphql query against a given data model:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.graphql.query(
                ...     id=("mySpace", "myDataModel", "v1"),
                ...     query="listThings { items { thingProperty } }",
                ... )
        """
        return run_sync(self.__async_client.data_modeling.graphql.query(id=id, query=query, variables=variables))
