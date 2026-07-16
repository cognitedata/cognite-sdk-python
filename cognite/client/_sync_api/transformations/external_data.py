"""
===============================================================================
d9f24d01a178f18294c46802409d6584
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.transformations.external_data import (
    ExternalDataSource,
    ExternalDataSourceList,
    ExternalDataSourceUsability,
    ExternalDataSourceWrite,
)
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr


class SyncTransformationExternalDataAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(self, limit: int | None = None) -> ExternalDataSourceList:
        """
        List Fabric OneLake external data sources.

        Args:
            limit (int | None): Maximum number of results to return. Use ``None`` or ``-1`` to return all. Defaults to returning all.
        Returns:
            ExternalDataSourceList: All registered OneLake external data sources.

        Examples:

            List all registered external data sources:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> sources = client.transformations.external_data_sources.list()
        """
        return run_sync(self.__async_client.transformations.external_data_sources.list(limit=limit))

    @overload
    def upsert(self, source: ExternalDataSourceWrite) -> ExternalDataSource: ...

    @overload
    def upsert(self, source: Sequence[ExternalDataSourceWrite]) -> ExternalDataSourceList: ...

    def upsert(
        self, source: ExternalDataSourceWrite | Sequence[ExternalDataSourceWrite]
    ) -> ExternalDataSource | ExternalDataSourceList:
        """
        Create or update (upsert) Fabric OneLake external data sources.

        An upsert creates the source if it doesn't exist, or overwrites it entirely if it does. Uniqueness is determined by ``externalId``.

        Args:
            source (ExternalDataSourceWrite | Sequence[ExternalDataSourceWrite]): Single source or list of sources to upsert.
        Returns:
            ExternalDataSource | ExternalDataSourceList: The upserted source(s).

        Examples:

            Register a Fabric OneLake source:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.transformations import (
                ...     OneLakeExternalDataSourceWrite,
                ... )
                >>> client = CogniteClient()
                >>> source = OneLakeExternalDataSourceWrite(
                ...     external_id="fabric-lakehouse-prod",
                ...     name="Production lakehouse",
                ...     client_id="<azure-app-id>",
                ...     tenant_id="<azure-tenant-uuid>",
                ...     client_secret="<secret>",
                ...     workspace_name="<fabric-workspace-guid>",
                ...     container_name="<fabric-lakehouse-guid>",
                ...     data_set_id=123456,
                ... )
                >>> res = client.transformations.external_data_sources.upsert(source)
        """
        return run_sync(self.__async_client.transformations.external_data_sources.upsert(source=source))

    def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """
        Delete Fabric OneLake external data sources.

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs to delete.
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Examples:

            Delete a source by external ID:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.transformations.external_data_sources.delete("fabric-lakehouse-prod")

            Delete multiple sources:

                >>> client.transformations.external_data_sources.delete(
                ...     ["fabric-lakehouse-prod", "fabric-lakehouse-staging"]
                ... )
        """
        return run_sync(
            self.__async_client.transformations.external_data_sources.delete(
                external_id=external_id, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def verify_usability(self, external_id: str) -> ExternalDataSourceUsability:
        """
        Verify that a Fabric OneLake external data source is usable.

        Checks that the source exists and that the configured Azure credentials can access the specified Fabric lakehouse. Returns a ``usable_version`` UUID if the source is accessible.

        Args:
            external_id (str): External ID of the source to verify.
        Returns:
            ExternalDataSourceUsability: Contains ``usable_version`` (a UUID) if the source is accessible, or ``None`` if the credentials are invalid or the source cannot be reached.

        Examples:

            Verify a source before running a transformation:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> result = client.transformations.external_data_sources.verify_usability(
                ...     "fabric-lakehouse-prod"
                ... )
                >>> assert result.usable_version is not None, (
                ...     "Source not configured or credentials invalid"
                ... )
        """
        return run_sync(
            self.__async_client.transformations.external_data_sources.verify_usability(external_id=external_id)
        )
