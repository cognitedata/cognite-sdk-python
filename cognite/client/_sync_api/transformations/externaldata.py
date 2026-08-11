"""
===============================================================================
8a68955cabb05edf6b593b4560f01814
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.transformations.externaldata import (
    ExternalDataSource,
    ExternalDataSourceList,
    ExternalDataSourceUsability,
    ExternalDataSourceWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncTransformationExternalDataSourcesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[ExternalDataSource]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[ExternalDataSourceList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[ExternalDataSource] | Iterator[ExternalDataSourceList]:
        """
        Iterate over external data sources

        Fetches data sources as they are iterated over, so you keep a limited number of them in memory.

        Args:
            chunk_size (int | None): Number of data sources to return in each chunk. Defaults to yielding one data source at a time.
            limit (int | None): Maximum number of data sources to return. Defaults to return all.

        Yields:
            ExternalDataSource | ExternalDataSourceList: yields ExternalDataSource one by one if chunk_size is not specified, else ExternalDataSourceList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.transformations.external_data_sources(chunk_size=chunk_size, limit=limit)
        )  # type: ignore [misc]

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> ExternalDataSourceList:
        """
        `List external data sources <https://api-docs.cognite.com/20230101-beta/tag/Transformation-External-Data-Sources/operation/listExternalDataSources>`_.

        Args:
            limit (int | None): Maximum number of data sources to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ExternalDataSourceList: List of external data sources.

        Examples:

            List data sources:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> data_sources = client.transformations.external_data_sources.list(limit=5)

            Iterate over data sources, one-by-one:

                >>> for data_source in client.transformations.external_data_sources():
                ...     data_source  # do something with the data source

            Iterate over chunks of data sources to reduce memory load:

                >>> for data_source_list in client.transformations.external_data_sources(chunk_size=25):
                ...     data_source_list  # do something with the data sources
        """
        return run_sync(self.__async_client.transformations.external_data_sources.list(limit=limit))

    @overload
    def upsert(
        self, data_source: ExternalDataSourceWrite, upsert_mode: Literal["replace"] = "replace"
    ) -> ExternalDataSource: ...

    @overload
    def upsert(
        self, data_source: Sequence[ExternalDataSourceWrite], upsert_mode: Literal["replace"] = "replace"
    ) -> ExternalDataSourceList: ...

    def upsert(
        self,
        data_source: ExternalDataSourceWrite | Sequence[ExternalDataSourceWrite],
        upsert_mode: Literal["replace"] = "replace",
    ) -> ExternalDataSource | ExternalDataSourceList:
        """
        `Upsert external data sources <https://api-docs.cognite.com/20230101-beta/tag/Transformation-External-Data-Sources/operation/upsertExternalDataSources>`_.

        Each item replaces the stored data source in full, so every request must contain complete settings
        including the client secret. Reading a data source never returns the client secret, so re-registering
        one means constructing a new write object with the secret filled in.

        Args:
            data_source (ExternalDataSourceWrite | Sequence[ExternalDataSourceWrite]): The data source(s) to create or replace.
            upsert_mode (Literal['replace']): How existing data sources are updated. Currently only "replace" is supported, which fully replaces the existing data source. Defaults to "replace".

        Returns:
            ExternalDataSource | ExternalDataSourceList: The created or replaced data source(s).

        Examples:

            Register a Fabric OneLake data source:

                >>> import os
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.transformations.externaldata import (
                ...     OneLakeCredentialsWrite,
                ...     OneLakeExternalDataSourceWrite,
                ...     OneLakeLocationDescription,
                ...     OneLakeSettingsWrite,
                ... )
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> data_source = OneLakeExternalDataSourceWrite(
                ...     external_id="fabric-lakehouse-prod",
                ...     name="Production lakehouse",
                ...     data_set_id=123456,
                ...     settings=OneLakeSettingsWrite(
                ...         credentials=OneLakeCredentialsWrite(
                ...             client_id="<azure-app-id>",
                ...             tenant_id="<azure-tenant-uuid>",
                ...             client_secret=os.environ["ONELAKE_CLIENT_SECRET"],
                ...         ),
                ...         location_description=OneLakeLocationDescription(
                ...             workspace_id="<fabric-workspace-guid>",
                ...             container_id="<fabric-lakehouse-guid>",
                ...         ),
                ...     ),
                ... )
                >>> res = client.transformations.external_data_sources.upsert(data_source)
        """
        return run_sync(
            self.__async_client.transformations.external_data_sources.upsert(
                data_source=data_source, upsert_mode=upsert_mode
            )
        )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """
        `Delete external data sources <https://api-docs.cognite.com/20230101-beta/tag/Transformation-External-Data-Sources/operation/deleteExternalDataSources>`_.

        Transformations that still reference a deleted data source fail when they next run.

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs of the data source(s) to delete. Unknown external IDs are not silently ignored; the endpoint does not support an ignore-unknown-ids option.

        Examples:

            Delete data sources by external ID:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.transformations.external_data_sources.delete(
                ...     ["fabric-lakehouse-prod", "fabric-lakehouse-staging"]
                ... )
        """
        return run_sync(self.__async_client.transformations.external_data_sources.delete(external_id=external_id))

    def verify_usability(self, external_id: str) -> ExternalDataSourceUsability:
        """
        `Verify external data source usability <https://api-docs.cognite.com/20230101-beta/tag/Transformation-External-Data-Sources/operation/verifyExternalDataSourceUsability>`_.

        Args:
            external_id (str): External ID of the data source to check.

        Returns:
            ExternalDataSourceUsability: The usability status, holding the latest usable version of the data source. The version is not set if the data source is missing or inaccessible.

        Examples:

            Check a data source before running a transformation that reads from it:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> usability = client.transformations.external_data_sources.verify_usability(
                ...     "fabric-lakehouse-prod"
                ... )
                >>> usability.is_usable
        """
        return run_sync(
            self.__async_client.transformations.external_data_sources.verify_usability(external_id=external_id)
        )
