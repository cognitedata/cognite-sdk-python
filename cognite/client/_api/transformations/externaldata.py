from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.transformations.externaldata import (
    ExternalDataSource,
    ExternalDataSourceList,
    ExternalDataSourceUsability,
    ExternalDataSourceWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class TransformationExternalDataSourcesAPI(APIClient):
    """Manage the external data sources available to transformations.

    An external data source stores the credentials and the location needed to reach an external storage
    system, and is referenced from transformation SQL by its external ID. Microsoft Fabric OneLake is
    currently the only supported system, read through ``ext_onelake()``.

    Note:
        This API is in public beta. The contract may change before general availability. Using it requires
        the Fabric connector to be enabled for the project.
    """

    _RESOURCE_PATH = "/transformations/externaldata"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Transformation External Data Sources"
        )

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> AsyncIterator[ExternalDataSource]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> AsyncIterator[ExternalDataSourceList]: ...

    async def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> AsyncIterator[ExternalDataSource] | AsyncIterator[ExternalDataSourceList]:
        """Iterate over external data sources

        Fetches data sources as they are iterated over, so you keep a limited number of them in memory.

        Args:
            chunk_size (int | None): Number of data sources to return in each chunk. Defaults to yielding one data source at a time.
            limit (int | None): Maximum number of data sources to return. Defaults to return all.

        Yields:
            ExternalDataSource | ExternalDataSourceList: yields ExternalDataSource one by one if chunk_size is not specified, else ExternalDataSourceList objects.
        """  # noqa: DOC404
        self._warning.warn()

        async for item in self._list_generator(  # type: ignore [call-overload]
            list_cls=ExternalDataSourceList,
            resource_cls=ExternalDataSource,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
        ):
            yield item

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> ExternalDataSourceList:
        """List external data sources.

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
        self._warning.warn()
        return await self._list(
            method="GET",
            list_cls=ExternalDataSourceList,
            resource_cls=ExternalDataSource,  # type: ignore[type-abstract]
            limit=limit,
        )

    @overload
    async def upsert(self, data_source: ExternalDataSourceWrite) -> ExternalDataSource: ...

    @overload
    async def upsert(self, data_source: Sequence[ExternalDataSourceWrite]) -> ExternalDataSourceList: ...

    async def upsert(
        self, data_source: ExternalDataSourceWrite | Sequence[ExternalDataSourceWrite]
    ) -> ExternalDataSource | ExternalDataSourceList:
        """Create or replace external data sources, matched on external ID.

        Each item replaces the stored data source in full, so every request must contain complete settings
        including the client secret. Reading a data source never returns the client secret, so re-registering
        one means constructing a new write object with the secret filled in.

        Args:
            data_source (ExternalDataSourceWrite | Sequence[ExternalDataSourceWrite]): The data source(s) to create or replace.

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
        self._warning.warn()
        return await self._create_multiple(
            items=data_source,  # type: ignore[arg-type]
            list_cls=ExternalDataSourceList,
            resource_cls=ExternalDataSource,  # type: ignore[type-abstract]
            input_resource_cls=ExternalDataSourceWrite,
        )

    async def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """Delete external data sources by external ID.

        Transformations that still reference a deleted data source fail when they next run.

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs of the data source(s) to delete.

        Examples:

            Delete data sources by external ID:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.transformations.external_data_sources.delete(
                ...     ["fabric-lakehouse-prod", "fabric-lakehouse-staging"]
                ... )
        """
        self._warning.warn()
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
        )

    async def verify_usability(self, external_id: str) -> ExternalDataSourceUsability:
        """Check whether an external data source can be used by a transformation.

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
        self._warning.warn()
        res = await self._post(
            url_path=self._RESOURCE_PATH + "/usability",
            json={"externalId": external_id},
            semaphore=self._get_semaphore("read"),
        )
        return ExternalDataSourceUsability._load(res.json())
