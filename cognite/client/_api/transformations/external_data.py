from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.transformations.external_data import (
    ExternalDataSource,
    ExternalDataSourceList,
    ExternalDataSourceUsability,
    ExternalDataSourceWrite,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class TransformationExternalDataAPI(APIClient):
    """Manage Fabric OneLake external data sources for transformations.

    OneLake external data sources are **read-only** from a transform perspective — transforms can
    read data from OneLake tables via ``ext_onelake()`` SQL, but writing to OneLake is not supported.

    Note:
        ``upsert()`` registers credentials and location in CDF — it does not write transform output
        into OneLake. For new sources, use :class:`~cognite.client.data_classes.transformations.OneLakeExternalDataSourceWrite`.
        Listing returns read models without ``client_secret``; to re-upsert credentials from a listed source,
        call ``source.as_write(client_secret=...)`` before ``upsert()``.
    """

    _RESOURCE_PATH = "/transformations/external_data"
    _CREATE_LIMIT = 1000
    _DELETE_LIMIT = 1000

    async def list(self, limit: int | None = None) -> ExternalDataSourceList:
        """List Fabric OneLake external data sources.

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
        return await self._list(
            method="GET",
            list_cls=ExternalDataSourceList,
            resource_cls=ExternalDataSource,
            limit=limit,
        )

    @overload
    async def upsert(self, source: ExternalDataSourceWrite) -> ExternalDataSource: ...

    @overload
    async def upsert(self, source: Sequence[ExternalDataSourceWrite]) -> ExternalDataSourceList: ...

    async def upsert(
        self,
        source: ExternalDataSourceWrite | Sequence[ExternalDataSourceWrite],
    ) -> ExternalDataSource | ExternalDataSourceList:
        """Create or update (upsert) Fabric OneLake external data sources.

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
        return await self._create_multiple(
            items=source,
            list_cls=ExternalDataSourceList,
            resource_cls=ExternalDataSource,
            input_resource_cls=ExternalDataSourceWrite,
        )

    async def delete(
        self,
        external_id: str | SequenceNotStr[str],
        ignore_unknown_ids: bool = False,
    ) -> None:
        """Delete Fabric OneLake external data sources.

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
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    async def verify_usability(self, external_id: str) -> ExternalDataSourceUsability:
        """Verify that a Fabric OneLake external data source is usable.

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
        res = await self._post(
            url_path=self._RESOURCE_PATH + "/usability",
            json={"externalId": external_id},
            # Client-side POST concurrency bucket; not OneLake write semantics.
            semaphore=self._get_semaphore("write"),
        )
        return ExternalDataSourceUsability._load(res.json())
