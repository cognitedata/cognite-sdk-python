from __future__ import annotations

import warnings
from abc import ABC
from typing import Any, ClassVar

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

__all__ = [
    "ExternalDataSource",
    "ExternalDataSourceCore",
    "ExternalDataSourceList",
    "ExternalDataSourceUsability",
    "ExternalDataSourceWrite",
    "ExternalDataSourceWriteList",
    "OneLakeCredentialsRead",
    "OneLakeCredentialsWrite",
    "OneLakeDataSourceSettingsRead",
    "OneLakeDataSourceSettingsWrite",
    "OneLakeLocationDescription",
]


class OneLakeLocationDescription(CogniteResource):
    """Location of a Fabric OneLake lakehouse.

    Args:
        workspace_name (str): Fabric workspace GUID or name.
        container_name (str): Fabric lakehouse GUID or name.
    """

    def __init__(self, workspace_name: str, container_name: str) -> None:
        self.workspace_name = workspace_name
        self.container_name = container_name

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            workspace_name=resource["workspaceName"],
            container_name=resource["containerName"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if camel_case:
            return {"workspaceName": self.workspace_name, "containerName": self.container_name}
        return {"workspace_name": self.workspace_name, "container_name": self.container_name}


class OneLakeCredentialsRead(CogniteResource):
    """Read-only view of Azure credentials for Fabric OneLake (clientSecret is never returned by the API).

    Args:
        client_id (str): Azure application (client) ID.
        tenant_id (str): Azure tenant (directory) ID.
    """

    def __init__(self, client_id: str, tenant_id: str) -> None:
        self.client_id = client_id
        self.tenant_id = tenant_id

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            client_id=resource["clientId"],
            tenant_id=resource["tenantId"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if camel_case:
            return {"clientId": self.client_id, "tenantId": self.tenant_id}
        return {"client_id": self.client_id, "tenant_id": self.tenant_id}


class OneLakeCredentialsWrite(CogniteResource):
    """Azure credentials for writing to Fabric OneLake.

    Args:
        client_id (str): Azure application (client) ID.
        tenant_id (str): Azure tenant (directory) ID.
        client_secret (str | None): Azure client secret. Required for upsert; None when reconstructed
            from a read model via as_write() since the API never returns the secret.
    """

    def __init__(self, client_id: str, tenant_id: str, client_secret: str | None = None) -> None:
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.client_secret = client_secret

    def __repr__(self) -> str:
        secret_display = "***" if self.client_secret is not None else None
        return (
            f"OneLakeCredentialsWrite(client_id={self.client_id!r}, tenant_id={self.tenant_id!r},"
            f" client_secret={secret_display!r})"
        )

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            client_id=resource["clientId"],
            tenant_id=resource["tenantId"],
            client_secret=resource.get("clientSecret"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any]
        if camel_case:
            result = {"clientId": self.client_id, "tenantId": self.tenant_id}
            if self.client_secret is not None:
                result["clientSecret"] = self.client_secret
        else:
            result = {"client_id": self.client_id, "tenant_id": self.tenant_id}
            if self.client_secret is not None:
                result["client_secret"] = self.client_secret
        return result


class OneLakeDataSourceSettingsRead(CogniteResource):
    """Settings for a Fabric OneLake external data source (read model — no client secret).

    Args:
        credentials (OneLakeCredentialsRead | None): Azure credentials (client ID and tenant ID only).
        location_description (OneLakeLocationDescription | None): Fabric workspace and lakehouse identifiers.
    """

    def __init__(
        self,
        credentials: OneLakeCredentialsRead | None = None,
        location_description: OneLakeLocationDescription | None = None,
    ) -> None:
        self.credentials = credentials
        self.location_description = location_description

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        credentials = None
        if (creds_raw := resource.get("credentials")) is not None:
            credentials = OneLakeCredentialsRead._load(creds_raw)
        location_description = None
        if (loc_raw := resource.get("locationDescription")) is not None:
            location_description = OneLakeLocationDescription._load(loc_raw)
        return cls(credentials=credentials, location_description=location_description)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.credentials is not None:
            result["credentials" if not camel_case else "credentials"] = self.credentials.dump(camel_case=camel_case)
        if self.location_description is not None:
            key = "locationDescription" if camel_case else "location_description"
            result[key] = self.location_description.dump(camel_case=camel_case)
        return result


class OneLakeDataSourceSettingsWrite(CogniteResource):
    """Settings for writing a Fabric OneLake external data source (includes client secret).

    Args:
        credentials (OneLakeCredentialsWrite | None): Azure credentials including client secret.
        location_description (OneLakeLocationDescription | None): Fabric workspace and lakehouse identifiers.
    """

    def __init__(
        self,
        credentials: OneLakeCredentialsWrite | None = None,
        location_description: OneLakeLocationDescription | None = None,
    ) -> None:
        self.credentials = credentials
        self.location_description = location_description

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        credentials = None
        if (creds_raw := resource.get("credentials")) is not None:
            credentials = OneLakeCredentialsWrite._load(creds_raw)
        location_description = None
        if (loc_raw := resource.get("locationDescription")) is not None:
            location_description = OneLakeLocationDescription._load(loc_raw)
        return cls(credentials=credentials, location_description=location_description)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.credentials is not None:
            result["credentials"] = self.credentials.dump(camel_case=camel_case)
        if self.location_description is not None:
            key = "locationDescription" if camel_case else "location_description"
            result[key] = self.location_description.dump(camel_case=camel_case)
        return result


class ExternalDataSourceCore(WriteableCogniteResource["ExternalDataSourceWrite"], ABC):
    """Shared base for ExternalDataSource (read) and ExternalDataSourceWrite (write).

    OneLake external data sources are **read-only** from a transform perspective — transforms can
    read data from OneLake tables via ``ext_onelake()`` SQL, but writing to OneLake is not supported.

    Args:
        external_id (str): External ID of the data source. Must be unique within the project.
        name (str | None): Human-readable name for the data source.
        data_set_id (int | None): ID of the data set that owns this resource (for ACL scoping).
    """

    _FORMAT: ClassVar[str] = "one_lake"

    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        data_set_id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.data_set_id = data_set_id


class ExternalDataSource(ExternalDataSourceCore):
    """A Fabric OneLake external data source (read model — returned by list).

    OneLake external data sources are **read-only** from a transform perspective — transforms can
    read data from OneLake tables via ``ext_onelake()`` SQL, but writing to OneLake is not supported.

    The ``clientSecret`` field is **never** returned by the API.

    Args:
        external_id (str): External ID of the data source.
        name (str | None): Human-readable name.
        data_set_id (int | None): Data set ID for ACL scoping.
        settings (OneLakeDataSourceSettingsRead | None): Connection settings (no client secret).
        format (str | None): Backend format identifier (always ``"one_lake"`` for OneLake sources).
        created_time (int | None): Time the resource was created (milliseconds since epoch).
        last_updated_time (int | None): Time the resource was last updated (milliseconds since epoch).
    """

    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        data_set_id: int | None = None,
        settings: OneLakeDataSourceSettingsRead | None = None,
        format: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
    ) -> None:
        super().__init__(external_id=external_id, name=name, data_set_id=data_set_id)
        self.settings = settings
        self.format = format
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        fmt = resource.get("format")
        if fmt is not None and fmt != cls._FORMAT:
            warnings.warn(
                f"Unknown external data source format: {fmt!r}. This version of the SDK may not fully support it.",
                UserWarning,
                stacklevel=2,
            )
        settings = None
        if (settings_raw := resource.get("settings")) is not None:
            settings = OneLakeDataSourceSettingsRead._load(settings_raw)
        return cls(
            external_id=resource["externalId"],
            name=resource.get("name"),
            data_set_id=resource.get("dataSetId"),
            settings=settings,
            format=fmt,
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
        )

    def as_write(self) -> ExternalDataSourceWrite:
        """Return this source as an ExternalDataSourceWrite.

        Note: The ``client_secret`` cannot be reconstructed from the read model (the API never returns it).
        The returned write object will have ``client_secret=None`` on its credentials.
        """
        settings_write: OneLakeDataSourceSettingsWrite | None = None
        if self.settings is not None:
            creds_write: OneLakeCredentialsWrite | None = None
            if self.settings.credentials is not None:
                creds_write = OneLakeCredentialsWrite(
                    client_id=self.settings.credentials.client_id,
                    tenant_id=self.settings.credentials.tenant_id,
                    client_secret=None,
                )
            settings_write = OneLakeDataSourceSettingsWrite(
                credentials=creds_write,
                location_description=self.settings.location_description,
            )
        return ExternalDataSourceWrite(
            external_id=self.external_id,
            name=self.name,
            data_set_id=self.data_set_id,
            settings=settings_write,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if camel_case:
            result["externalId"] = self.external_id
            if self.name is not None:
                result["name"] = self.name
            if self.data_set_id is not None:
                result["dataSetId"] = self.data_set_id
            if self.settings is not None:
                result["settings"] = self.settings.dump(camel_case=True)
            if self.format is not None:
                result["format"] = self.format
            if self.created_time is not None:
                result["createdTime"] = self.created_time
            if self.last_updated_time is not None:
                result["lastUpdatedTime"] = self.last_updated_time
        else:
            result["external_id"] = self.external_id
            if self.name is not None:
                result["name"] = self.name
            if self.data_set_id is not None:
                result["data_set_id"] = self.data_set_id
            if self.settings is not None:
                result["settings"] = self.settings.dump(camel_case=False)
            if self.format is not None:
                result["format"] = self.format
            if self.created_time is not None:
                result["created_time"] = self.created_time
            if self.last_updated_time is not None:
                result["last_updated_time"] = self.last_updated_time
        return result


class ExternalDataSourceWrite(ExternalDataSourceCore):
    """A Fabric OneLake external data source (write model — used for upsert).

    OneLake external data sources are **read-only** from a transform perspective — transforms can
    read data from OneLake tables via ``ext_onelake()`` SQL, but writing to OneLake is not supported.

    The ``format`` field is always ``"one_lake"`` and is injected automatically on serialization.

    Args:
        external_id (str): External ID of the data source.
        name (str | None): Human-readable name.
        data_set_id (int | None): Data set ID for ACL scoping.
        settings (OneLakeDataSourceSettingsWrite | None): Connection settings including client secret.
    """

    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        data_set_id: int | None = None,
        settings: OneLakeDataSourceSettingsWrite | None = None,
    ) -> None:
        super().__init__(external_id=external_id, name=name, data_set_id=data_set_id)
        self.settings = settings

    @classmethod
    def onelake(
        cls,
        external_id: str,
        client_id: str,
        tenant_id: str,
        client_secret: str,
        workspace_name: str,
        container_name: str,
        name: str | None = None,
        data_set_id: int | None = None,
    ) -> ExternalDataSourceWrite:
        """Create an ExternalDataSourceWrite for a Fabric OneLake source.

        OneLake external data sources are **read-only** from a transform perspective — transforms can
        read data from OneLake tables via ``ext_onelake()`` SQL, but writing to OneLake is not supported.

        Args:
            external_id (str): External ID for the data source. Must be unique.
            client_id (str): Azure application (client) ID.
            tenant_id (str): Azure tenant (directory) ID.
            client_secret (str): Azure client secret.
            workspace_name (str): Fabric workspace GUID or name.
            container_name (str): Fabric lakehouse GUID or name.
            name (str | None): Human-readable name.
            data_set_id (int | None): Data set ID for ACL scoping.

        Returns:
            ExternalDataSourceWrite: Ready to pass to ``client.transformations.external_data_sources.upsert()``.

        Examples:

            Register a Fabric OneLake source:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.transformations.external_data import ExternalDataSourceWrite
                >>> client = CogniteClient()
                >>> source = ExternalDataSourceWrite.onelake(
                ...     external_id="fabric-lakehouse-prod",
                ...     name="Production lakehouse",
                ...     client_id="<azure-app-id>",
                ...     tenant_id="<azure-tenant-uuid>",
                ...     client_secret="<secret>",
                ...     workspace_name="<fabric-workspace-guid>",
                ...     container_name="<fabric-lakehouse-guid>",
                ...     data_set_id=123456,
                ... )
                >>> client.transformations.external_data_sources.upsert(source)
        """
        return cls(
            external_id=external_id,
            name=name,
            data_set_id=data_set_id,
            settings=OneLakeDataSourceSettingsWrite(
                credentials=OneLakeCredentialsWrite(
                    client_id=client_id,
                    tenant_id=tenant_id,
                    client_secret=client_secret,
                ),
                location_description=OneLakeLocationDescription(
                    workspace_name=workspace_name,
                    container_name=container_name,
                ),
            ),
        )

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        settings = None
        if (settings_raw := resource.get("settings")) is not None:
            settings = OneLakeDataSourceSettingsWrite._load(settings_raw)
        return cls(
            external_id=resource["externalId"],
            name=resource.get("name"),
            data_set_id=resource.get("dataSetId"),
            settings=settings,
        )

    def as_write(self) -> ExternalDataSourceWrite:
        return self

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if camel_case:
            result["externalId"] = self.external_id
            result["format"] = self._FORMAT
            if self.name is not None:
                result["name"] = self.name
            if self.data_set_id is not None:
                result["dataSetId"] = self.data_set_id
            if self.settings is not None:
                result["settings"] = self.settings.dump(camel_case=True)
        else:
            result["external_id"] = self.external_id
            result["format"] = self._FORMAT
            if self.name is not None:
                result["name"] = self.name
            if self.data_set_id is not None:
                result["data_set_id"] = self.data_set_id
            if self.settings is not None:
                result["settings"] = self.settings.dump(camel_case=False)
        return result


class ExternalDataSourceList(WriteableCogniteResourceList[ExternalDataSourceWrite, ExternalDataSource]):
    """A list of ExternalDataSource (read model) objects."""

    _RESOURCE = ExternalDataSource

    def as_write(self) -> ExternalDataSourceWriteList:
        """Return all sources in their write format (client_secret will be None on each)."""
        return ExternalDataSourceWriteList([item.as_write() for item in self.data])


class ExternalDataSourceWriteList(CogniteResourceList[ExternalDataSourceWrite]):
    """A list of ExternalDataSourceWrite objects."""

    _RESOURCE = ExternalDataSourceWrite


class ExternalDataSourceUsability(CogniteResource):
    """Result of verifying a Fabric OneLake external data source's usability.

    Args:
        external_id (str | None): External ID of the verified data source.
        usable_version (str | None): UUID indicating the data source is accessible and credentials are valid.
            ``None`` if the source cannot be accessed.
    """

    def __init__(self, external_id: str | None = None, usable_version: str | None = None) -> None:
        self.external_id = external_id
        self.usable_version = usable_version

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource.get("externalId"),
            usable_version=resource.get("usableVersion"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if camel_case:
            return {"externalId": self.external_id, "usableVersion": self.usable_version}
        return {"external_id": self.external_id, "usable_version": self.usable_version}
