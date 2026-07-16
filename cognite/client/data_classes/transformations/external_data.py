from __future__ import annotations

from typing import Any, ClassVar, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._text import convert_all_keys_recursive

ONE_LAKE_FORMAT = "one_lake"


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


class OneLakeCredentials(CogniteResource):
    """Response model for Azure credentials returned by list/get (``clientSecret`` is never included).

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


class OneLakeCredentialsWrite(CogniteResource):
    """Upsert model for Azure credentials when registering a OneLake external data source in CDF.

    Note:
        ``Write`` does **not** mean writing data into OneLake, it merely follows the naming convention
        in the SDK for create/update-style classes.

    Args:
        client_id (str): Azure application (client) ID.
        tenant_id (str): Azure tenant (directory) ID.
        client_secret (str): Azure client secret
    """

    def __init__(self, client_id: str, tenant_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.client_secret = client_secret

    def __repr__(self) -> str:
        return (
            f"OneLakeCredentialsWrite(client_id={self.client_id!r}, tenant_id={self.tenant_id!r},"
            f" client_secret=<redacted>)"
        )

    def __str__(self) -> str:
        return repr(self)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            client_id=resource["clientId"],
            tenant_id=resource["tenantId"],
            client_secret=resource["clientSecret"],
        )


class OneLakeDataSourceSettings(CogniteResource):
    """Response model for OneLake connection settings.

    Args:
        credentials (OneLakeCredentials | None): Azure credentials (client ID and tenant ID only).
        location_description (OneLakeLocationDescription | None): Fabric workspace and lakehouse identifiers.
    """

    def __init__(
        self,
        credentials: OneLakeCredentials | None = None,
        location_description: OneLakeLocationDescription | None = None,
    ) -> None:
        self.credentials = credentials
        self.location_description = location_description

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            credentials=OneLakeCredentials._load_if(resource.get("credentials")),
            location_description=OneLakeLocationDescription._load_if(resource.get("locationDescription")),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.credentials is not None:
            result["credentials"] = self.credentials.dump(camel_case=camel_case)
        if self.location_description is not None:
            key = "locationDescription" if camel_case else "location_description"
            result[key] = self.location_description.dump(camel_case=camel_case)
        return result


class OneLakeDataSourceSettingsWrite(CogniteResource):
    """Upsert model for OneLake connection settings registered in CDF.

    Args:
        credentials (OneLakeCredentialsWrite | None): Azure credentials for ``upsert()``.
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
        return cls(
            credentials=OneLakeCredentialsWrite._load_if(resource.get("credentials")),
            location_description=OneLakeLocationDescription._load_if(resource.get("locationDescription")),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        if self.credentials is not None:
            result["credentials"] = self.credentials.dump(camel_case=camel_case)
        if self.location_description is not None:
            key = "locationDescription" if camel_case else "location_description"
            result[key] = self.location_description.dump(camel_case=camel_case)
        return result


class ExternalDataSource(WriteableCogniteResource["ExternalDataSourceWrite"]):
    """An external data source (API read model — returned by list/get).

    Use :class:`OneLakeExternalDataSource` for Fabric OneLake sources.

    Args:
        external_id (str): External ID of the data source.
        format (str): Backend format identifier (``"one_lake"`` for OneLake sources).
        name (str | None): Human-readable name.
        data_set_id (int | None): Data set ID for ACL scoping.
        settings (OneLakeDataSourceSettings | None): Connection settings.
        created_time (int | None): Time the resource was created (milliseconds since epoch).
        last_updated_time (int | None): Time the resource was last updated (milliseconds since epoch).
    """

    def __init__(
        self,
        external_id: str,
        format: str,
        name: str | None = None,
        data_set_id: int | None = None,
        settings: OneLakeDataSourceSettings | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.data_set_id = data_set_id
        self.settings = settings
        self.format = format
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        fmt = resource["format"]
        if fmt == ONE_LAKE_FORMAT:
            return cast(Self, OneLakeExternalDataSource._load(resource))
        return cast(Self, UnknownExternalDataSource._load(resource))

    def as_write(self, client_secret: str | None = None) -> ExternalDataSourceWrite:
        """Return an upsert model for updating this source in CDF.

        Args:
            client_secret (str | None): Required when the read model includes credentials, because the API does not return ``client_secret``. Omit when only metadata (name, data set, location) changes.
        Returns:
            ExternalDataSourceWrite: Upsert model for this source.
        """
        settings_write: OneLakeDataSourceSettingsWrite | None = None
        if self.settings is not None:
            creds_write: OneLakeCredentialsWrite | None = None
            if self.settings.credentials is not None:
                if client_secret is None:
                    raise ValueError(
                        "client_secret is required to convert credentials to a write model because the API "
                        "does not return it. Pass client_secret to as_write(), or construct "
                        "OneLakeExternalDataSourceWrite with credentials."
                    )
                creds_write = OneLakeCredentialsWrite(
                    client_id=self.settings.credentials.client_id,
                    tenant_id=self.settings.credentials.tenant_id,
                    client_secret=client_secret,
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
        result = super().dump(camel_case=camel_case)
        if self.settings is not None:
            result["settings"] = self.settings.dump(camel_case=camel_case)
        return result


class OneLakeExternalDataSource(ExternalDataSource):
    """A Fabric OneLake external data source (API read model).

    OneLake external data sources are **read-only from a transform perspective** — transforms can
    read data from OneLake tables via ``ext_onelake()`` SQL, but writing transform output to OneLake
    is not supported.

    Args:
        external_id (str): External ID of the data source.
        format (str): Backend format identifier (always ``"one_lake"``).
        name (str | None): Human-readable name.
        data_set_id (int | None): Data set ID for ACL scoping.
        settings (OneLakeDataSourceSettings | None): Connection settings.
        created_time (int | None): Time the resource was created (milliseconds since epoch).
        last_updated_time (int | None): Time the resource was last updated (milliseconds since epoch).
    """

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        settings = OneLakeDataSourceSettings._load_if(resource.get("settings"))
        return cls(
            external_id=resource["externalId"],
            format=resource["format"],
            name=resource.get("name"),
            data_set_id=resource.get("dataSetId"),
            settings=settings,
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
        )


class UnknownExternalDataSource(ExternalDataSource):
    """An external data source with a format not supported by this SDK version.

    Args:
        external_id (str): External ID of the data source.
        format (str): Backend format identifier returned by the API.
        name (str | None): Human-readable name.
        data_set_id (int | None): Data set ID for ACL scoping.
        settings (OneLakeDataSourceSettings | None): Connection settings, if present in the payload.
        created_time (int | None): Time the resource was created (milliseconds since epoch).
        last_updated_time (int | None): Time the resource was last updated (milliseconds since epoch).
    """

    def __init__(
        self,
        external_id: str,
        format: str,
        name: str | None = None,
        data_set_id: int | None = None,
        settings: OneLakeDataSourceSettings | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            format=format,
            name=name,
            data_set_id=data_set_id,
            settings=settings,
            created_time=created_time,
            last_updated_time=last_updated_time,
        )
        self._raw: dict[str, Any] | None = None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        settings = OneLakeDataSourceSettings._load_if(resource.get("settings"))
        instance = cls(
            external_id=resource["externalId"],
            format=resource["format"],
            name=resource.get("name"),
            data_set_id=resource.get("dataSetId"),
            settings=settings,
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
        )
        instance._raw = resource
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        if self._raw is not None:
            return convert_all_keys_recursive(self._raw, camel_case=camel_case)
        return super().dump(camel_case=camel_case)

    def as_write(self, client_secret: str | None = None) -> ExternalDataSourceWrite:
        raise ValueError(
            f"Cannot convert unknown external data source format {self.format!r} to a write model in this SDK version."
        )


class ExternalDataSourceWrite(WriteableCogniteResource["ExternalDataSourceWrite"]):
    """Upsert model for an external data source (``external_data_sources.upsert()``).

    Use :class:`OneLakeExternalDataSourceWrite` to register a Fabric OneLake source.

    Args:
        external_id (str): External ID of the data source.
        name (str | None): Human-readable name.
        data_set_id (int | None): Data set ID for ACL scoping.
        settings (OneLakeDataSourceSettingsWrite | None): Connection settings including client secret.
    """

    _FORMAT: ClassVar[str] = ONE_LAKE_FORMAT

    def __init__(
        self,
        external_id: str,
        name: str | None = None,
        data_set_id: int | None = None,
        settings: OneLakeDataSourceSettingsWrite | None = None,
    ) -> None:
        self.external_id = external_id
        self.name = name
        self.data_set_id = data_set_id
        self.settings = settings

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        settings = OneLakeDataSourceSettingsWrite._load_if(resource.get("settings"))
        return cls(
            external_id=resource["externalId"],
            name=resource.get("name"),
            data_set_id=resource.get("dataSetId"),
            settings=settings,
        )

    def as_write(self) -> ExternalDataSourceWrite:
        return self

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case=camel_case)
        result["format"] = self._FORMAT
        if self.settings is not None:
            result["settings"] = self.settings.dump(camel_case=camel_case)
        return result


class OneLakeExternalDataSourceWrite(ExternalDataSourceWrite):
    """Upsert model for registering a Fabric OneLake external data source in CDF.

    Registers Azure credentials and lakehouse location so transforms can **read** via
    ``ext_onelake()``. Does not write data into OneLake.

    Args:
        external_id (str): External ID for the data source. Must be unique.
        client_id (str): Azure application (client) ID.
        tenant_id (str): Azure tenant (directory) ID.
        client_secret (str): Azure client secret.
        workspace_name (str): Fabric workspace GUID or name.
        container_name (str): Fabric lakehouse GUID or name.
        name (str | None): Human-readable name.
        data_set_id (int | None): Data set ID for ACL scoping.

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
            >>> client.transformations.external_data_sources.upsert(source)
    """

    def __init__(
        self,
        external_id: str,
        client_id: str,
        tenant_id: str,
        client_secret: str,
        workspace_name: str,
        container_name: str,
        name: str | None = None,
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(
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
        settings = OneLakeDataSourceSettingsWrite._load_if(resource.get("settings"))
        if settings is not None and settings.credentials is not None and settings.location_description is not None:
            return cls(
                external_id=resource["externalId"],
                client_id=settings.credentials.client_id,
                tenant_id=settings.credentials.tenant_id,
                client_secret=settings.credentials.client_secret,
                workspace_name=settings.location_description.workspace_name,
                container_name=settings.location_description.container_name,
                name=resource.get("name"),
                data_set_id=resource.get("dataSetId"),
            )
        return super()._load(resource)


class ExternalDataSourceList(
    WriteableCogniteResourceList[ExternalDataSourceWrite, ExternalDataSource], ExternalIDTransformerMixin
):
    """A list of ExternalDataSource (read model) objects."""

    _RESOURCE = ExternalDataSource

    def as_write(self, client_secret: str | None = None) -> ExternalDataSourceWriteList:
        """Return upsert models for each source.

        Args:
            client_secret (str | None): Passed through to :meth:`ExternalDataSource.as_write` for each item.
        Returns:
            ExternalDataSourceWriteList: Upsert models for each source in the list.
        """
        return ExternalDataSourceWriteList([item.as_write(client_secret=client_secret) for item in self.data])


class ExternalDataSourceWriteList(CogniteResourceList[ExternalDataSourceWrite], ExternalIDTransformerMixin):
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
