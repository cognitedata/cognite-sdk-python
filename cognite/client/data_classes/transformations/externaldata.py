from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, NoReturn

from typing_extensions import Self, override

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    UnknownCogniteResource,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

ONE_LAKE_FORMAT = "one_lake"


class OneLakeLocationDescription(CogniteResource):
    """Location of the data within Microsoft Fabric OneLake.

    Args:
        workspace_id (str): Fabric workspace ID. Find it in the Fabric portal under Workspace settings > Workspace ID.
        container_id (str): Fabric lakehouse ID. Find it in the Fabric portal under Lakehouse settings > Item ID.
    """

    def __init__(self, workspace_id: str, container_id: str) -> None:
        self.workspace_id = workspace_id
        self.container_id = container_id

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            workspace_id=resource["workspaceId"],
            container_id=resource["containerId"],
        )


class OneLakeCredentials(CogniteResource):
    """Credentials used to authenticate with Microsoft Fabric OneLake. The client secret is never
    included when reading a data source.

    Args:
        client_id (str): Microsoft Entra ID application (client) ID.
        tenant_id (str): Microsoft Entra ID tenant (directory) ID.
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
    """Credentials used to authenticate with Microsoft Fabric OneLake.

    Args:
        client_id (str): Microsoft Entra ID application (client) ID.
        tenant_id (str): Microsoft Entra ID tenant (directory) ID.
        client_secret (str): Microsoft Entra ID client secret. Required in every write. Stored encrypted
            and never returned in a response.
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


class OneLakeSettings(CogniteResource):
    """Connection settings for the external data source, without the client secret.

    Args:
        credentials (OneLakeCredentials): Azure credentials (client ID and tenant ID only).
        location_description (OneLakeLocationDescription): Fabric workspace and lakehouse identifiers.
    """

    def __init__(self, credentials: OneLakeCredentials, location_description: OneLakeLocationDescription) -> None:
        self.credentials = credentials
        self.location_description = location_description

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            credentials=OneLakeCredentials._load(resource["credentials"]),
            location_description=OneLakeLocationDescription._load(resource["locationDescription"]),
        )

    @override
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["credentials"] = self.credentials.dump(camel_case=camel_case)
        key = "locationDescription" if camel_case else "location_description"
        output[key] = self.location_description.dump(camel_case=camel_case)
        return output


class OneLakeSettingsWrite(CogniteResource):
    """Connection settings for the external data source.

    Args:
        credentials (OneLakeCredentialsWrite): Azure credentials for the upsert.
        location_description (OneLakeLocationDescription): Fabric workspace and lakehouse identifiers.
    """

    def __init__(self, credentials: OneLakeCredentialsWrite, location_description: OneLakeLocationDescription) -> None:
        self.credentials = credentials
        self.location_description = location_description

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            credentials=OneLakeCredentialsWrite._load(resource["credentials"]),
            location_description=OneLakeLocationDescription._load(resource["locationDescription"]),
        )

    @override
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["credentials"] = self.credentials.dump(camel_case=camel_case)
        key = "locationDescription" if camel_case else "location_description"
        output[key] = self.location_description.dump(camel_case=camel_case)
        return output


class ExternalDataSourceWrite(CogniteResource, ABC):
    """Upsert model for an external data source to create or replace in CDF.

    Format-specific subclasses (e.g. :class:`OneLakeExternalDataSourceWrite`) hold typed settings.

    Args:
        external_id (str): External ID for the data source. Must be unique.
        name (str | None): Display name for the external data source.
        data_set_id (int | None): Data set ID for ACL scoping.
    """

    _format: ClassVar[str]

    def __init__(self, external_id: str, name: str | None = None, data_set_id: int | None = None) -> None:
        self.external_id = external_id
        self.name = name
        self.data_set_id = data_set_id

    @classmethod
    @abstractmethod
    def _load_data_source(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> ExternalDataSourceWrite:
        format_ = resource.get("format")
        if format_ is None and hasattr(cls, "_format"):
            format_ = cls._format
        elif format_ is None:
            raise KeyError("format")
        try:
            source_cls = _EXTERNAL_DATA_SOURCE_WRITE_CLASS_BY_FORMAT[format_]
        except KeyError:
            raise TypeError(
                f"Unknown external data source format: {format_}. You may need to upgrade the SDK to a "
                "version that supports this format."
            )
        return source_cls._load_data_source(resource)

    @override
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["format"] = self._format
        return output


class OneLakeExternalDataSourceWrite(ExternalDataSourceWrite):
    """Upsert model for registering a Fabric OneLake external data source in CDF.

    Registers Azure credentials and lakehouse location so transforms can read via ``ext_onelake()``.
    Does not write data into OneLake. Each upsert replaces the stored data source in full, so every
    request must contain complete ``settings`` including ``client_secret``.

    Args:
        external_id (str): External ID for the data source. Must be unique.
        settings (OneLakeSettingsWrite): OneLake credentials and location.
        name (str | None): Display name for the external data source.
        data_set_id (int | None): Data set ID for ACL scoping.

    Examples:

        Construct a Fabric OneLake source for upsert:

            >>> import os
            >>> from cognite.client.data_classes.transformations.externaldata import (
            ...     OneLakeCredentialsWrite,
            ...     OneLakeExternalDataSourceWrite,
            ...     OneLakeLocationDescription,
            ...     OneLakeSettingsWrite,
            ... )
            >>> source = OneLakeExternalDataSourceWrite(
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
    """

    _format: ClassVar[str] = ONE_LAKE_FORMAT

    def __init__(
        self,
        external_id: str,
        settings: OneLakeSettingsWrite,
        name: str | None = None,
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(external_id=external_id, name=name, data_set_id=data_set_id)
        self.settings = settings

    @classmethod
    def _load_data_source(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            settings=OneLakeSettingsWrite._load(resource["settings"]),
            name=resource.get("name"),
            data_set_id=resource.get("dataSetId"),
        )

    @override
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["settings"] = self.settings.dump(camel_case=camel_case)
        return output


class ExternalDataSource(WriteableCogniteResource[ExternalDataSourceWrite], ABC):
    """An external data source configured for use with CDF Transformations (API read model — returned
    by list/get).

    Format-specific subclasses (e.g. :class:`OneLakeExternalDataSource`) hold typed settings.

    Note:
        This API is in public beta. The contract may change before general availability.

    Args:
        external_id (str): External ID of the data source.
        created_time (int): Time the resource was created (milliseconds since epoch).
        last_updated_time (int): Time the resource was last updated (milliseconds since epoch).
        name (str | None): Display name for the external data source. Omitted when no name is set.
        data_set_id (int | None): Data set ID for ACL scoping.
    """

    _format: ClassVar[str]

    def __init__(
        self,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        name: str | None = None,
        data_set_id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.name = name
        self.data_set_id = data_set_id

    @classmethod
    @abstractmethod
    def _load_data_source(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> ExternalDataSource:
        format_ = resource.get("format")
        if format_ is None and hasattr(cls, "_format"):
            format_ = cls._format
        elif format_ is None:
            raise KeyError("format")
        source_class = _EXTERNAL_DATA_SOURCE_CLASS_BY_FORMAT.get(format_)
        if source_class is None:
            return UnknownCogniteResource(resource)  # type: ignore[return-value]
        return source_class._load_data_source(resource)

    @override
    def as_write(self) -> NoReturn:
        raise TypeError(
            f"{type(self).__name__} cannot be converted to write as the API does not return the client secret"
        )

    @override
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["format"] = self._format
        return output


class OneLakeExternalDataSource(ExternalDataSource):
    """A Fabric OneLake external data source configured for use with CDF Transformations (API read model).

    OneLake external data sources are read-only from a transform perspective — transforms can read data
    from OneLake tables via ``ext_onelake()`` SQL, but writing transform output to OneLake is not
    supported.

    Note:
        This API is in public beta. The contract may change before general availability.

    Args:
        external_id (str): External ID of the data source.
        settings (OneLakeSettings): Connection settings.
        created_time (int): Time the resource was created (milliseconds since epoch).
        last_updated_time (int): Time the resource was last updated (milliseconds since epoch).
        name (str | None): Display name for the external data source. Omitted when no name is set.
        data_set_id (int | None): Data set ID for ACL scoping.
    """

    _format: ClassVar[str] = ONE_LAKE_FORMAT

    def __init__(
        self,
        external_id: str,
        settings: OneLakeSettings,
        created_time: int,
        last_updated_time: int,
        name: str | None = None,
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            created_time=created_time,
            last_updated_time=last_updated_time,
            name=name,
            data_set_id=data_set_id,
        )
        self.settings = settings

    @classmethod
    def _load_data_source(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            settings=OneLakeSettings._load(resource["settings"]),
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            name=resource.get("name"),
            data_set_id=resource.get("dataSetId"),
        )

    @override
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["settings"] = self.settings.dump(camel_case=camel_case)
        return output


class ExternalDataSourceWriteList(CogniteResourceList[ExternalDataSourceWrite], ExternalIDTransformerMixin):
    """A list of ExternalDataSourceWrite objects."""

    _RESOURCE = ExternalDataSourceWrite


class ExternalDataSourceList(
    WriteableCogniteResourceList[ExternalDataSourceWrite, ExternalDataSource], ExternalIDTransformerMixin
):
    """A list of ExternalDataSource (read model) objects."""

    _RESOURCE = ExternalDataSource

    @override
    def as_write(self) -> NoReturn:
        raise TypeError(f"{type(self).__name__} cannot be converted to write")


class ExternalDataSourceUsability(CogniteResource):
    """Usability status for an external data source.

    Args:
        external_id (str): External ID of the verified data source.
        usable_version (str | None): Latest version of the data source when it can be used. Not present
            if the resource is missing or inaccessible.
    """

    def __init__(self, external_id: str, usable_version: str | None = None) -> None:
        self.external_id = external_id
        self.usable_version = usable_version

    @property
    def is_usable(self) -> bool:
        return self.usable_version is not None

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"]["externalId"],
            usable_version=resource.get("usableVersion"),
        )

    @override
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        key = "externalId" if camel_case else "external_id"
        if key in output:
            output[key] = {key: output[key]}
        return output


_EXTERNAL_DATA_SOURCE_WRITE_CLASS_BY_FORMAT: dict[str, type[ExternalDataSourceWrite]] = {
    subclass._format: subclass  # type: ignore[type-abstract]
    for subclass in ExternalDataSourceWrite.__subclasses__()
}

_EXTERNAL_DATA_SOURCE_CLASS_BY_FORMAT: dict[str, type[ExternalDataSource]] = {
    subclass._format: subclass  # type: ignore[type-abstract]
    for subclass in ExternalDataSource.__subclasses__()
}
