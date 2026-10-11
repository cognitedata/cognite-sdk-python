from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Literal, NoReturn, TypeAlias

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    UnknownCogniteResource,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

ONE_LAKE_FORMAT = "one_lake"
SNOWFLAKE_FORMAT = "snowflake"

# Typing alias only: dispatch stays stringly-typed (plain str lookup in the registries below) for forward
# compatibility. Unknown formats load as UnknownCogniteResource on read and raise TypeError on write.
ExternalDataSourceFormat: TypeAlias = Literal["one_lake", "snowflake"]


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

    _SENSITIVE_FIELDS: ClassVar[frozenset[str]] = frozenset({"client_secret"})

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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["credentials"] = self.credentials.dump(camel_case=camel_case)
        key = "locationDescription" if camel_case else "location_description"
        output[key] = self.location_description.dump(camel_case=camel_case)
        return output


class OneLakeSettingsWrite(CogniteResource):
    """Connection settings for the external data source.

    Args:
        credentials (OneLakeCredentialsWrite): Azure credentials for the data source.
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["credentials"] = self.credentials.dump(camel_case=camel_case)
        key = "locationDescription" if camel_case else "location_description"
        output[key] = self.location_description.dump(camel_case=camel_case)
        return output


class SnowflakeLocationDescription(CogniteResource):
    """Location of the data within Snowflake.

    Args:
        warehouse_name (str): Name of the Snowflake warehouse used to run queries.
    """

    def __init__(self, warehouse_name: str) -> None:
        self.warehouse_name = warehouse_name

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(warehouse_name=resource["warehouseName"])


class SnowflakeCredentials(CogniteResource):
    """Credentials used to authenticate with Snowflake (key-pair authentication). The private key is never
    included when reading a data source.

    Args:
        account_identifier (str): Snowflake account identifier, in the form ``<orgname>-<accountname>``.
        user_name (str): Snowflake user name.
        role_name (str): Snowflake role to use.
        public_key (str): Public key generated by Cognite. Register it with the Snowflake user via
            ``ALTER USER ... SET RSA_PUBLIC_KEY``.
    """

    def __init__(self, account_identifier: str, user_name: str, role_name: str, public_key: str) -> None:
        self.account_identifier = account_identifier
        self.user_name = user_name
        self.role_name = role_name
        self.public_key = public_key

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            account_identifier=resource["accountIdentifier"],
            user_name=resource["userName"],
            role_name=resource["roleName"],
            public_key=resource["publicKey"],
        )


class SnowflakeCredentialsWrite(CogniteResource):
    """Credentials used to authenticate with Snowflake (key-pair authentication). No secret is supplied:
    Cognite generates the RSA key pair server-side.

    Args:
        account_identifier (str): Snowflake account identifier, in the form ``<orgname>-<accountname>``.
        user_name (str): Snowflake user name.
        role_name (str): Snowflake role to use.
    """

    def __init__(self, account_identifier: str, user_name: str, role_name: str) -> None:
        self.account_identifier = account_identifier
        self.user_name = user_name
        self.role_name = role_name

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            account_identifier=resource["accountIdentifier"],
            user_name=resource["userName"],
            role_name=resource["roleName"],
        )


class SnowflakeSettings(CogniteResource):
    """Connection settings for the Snowflake external data source, including the generated public key.

    Args:
        credentials (SnowflakeCredentials): Snowflake credentials (including the public key to register).
        location_description (SnowflakeLocationDescription): Snowflake warehouse.
    """

    def __init__(self, credentials: SnowflakeCredentials, location_description: SnowflakeLocationDescription) -> None:
        self.credentials = credentials
        self.location_description = location_description

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            credentials=SnowflakeCredentials._load(resource["credentials"]),
            location_description=SnowflakeLocationDescription._load(resource["locationDescription"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["credentials"] = self.credentials.dump(camel_case=camel_case)
        key = "locationDescription" if camel_case else "location_description"
        output[key] = self.location_description.dump(camel_case=camel_case)
        return output


class SnowflakeSettingsWrite(CogniteResource):
    """Connection settings for the Snowflake external data source.

    Args:
        credentials (SnowflakeCredentialsWrite): Snowflake credentials for the data source.
        location_description (SnowflakeLocationDescription): Snowflake warehouse.
    """

    def __init__(
        self, credentials: SnowflakeCredentialsWrite, location_description: SnowflakeLocationDescription
    ) -> None:
        self.credentials = credentials
        self.location_description = location_description

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            credentials=SnowflakeCredentialsWrite._load(resource["credentials"]),
            location_description=SnowflakeLocationDescription._load(resource["locationDescription"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["credentials"] = self.credentials.dump(camel_case=camel_case)
        key = "locationDescription" if camel_case else "location_description"
        output[key] = self.location_description.dump(camel_case=camel_case)
        return output


class ExternalDataSourceWrite(CogniteResource, ABC):
    """Write model for an external data source to create in CDF.

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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["format"] = self._format
        return output


class OneLakeExternalDataSourceWrite(ExternalDataSourceWrite):
    """Write model for registering a Fabric OneLake external data source in CDF.

    Registers Azure credentials and lakehouse location so transforms can read via ``ext_onelake()``.
    Does not write data into OneLake. Fails if a data source with the given ``external_id`` already
    exists; it is not merged or replaced.

    Args:
        external_id (str): External ID for the data source. Must be unique.
        settings (OneLakeSettingsWrite): OneLake credentials and location.
        name (str | None): Display name for the external data source.
        data_set_id (int | None): Data set ID for ACL scoping.

    Examples:

        Construct a Fabric OneLake source for creation:

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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["settings"] = self.settings.dump(camel_case=camel_case)
        return output


class SnowflakeExternalDataSourceWrite(ExternalDataSourceWrite):
    """Write model for registering a Snowflake external data source in CDF.

    Cognite generates the RSA key pair server-side. After creation, read the data source back and register
    the returned ``public_key`` with the Snowflake user via ``ALTER USER ... SET RSA_PUBLIC_KEY``.
    Fails if a data source with the given ``external_id`` already exists; it is not merged or replaced.

    Args:
        external_id (str): External ID for the data source. Must be unique.
        settings (SnowflakeSettingsWrite): Snowflake credentials and location.
        expiry_time (int): Time when the data source's credentials expire (milliseconds since epoch).
            Required. Must be within one year from now; the API rejects later values.
        name (str | None): Display name for the external data source.
        data_set_id (int | None): Data set ID for ACL scoping.

    Examples:

        Construct a Snowflake source for creation, with credentials expiring in 90 days:

            >>> import time
            >>> from cognite.client.data_classes.transformations.externaldata import (
            ...     SnowflakeCredentialsWrite,
            ...     SnowflakeExternalDataSourceWrite,
            ...     SnowflakeLocationDescription,
            ...     SnowflakeSettingsWrite,
            ... )
            >>> source = SnowflakeExternalDataSourceWrite(
            ...     external_id="snowflake-analytics-prod",
            ...     name="Production warehouse",
            ...     data_set_id=123456,
            ...     settings=SnowflakeSettingsWrite(
            ...         credentials=SnowflakeCredentialsWrite(
            ...             account_identifier="<orgname>-<accountname>",
            ...             user_name="COGNITE_SVC",
            ...             role_name="COGNITE_READER",
            ...         ),
            ...         location_description=SnowflakeLocationDescription(
            ...             warehouse_name="COMPUTE_WH"
            ...         ),
            ...     ),
            ...     expiry_time=int((time.time() + 90 * 24 * 3600) * 1000),
            ... )
    """

    _format: ClassVar[str] = SNOWFLAKE_FORMAT

    def __init__(
        self,
        external_id: str,
        settings: SnowflakeSettingsWrite,
        expiry_time: int,
        name: str | None = None,
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(external_id=external_id, name=name, data_set_id=data_set_id)
        self.settings = settings
        self.expiry_time = expiry_time

    @classmethod
    def _load_data_source(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            settings=SnowflakeSettingsWrite._load(resource["settings"]),
            expiry_time=resource["expiryTime"],
            name=resource.get("name"),
            data_set_id=resource.get("dataSetId"),
        )

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

    def as_write(self) -> NoReturn:
        raise TypeError(
            f"{type(self).__name__} cannot be converted to write as the API does not return the stored credential material"
        )

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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        output["settings"] = self.settings.dump(camel_case=camel_case)
        return output


class SnowflakeExternalDataSource(ExternalDataSource):
    """A Snowflake external data source configured for use with CDF Transformations (API read model).

    Note:
        This API is in private preview. The contract may change before general availability.

    Args:
        external_id (str): External ID of the data source.
        settings (SnowflakeSettings): Connection settings, including the generated public key.
        created_time (int): Time the resource was created (milliseconds since epoch).
        last_updated_time (int): Time the resource was last updated (milliseconds since epoch).
        name (str | None): Display name for the external data source. Omitted when no name is set.
        data_set_id (int | None): Data set ID for ACL scoping.
        expiry_time (int | None): Time when the data source's credentials expire (milliseconds since epoch).
    """

    _format: ClassVar[str] = SNOWFLAKE_FORMAT

    def __init__(
        self,
        external_id: str,
        settings: SnowflakeSettings,
        created_time: int,
        last_updated_time: int,
        name: str | None = None,
        data_set_id: int | None = None,
        expiry_time: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            created_time=created_time,
            last_updated_time=last_updated_time,
            name=name,
            data_set_id=data_set_id,
        )
        self.settings = settings
        self.expiry_time = expiry_time

    @classmethod
    def _load_data_source(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            settings=SnowflakeSettings._load(resource["settings"]),
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            name=resource.get("name"),
            data_set_id=resource.get("dataSetId"),
            expiry_time=resource.get("expiryTime"),
        )

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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        key = "externalId" if camel_case else "external_id"
        if key in output:
            output[key] = {key: output[key]}
        return output


class ExternalDataSourceRotateKeys(CogniteResource):
    """Request to rotate the key pair of a Snowflake external data source.

    Args:
        external_id (str): External ID of the data source whose keys should be rotated.
        expiry_time (int): Time when the newly generated credentials expire (milliseconds since epoch).
            Required. Must be within one year from now; the API rejects later values.
    """

    def __init__(self, external_id: str, expiry_time: int) -> None:
        self.external_id = external_id
        self.expiry_time = expiry_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(external_id=resource["externalId"], expiry_time=resource["expiryTime"])


class ExternalDataSourceRotatedKeys(CogniteResource):
    """Result of rotating the key pair of a Snowflake external data source.

    Args:
        external_id (str): External ID of the data source whose keys were rotated.
        public_key (str): Newly generated public key. Register it with the Snowflake user via
            ``ALTER USER ... SET RSA_PUBLIC_KEY``.
        expiry_time (int): Time when the new credentials expire (milliseconds since epoch).
    """

    def __init__(self, external_id: str, public_key: str, expiry_time: int) -> None:
        self.external_id = external_id
        self.public_key = public_key
        self.expiry_time = expiry_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            public_key=resource["publicKey"],
            expiry_time=resource["expiryTime"],
        )


_EXTERNAL_DATA_SOURCE_WRITE_CLASS_BY_FORMAT: dict[str, type[ExternalDataSourceWrite]] = {
    subclass._format: subclass  # type: ignore[type-abstract]
    for subclass in ExternalDataSourceWrite.__subclasses__()
}

_EXTERNAL_DATA_SOURCE_CLASS_BY_FORMAT: dict[str, type[ExternalDataSource]] = {
    subclass._format: subclass  # type: ignore[type-abstract]
    for subclass in ExternalDataSource.__subclasses__()
}
