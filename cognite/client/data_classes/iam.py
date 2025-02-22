from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NoReturn, TypeAlias, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteResponse,
    CogniteUpdate,
    IdTransformerMixin,
    InternalIdTransformerMixin,
    NameTransformerMixin,
    PropertySpec,
    T_WriteClass,
    UnknownCogniteObject,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.capabilities import Capability, ProjectCapabilityList
from cognite.client.utils._importing import local_import

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client import CogniteClient


ALL_USER_ACCOUNTS = "allUserAccounts"


class GroupCore(WriteableCogniteResource["GroupWrite"], ABC):
    """No description.

    Args:
        name (str): Name of the group
        source_id (str | None): ID of the group in the source. If this is the same ID as a group in the IDP, a service account in that group will implicitly be a part of this group as well.
        capabilities (list[Capability] | None): List of capabilities (acls) this group should grant its users. Can not be used together with 'members'.
        metadata (dict[str, str] | None): Custom, immutable application specific metadata. String key -> String value. Limits: Key are at most 32 bytes. Values are at most 512 bytes. Up to 16 key-value pairs. Total size is at most 4096.
        members (Literal['allUserAccounts'] | list[str] | None): Specifies which users are members of the group. Can not be used together with 'source_id'.
    """

    def __init__(
        self,
        name: str,
        source_id: str | None = None,
        capabilities: list[Capability] | None = None,
        metadata: dict[str, str] | None = None,
        members: Literal["allUserAccounts"] | list[str] | None = None,
    ) -> None:
        self.name = name
        self.source_id = source_id
        self.capabilities = capabilities
        if isinstance(self.capabilities, Capability):
            self.capabilities = [capabilities]
        self.metadata = metadata
        self.members = members

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None, allow_unknown: bool = False) -> Self:
        return cls(
            name=resource["name"],
            source_id=resource.get("sourceId"),
            capabilities=[Capability.load(c, allow_unknown=allow_unknown) for c in resource.get("capabilities", [])]
            or None,
            metadata=resource.get("metadata"),
            members=resource.get("members"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case=camel_case)
        if self.capabilities is not None:
            dumped["capabilities"] = [c.dump(camel_case=camel_case) for c in self.capabilities]
        return dumped


class Group(GroupCore):
    """Groups are used to give principals the capabilities to access CDF resources. One principal
    can be a member in multiple groups and one group can have multiple members.

    Groups can either be managed through the external identity provider for the project or managed by CDF.

    Args:
        name (str): Name of the group
        source_id (str | None): ID of the group in the source. If this is the same ID as a group in the IDP, a service account in that group will implicitly be a part of this group as well.
        capabilities (list[Capability] | None): List of capabilities (acls) this group should grant its users. Can not be used together with 'members'.
        id (int | None): No description.
        is_deleted (bool | None): No description.
        deleted_time (int | None): No description.
        metadata (dict[str, str] | None): Custom, immutable application specific metadata. String key -> String value. Limits: Key are at most 32 bytes. Values are at most 512 bytes. Up to 16 key-value pairs. Total size is at most 4096.
        members (Literal['allUserAccounts'] | list[str] | None): Specifies which users are members of the group. Can not be used together with 'source_id'.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        name: str,
        source_id: str | None = None,
        capabilities: list[Capability] | None = None,
        id: int | None = None,
        is_deleted: bool | None = None,
        deleted_time: int | None = None,
        metadata: dict[str, str] | None = None,
        members: Literal["allUserAccounts"] | list[str] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(
            name=name,
            source_id=source_id,
            capabilities=capabilities,
            metadata=metadata,
            members=members,
        )
        # id is required when using the class to read, but doesn't make sense passing in when
        # creating a new object. So in order to make the typing correct here
        # (i.e. int and not Optional[int]), we force the type to be int rather than Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> GroupWrite:
        """Returns a writing version of this group."""
        return GroupWrite(
            name=self.name,
            source_id=self.source_id,
            capabilities=self.capabilities,
            metadata=self.metadata,
            members=self.members,
        )

    @property
    def is_externally_managed(self) -> bool:
        # Note: `source_id` may be returned as the empty string, hence the 'truthy' test
        if self.source_id and self.members is not None:
            raise ValueError("Managed status is unknown")
        return self.source_id is not None

    @property
    def is_managed_in_cdf(self) -> bool:
        if self.source_id and self.members is not None:
            raise ValueError("Managed status is unknown")
        return self.members is not None

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None, allow_unknown: bool = False) -> Group:
        return cls(
            name=resource["name"],
            source_id=resource.get("sourceId"),
            capabilities=[Capability.load(c, allow_unknown) for c in resource.get("capabilities", [])] or None,
            id=resource.get("id"),
            is_deleted=resource.get("isDeleted"),
            deleted_time=resource.get("deletedTime"),
            metadata=resource.get("metadata"),
            members=resource.get("members"),
            cognite_client=cognite_client,
        )

    def to_pandas(
        self,
        expand_metadata: bool = False,
        metadata_prefix: str = "metadata.",
        ignore: list[str] | None = None,
        camel_case: bool = False,
        convert_timestamps: bool = True,
    ) -> pd.DataFrame:
        df = super().to_pandas(expand_metadata, metadata_prefix, ignore, camel_case, convert_timestamps)

        # The API uses -1 to represent "no deleted time". It looks weird if deleted = False,
        # but deleted_time = 1969-12-31 23:59:59.999:
        key = "deletedTime" if camel_case else "deleted_time"
        if self.deleted_time == -1 and convert_timestamps and key in df.index:
            df.at[key, "value"] = local_import("pandas").NaT
        return df


class GroupWrite(GroupCore):
    """Groups are used to give principals the capabilities to access CDF resources. One principal
    can be a member in multiple groups and one group can have multiple members.

    Groups can either be managed through the external identity provider for the project or managed by CDF.

    Args:
        name (str): Name of the group
        source_id (str | None): ID of the group in the source. If this is the same ID as a group in the IDP, a service account in that group will implicitly be a part of this group as well.
        capabilities (list[Capability] | None): List of capabilities (acls) this group should grant its users. Can not be used together with 'members'.
        metadata (dict[str, str] | None): Custom, immutable application specific metadata. String key -> String value. Limits: Key are at most 32 bytes. Values are at most 512 bytes. Up to 16 key-value pairs. Total size is at most 4096.
        members (Literal['allUserAccounts'] | list[str] | None): Specifies which users are members of the group. Can not be used together with 'source_id'.
    """

    def __init__(
        self,
        name: str,
        source_id: str | None = None,
        capabilities: list[Capability] | None = None,
        metadata: dict[str, str] | None = None,
        members: Literal["allUserAccounts"] | list[str] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            source_id=source_id,
            capabilities=capabilities,
            metadata=metadata,
            members=members,
        )

    def as_write(self) -> GroupWrite:
        """Returns this GroupWrite instance."""
        return self


class GroupWriteList(CogniteResourceList[GroupWrite], NameTransformerMixin):
    _RESOURCE = GroupWrite

    @classmethod
    def _load(
        cls,
        resource_list: Iterable[dict[str, Any]],
        cognite_client: CogniteClient | None = None,
        allow_unknown: bool = False,
    ) -> Self:
        return cls(
            [cls._RESOURCE._load(res, cognite_client, allow_unknown) for res in resource_list],
            cognite_client=cognite_client,
        )


class GroupList(WriteableCogniteResourceList[GroupWrite, Group], NameTransformerMixin, InternalIdTransformerMixin):
    _RESOURCE = Group

    @classmethod
    def _load(
        cls,
        resource_list: Iterable[dict[str, Any]],
        cognite_client: CogniteClient | None = None,
        allow_unknown: bool = False,
    ) -> Self:
        return cls(
            [cls._RESOURCE._load(res, cognite_client, allow_unknown) for res in resource_list],
            cognite_client=cognite_client,
        )

    def as_write(self) -> GroupWriteList:
        """Returns a writing version of this group list."""
        return GroupWriteList([s.as_write() for s in self], cognite_client=self._get_cognite_client())

    def to_pandas(
        self,
        camel_case: bool = False,
        expand_metadata: bool = False,
        metadata_prefix: str = "metadata.",
        convert_timestamps: bool = True,
    ) -> pd.DataFrame:
        df = super().to_pandas(camel_case, expand_metadata, metadata_prefix, convert_timestamps)

        # The API uses -1 to represent "no deleted time". It looks weird if deleted = False,
        # but deleted_time = 1969-12-31 23:59:59.999:
        key = "deletedTime" if camel_case else "deleted_time"
        if convert_timestamps and key in df:
            pd = local_import("pandas")
            df.loc[df[key] == pd.Timestamp(-1, unit="ms"), key] = pd.NaT
        return df


class SecurityCategoryCore(WriteableCogniteResource["SecurityCategoryWrite"], ABC):
    """No description.

    Args:
        name (str | None): Name of the security category
    """

    def __init__(self, name: str | None = None) -> None:
        self.name = name


class SecurityCategory(SecurityCategoryCore):
    """Security categories can be used to restrict access to a resource.
    This is the reading version of a security category, which is used when retrieving security categories.

    Args:
        name (str | None): Name of the security category
        id (int | None): Id of the security category
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self, name: str | None = None, id: int | None = None, cognite_client: CogniteClient | None = None
    ) -> None:
        super().__init__(name=name)
        self.id = id
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> SecurityCategoryWrite:
        """Returns a writing version of this security category."""
        if self.name is None:
            raise ValueError("SecurityCategory must have an id to be used as write")
        return SecurityCategoryWrite(name=self.name)


class SecurityCategoryWrite(SecurityCategoryCore):
    """Security categories can be used to restrict access to a resource.
    This is the writing version of a security category, which is used when creating security categories.


    Args:
        name (str): Name of the security category
    """

    def __init__(self, name: str) -> None:
        super().__init__(name=name)

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(name=resource["name"])

    def as_write(self) -> SecurityCategoryWrite:
        """Returns this SecurityCategoryWrite instance."""
        return self


class SecurityCategoryWriteList(CogniteResourceList[SecurityCategoryWrite], NameTransformerMixin):
    _RESOURCE = SecurityCategoryWrite


class SecurityCategoryList(
    WriteableCogniteResourceList[SecurityCategoryWrite, SecurityCategory],
    InternalIdTransformerMixin,
    NameTransformerMixin,
):
    _RESOURCE = SecurityCategory

    def as_write(self) -> SecurityCategoryWriteList:
        """Returns a writing version of this security category list."""
        return SecurityCategoryWriteList([s.as_write() for s in self], cognite_client=self._get_cognite_client())


class ProjectSpec(CogniteResponse):
    """A CDF project spec

    Args:
        url_name (str): The url name for the project
        groups (list[int]): Group ids in the project
    """

    def __init__(self, url_name: str, groups: list[int]) -> None:
        self.url_name = url_name
        self.groups = groups

    @property
    def project_url_name(self) -> str:
        return self.url_name

    @classmethod
    def load(cls, api_response: dict[str, Any]) -> ProjectSpec:
        return cls(url_name=api_response["projectUrlName"], groups=api_response["groups"])

    def dump(self, camel_case: bool = True) -> dict[str, str | list[int]]:
        return {
            "projectUrlName" if camel_case else "project_url_name": self.url_name,
            "groups": self.groups,
        }


class TokenInspection(CogniteResponse):
    """Current login status

    Args:
        subject (str): Subject (sub claim) of JWT.
        projects (list[ProjectSpec]): Projects this token is valid for.
        capabilities (ProjectCapabilityList): Capabilities associated with this token.
    """

    def __init__(self, subject: str, projects: list[ProjectSpec], capabilities: ProjectCapabilityList) -> None:
        self.subject = subject
        self.projects = projects
        self.capabilities = capabilities

    @classmethod
    def load(
        cls, api_response: dict[str, Any], cognite_client: CogniteClient | None = None, allow_unknown: bool = False
    ) -> TokenInspection:
        return cls(
            subject=api_response["subject"],
            projects=[ProjectSpec.load(p) for p in api_response["projects"]],
            capabilities=ProjectCapabilityList._load(api_response["capabilities"], cognite_client, allow_unknown),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "subject": self.subject,
            "projects": [p.dump(camel_case=camel_case) for p in self.projects],
            "capabilities": self.capabilities.dump(camel_case=camel_case),
        }


SessionStatus: TypeAlias = Literal["READY", "ACTIVE", "CANCELLED", "EXPIRED", "REVOKED", "ACCESS_LOST"]
SessionType: TypeAlias = Literal["CLIENT_CREDENTIALS", "TOKEN_EXCHANGE", "ONESHOT_TOKEN_EXCHANGE"]


class CreatedSession(CogniteResponse):
    """Session creation related information

    Args:
        id (int): ID of the created session.
        status (SessionStatus): Current status of the session.
        nonce (str): Nonce to be passed to the internal service that will bind the session
        type (SessionType | None): Credentials kind used to create the session.
        client_id (str | None): Client ID in identity provider. Returned only if the session was created using client credentials
    """

    def __init__(
        self,
        id: int,
        status: SessionStatus,
        nonce: str,
        type: SessionType | None = None,
        client_id: str | None = None,
    ) -> None:
        self.id = id
        self.status = status
        self.nonce = nonce
        self.type = type
        self.client_id = client_id

    @classmethod
    def load(cls, response: dict[str, Any]) -> CreatedSession:
        return cls(
            id=response["id"],
            status=response["status"],
            nonce=response["nonce"],
            type=response.get("type"),
            client_id=response.get("clientId"),
        )


class Session(CogniteResource):
    """Session status

    Args:
        id (int | None): ID of the session.
        type (SessionType | None): Credentials kind used to create the session.
        status (SessionStatus | None): Current status of the session.
        creation_time (int | None): Session creation time, in milliseconds since 1970
        expiration_time (int | None): Session expiry time, in milliseconds since 1970. This value is updated on refreshing a token
        client_id (str | None): Client ID in identity provider. Returned only if the session was created using client credentials
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        id: int | None = None,
        type: SessionType | None = None,
        status: SessionStatus | None = None,
        creation_time: int | None = None,
        expiration_time: int | None = None,
        client_id: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.id = id
        self.type = type
        self.status = status
        self.creation_time = creation_time
        self.expiration_time = expiration_time
        self.client_id = client_id


class SessionList(CogniteResourceList[Session], IdTransformerMixin):
    _RESOURCE = Session


class ClientCredentials(CogniteResource):
    """Client credentials for session creation

    Args:
        client_id (str): Client ID from identity provider.
        client_secret (str): Client secret from identity provider.
    """

    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "clientId" if camel_case else "client_id": self.client_id,
            "clientSecret" if camel_case else "client_secret": self.client_secret,
        }

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> ClientCredentials:
        return cls(client_id=resource["clientId"], client_secret=resource["clientSecret"])


class PrincipalWrite(CogniteResource, ABC):
    """Base class for all principals in write/request format."""

    _type: ClassVar[str]

    @classmethod
    @abstractmethod
    def _load_principal(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        try:
            principal_cls = _PRINCIPAL_WRITE_CLASS_BY_TYPE[type_.upper()]
        except KeyError:
            raise ValueError(f"Unknown principal type {type_}")
        return cast(Self, principal_cls._load_principal(resource))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type.upper()
        return output


class Principal(WriteableCogniteResource[T_WriteClass], ABC):
    """Base class for all principals in read/response format."""

    _type: ClassVar[str]

    @classmethod
    @abstractmethod
    def _load_principal(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        principal_class = _PRINCIPAL_CLASS_BY_TYPE.get(type_.casefold())
        if principal_class is None:
            return UnknownCogniteObject(resource)  # type: ignore[return-value]
        return cast(Self, principal_class._load_principal(resource))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type.upper()
        return output


class PrincipalUpdate(CogniteUpdate, ABC):
    _type: ClassVar[str]

    def dump(self, camel_case: Literal[True] = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type
        return output

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        if item is None or not isinstance(item, PrincipalWrite):
            return []
        return _PRINCIPAL_UPDATE_BY_TYPE[item._type]._get_update_properties(item)


class PrincipalWriteList(CogniteResourceList[PrincipalWrite]):
    _RESOURCE = PrincipalWrite


class PrincipalList(WriteableCogniteResourceList[PrincipalWrite, Principal]):
    _RESOURCE = Principal

    def as_write(
        self,
    ) -> PrincipalWriteList:
        return PrincipalWriteList([item.as_write() for item in self.data])


@dataclass
class PrincipalId(CogniteObject):
    """
    The ID of a principal.

    Args:
        org_id (str): The ID of an organization
        user_id (str): The ID of an organization user
    """

    org_id: str
    user_id: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(org_id=resource["orgId"], user_id=resource["userId"])


class ServiceAccountWrite(PrincipalWrite):
    """A service account.

    This is the write/request format of the service account dto.

    Args:
        name (str): Human-readable name of a service account
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        description (str | None): Longer description of a service account

    """

    _type = "SERVICE_ACCOUNT"

    def __init__(self, name: str, external_id: str | None = None, description: str | None = None) -> None:
        self.name = name
        self.external_id = external_id
        self.description = description

    def as_write(self) -> ServiceAccountWrite:
        return self

    @classmethod
    def _load_principal(cls, resource: dict[str, Any]) -> Self:
        return cls(
            name=resource["name"],
            external_id=resource.get("externalId"),
            description=resource.get("description"),
        )


class ServiceAccount(Principal):
    """A service account.

    This is the read/response format of the service account.

    Args:
        id (str): Unique identifier of a service account
        name (str): Human-readable name of a service account
        created_by (PrincipalId): The ID of an organization user
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        picture_url (str): URL to a picture of the principal
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        description (str | None): Longer description of a service account

    """

    _type = "service_account"

    def __init__(
        self,
        id: str,
        name: str,
        created_by: PrincipalId,
        created_time: int,
        last_updated_time: int,
        picture_url: str,
        external_id: str | None = None,
        description: str | None = None,
    ) -> None:
        self.name = name
        self.external_id = external_id
        self.description = description
        self.id = id
        self.created_by = created_by
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.picture_url = picture_url

    def as_write(self) -> ServiceAccountWrite:
        return ServiceAccountWrite(
            external_id=self.external_id,
            description=self.description,
            name=self.name,
        )

    @classmethod
    def _load_principal(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            external_id=resource.get("externalId"),
            name=resource["name"],
            description=resource.get("description"),
            created_by=PrincipalId._load(resource["createdBy"]),
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            picture_url=resource["pictureUrl"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["createdBy" if camel_case else "created_by"] = self.created_by.dump(camel_case)
        return output


class ServiceAccountUpdate(PrincipalUpdate):
    _type = "service_account"

    class _NullableStringServiceAccountUpdate(CognitePrimitiveUpdate):
        def set(self, value: str | None) -> ServiceAccountUpdate:
            return self._set(value)

    class _StringServiceAccountUpdate(CognitePrimitiveUpdate):
        def set(self, value: str) -> ServiceAccountUpdate:
            return self._set(value)

    @property
    def name(self) -> _StringServiceAccountUpdate:
        return ServiceAccountUpdate._StringServiceAccountUpdate(self, "name")

    @property
    def description(self) -> _NullableStringServiceAccountUpdate:
        return ServiceAccountUpdate._NullableStringServiceAccountUpdate(self, "description")

    @property
    def external_id(self) -> _NullableStringServiceAccountUpdate:
        return ServiceAccountUpdate._NullableStringServiceAccountUpdate(self, "externalId")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("description", is_nullable=True),
            PropertySpec("external_id", is_nullable=True),
        ]


class ServiceAccountSecretCore(WriteableCogniteResource["ServiceAccountSecretWrite"], ABC): ...


class ServiceAccountSecretWrite(ServiceAccountSecretCore):
    """A service account secret.

    This is the write/request format of the service account secret dto.

    Args:
        expires_in_seconds (int | None): The number of seconds until the secret expires. The maximum value is 180 days.

    """

    def __init__(self, expires_in_seconds: int | None = None) -> None:
        self.expires_in_seconds = expires_in_seconds

    def as_write(self) -> ServiceAccountSecretWrite:
        return self


class ServiceAccountSecret(ServiceAccountSecretCore):
    """A service account secret.

    This is the read/response format of the service account secret dto.

    Args:
        id (int): Unique identifier of a service account secret
        secret (str): A client secret for a service account. This value is sensitive, and should be stored securely.
            It will only be shown right after creation, and will not be retrievable later.
        created_by (PrincipalId): The ID of an organization user
        expiration_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_token_issuance_time (str | None): The time when this secret was last used to issue a CDF access token.

    """

    def __init__(
        self,
        id: int,
        secret: str,
        created_by: PrincipalId,
        expiration_time: int,
        created_time: int,
        last_token_issuance_time: str | None = None,
    ) -> None:
        self.id = id
        self.secret = secret
        self.created_by = created_by
        self.expiration_time = expiration_time
        self.created_time = created_time
        self.last_token_issuance_time = last_token_issuance_time

    def as_write(self) -> NoReturn:
        raise TypeError(f"{type(self).__name__} cannot be converted to a write object")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["createdBy" if camel_case else "created_by"] = self.created_by.dump(camel_case)
        return output

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            id=resource["id"],
            secret=resource["secret"],
            created_by=PrincipalId._load(resource["createdBy"]),
            expiration_time=resource["expirationTime"],
            created_time=resource["createdTime"],
            last_token_issuance_time=resource.get("lastTokenIssuanceTime"),
        )


class ServiceAccountSecretWriteList(CogniteResourceList[ServiceAccountSecretWrite]):
    _RESOURCE = ServiceAccountSecretWrite


class ServiceAccountSecretList(
    WriteableCogniteResourceList[ServiceAccountSecretWrite, ServiceAccountSecret], InternalIdTransformerMixin
):
    _RESOURCE = ServiceAccountSecret

    def as_write(self) -> NoReturn:
        raise TypeError(f"{type(self).__name__} cannot be converted to a write object")


_PRINCIPAL_WRITE_CLASS_BY_TYPE: dict[str, type[PrincipalWrite]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in PrincipalWrite.__subclasses__()
}

_PRINCIPAL_CLASS_BY_TYPE: dict[str, type[Principal]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in Principal.__subclasses__()
}

_PRINCIPAL_UPDATE_BY_TYPE: dict[str, type[PrincipalUpdate]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in PrincipalUpdate.__subclasses__()
}
