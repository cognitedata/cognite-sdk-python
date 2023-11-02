from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse
from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client import CogniteClient


class Group(CogniteResource):
    """No description.

    Args:
        name (str): Name of the group
        source_id (str | None): ID of the group in the source. If this is the same ID as a group in the IDP, a service account in that group will implicitly be a part of this group as well.
        capabilities (list[Capability] | None): No description.
        id (int | None): No description.
        is_deleted (bool | None): No description.
        deleted_time (int | None): No description.
        metadata (dict[str, Any] | None): Custom, immutable application specific metadata. String key -> String value. Limits:
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        name: str,
        source_id: str | None = None,
        capabilities: list[Capability] | None = None,
        id: int | None = None,
        is_deleted: bool | None = None,
        deleted_time: int | None = None,
        metadata: dict[str, Any] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.name = name
        self.source_id = source_id
        self.capabilities = capabilities
        self.id = id
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self.metadata = metadata
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            name=resource["name"],
            source_id=resource.get("sourceId"),
            capabilities=[Capability.load(c) for c in resource.get("capabilities", [])] or None,
            id=resource.get("id"),
            is_deleted=resource.get("isDeleted"),
            deleted_time=resource.get("deletedTime"),
            metadata=resource.get("metadata"),
            cognite_client=cognite_client,
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        dumped = super().dump(camel_case=camel_case)
        if self.capabilities is not None:
            dumped["capabilities"] = [c.dump(camel_case=camel_case) for c in self.capabilities]
        return dumped

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


class GroupList(CogniteResourceList[Group]):
    _RESOURCE = Group

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


class SecurityCategory(CogniteResource):
    """No description.

    Args:
        name (str | None): Name of the security category
        id (int | None): Id of the security category
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self, name: str | None = None, id: int | None = None, cognite_client: CogniteClient | None = None
    ) -> None:
        self.name = name
        self.id = id
        self._cognite_client = cast("CogniteClient", cognite_client)


class SecurityCategoryList(CogniteResourceList[SecurityCategory]):
    _RESOURCE = SecurityCategory


class ProjectSpec(CogniteResponse):
    """A CDF project spec

    Args:
        url_name (str): The url name for the project
        groups (list[int]): Group ids in the project
    """

    def __init__(self, url_name: str, groups: list[int]) -> None:
        self.url_name = url_name
        self.groups = groups

    @classmethod
    def load(cls, api_response: dict[str, Any]) -> ProjectSpec:
        return cls(url_name=api_response["projectUrlName"], groups=api_response["groups"])


class TokenInspection(CogniteResponse):
    """Current login status

    Args:
        subject (str): Subject (sub claim) of JWT.
        projects (list[ProjectSpec]): Projects this token is valid for.
        capabilities (list[dict]): Capabilities associated with this token.
    """

    def __init__(self, subject: str, projects: list[ProjectSpec], capabilities: list[dict]) -> None:
        self.subject = subject
        self.projects = projects
        self.capabilities = capabilities

    @classmethod
    def load(cls, api_response: dict[str, Any]) -> TokenInspection:
        return cls(
            subject=api_response["subject"],
            projects=[ProjectSpec.load(p) for p in api_response["projects"]],
            capabilities=api_response["capabilities"],
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        dumped = {
            "subject": self.subject,
            "projects": [p.dump(camel_case=camel_case) for p in self.projects],
            "capabilities": self.capabilities,
        }
        if camel_case:
            dumped = convert_all_keys_to_camel_case(dumped)
        return dumped


class CreatedSession(CogniteResponse):
    """Session creation related information

    Args:
        id (int): ID of the created session.
        status (str): Current status of the session.
        nonce (str): Nonce to be passed to the internal service that will bind the session
        type (str | None): Credentials kind used to create the session.
        client_id (str | None): Client ID in identity provider. Returned only if the session was created using client credentials
    """

    def __init__(
        self,
        id: int,
        status: str,
        nonce: str,
        type: str | None = None,
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
        type (str | None): Credentials kind used to create the session.
        status (str | None): Current status of the session.
        creation_time (int | None): Session creation time, in milliseconds since 1970
        expiration_time (int | None): Session expiry time, in milliseconds since 1970. This value is updated on refreshing a token
        client_id (str | None): Client ID in identity provider. Returned only if the session was created using client credentials
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        id: int | None = None,
        type: str | None = None,
        status: str | None = None,
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


class SessionList(CogniteResourceList[Session]):
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

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            "clientId" if camel_case else "client_id": self.client_id,
            "clientSecret" if camel_case else "client_secret": self.client_secret,
        }

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> ClientCredentials:
        return cls(client_id=resource["clientId"], client_secret=resource["clientSecret"])
