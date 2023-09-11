from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Group(CogniteResource):
    """No description.

    Args:
        name (str | None): Name of the group
        source_id (str | None): ID of the group in the source. If this is the same ID as a group in the IDP, a service account in that group will implicitly be a part of this group as well.
        capabilities (list[dict[str, Any]] | None): No description.
        id (int | None): No description.
        is_deleted (bool | None): No description.
        deleted_time (int | None): No description.
        metadata (dict[str, Any] | None): Custom, immutable application specific metadata. String key -> String value. Limits:
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        name: str | None = None,
        source_id: str | None = None,
        capabilities: list[dict[str, Any]] | None = None,
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


class GroupList(CogniteResourceList[Group]):
    _RESOURCE = Group


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
    def _load(cls, api_response: dict[str, Any]) -> ProjectSpec:
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
    def _load(cls, api_response: dict[str, Any]) -> TokenInspection:
        return cls(
            subject=api_response["subject"],
            projects=[ProjectSpec._load(p) for p in api_response["projects"]],
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
    def _load(cls, response: dict[str, Any]) -> CreatedSession:
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
