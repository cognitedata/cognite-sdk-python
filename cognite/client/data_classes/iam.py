from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class ServiceAccount(CogniteResource):
    """No description.

    Args:
        name (str): Unique name of the service account
        groups (List[int]): List of group ids
        id (int): No description.
        is_deleted (bool): If this service account has been logically deleted
        deleted_time (int): Time of deletion
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str | None = None,
        groups: list[int] | None = None,
        id: int | None = None,
        is_deleted: bool | None = None,
        deleted_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ):
        self.name = name
        self.groups = groups
        self.id = id
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self._cognite_client = cast("CogniteClient", cognite_client)


class ServiceAccountList(CogniteResourceList):
    _RESOURCE = ServiceAccount


class APIKey(CogniteResource):
    """No description.

    Args:
        id (int): The internal ID for the API key.
        service_account_id (int): The ID of the service account.
        created_time (int): The time of creation in Unix milliseconds.
        status (str): The status of the API key.
        value (str): The API key to be used against the API.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int | None = None,
        service_account_id: int | None = None,
        created_time: int | None = None,
        status: str | None = None,
        value: str | None = None,
        cognite_client: CogniteClient | None = None,
    ):
        self.id = id
        self.service_account_id = service_account_id
        self.created_time = created_time
        self.status = status
        self.value = value
        self._cognite_client = cast("CogniteClient", cognite_client)


class APIKeyList(CogniteResourceList):
    _RESOURCE = APIKey


class Group(CogniteResource):
    """No description.

    Args:
        name (str): Name of the group
        source_id (str): ID of the group in the source. If this is the same ID as a group in the IDP, a service account in that group will implicitly be a part of this group as well.
        capabilities (List[Dict[str, Any]]): No description.
        id (int): No description.
        is_deleted (bool): No description.
        deleted_time (int): No description.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str | None = None,
        source_id: str | None = None,
        capabilities: list[dict[str, Any]] | None = None,
        id: int | None = None,
        is_deleted: bool | None = None,
        deleted_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ):
        self.name = name
        self.source_id = source_id
        self.capabilities = capabilities
        self.id = id
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self._cognite_client = cast("CogniteClient", cognite_client)


class GroupList(CogniteResourceList):
    _RESOURCE = Group


class SecurityCategory(CogniteResource):
    """No description.

    Args:
        name (str): Name of the security category
        id (int): Id of the security category
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, name: str | None = None, id: int | None = None, cognite_client: CogniteClient | None = None):
        self.name = name
        self.id = id
        self._cognite_client = cast("CogniteClient", cognite_client)


class SecurityCategoryList(CogniteResourceList):
    _RESOURCE = SecurityCategory


class ProjectSpec(CogniteResponse):
    """A cdf project spec
    Args:
        url_name (str): The url name for the project
        groups (List[int]): Group ids in the project
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
        projects (List[ProjectSpec]): Projects this token is valid for.
        capabilities (List[Dict]): Capabilities associated with this token.
    """

    def __init__(self, subject: str, projects: list[ProjectSpec], capabilities: list[dict]):
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
            dumped = {utils._auxiliary.to_camel_case(key): value for key, value in dumped.items()}
        return dumped


class CreatedSession(CogniteResource):
    """session creation related information

    Args:
        id (int): ID of the created session.
        type (str): Credentials kind used to create the session.
        status (str): Current status of the session.
        nonce (str): Nonce to be passed to the internal service that will bind the session
        client_id (str): Client ID in identity provider. Returned only if the session was created using client credentials
    """

    def __init__(
        self,
        id: int | None = None,
        type: str | None = None,
        status: str | None = None,
        nonce: str | None = None,
        client_id: str | None = None,
        cognite_client: CogniteClient | None = None,
    ):
        self.id = id
        self.type = type
        self.status = status
        self.nonce = nonce
        self.client_id = client_id


class CreatedSessionList(CogniteResourceList):
    _RESOURCE = CreatedSession
    _ASSERT_CLASSES = False


class Session(CogniteResource):
    """Session status

    Args:
        id (int): ID of the session.
        type (str): Credentials kind used to create the session.
        status (str): Current status of the session.
        creation_time (int): Session creation time, in milliseconds since 1970
        expiration_time (int): Session expiry time, in milliseconds since 1970. This value is updated on refreshing a token
        client_id (str): Client ID in identity provider. Returned only if the session was created using client credentials
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
    ):
        self.id = id
        self.type = type
        self.status = status
        self.creation_time = creation_time
        self.expiration_time = expiration_time
        self.client_id = client_id


class SessionList(CogniteResourceList):
    _RESOURCE = Session
    _ASSERT_CLASSES = False


class ClientCredentials(CogniteResource):
    """Client credentials for session creation

    Args:
        client_id (str): Client ID from identity provider.
        client_secret (str): Client secret from identity provider.
    """

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
