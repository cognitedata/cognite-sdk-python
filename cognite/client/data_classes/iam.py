from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Group(CogniteResource):
    """No description.

    Args:
        name (str): Name of the group
        source_id (str): ID of the group in the source. If this is the same ID as a group in the IDP, a service account in that group will implicitly be a part of this group as well.
        capabilities (List[Dict[str, Any]]): No description.
        id (int): No description.
        is_deleted (bool): No description.
        deleted_time (int): No description.
        metadata (Dict[str, Any]): Custom, immutable application specific metadata. String key -> String value. Limits:
        Key are at most 32 bytes. Values are at most 512 bytes. Up to 16 key-value pairs. Total size is at most 4096.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        source_id: Optional[str] = None,
        capabilities: Optional[List[Dict[str, Any]]] = None,
        id: Optional[int] = None,
        is_deleted: Optional[bool] = None,
        deleted_time: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        cognite_client: Optional[CogniteClient] = None,
    ):
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
        name (str): Name of the security category
        id (int): Id of the security category
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self, name: Optional[str] = None, id: Optional[int] = None, cognite_client: Optional[CogniteClient] = None
    ):
        self.name = name
        self.id = id
        self._cognite_client = cast("CogniteClient", cognite_client)


class SecurityCategoryList(CogniteResourceList[SecurityCategory]):
    _RESOURCE = SecurityCategory


class ProjectSpec(CogniteResponse):
    """A CDF project spec

    Args:
        url_name (str): The url name for the project
        groups (List[int]): Group ids in the project
    """

    def __init__(self, url_name: str, groups: List[int]) -> None:
        self.url_name = url_name
        self.groups = groups

    @classmethod
    def _load(cls, api_response: Dict[str, Any]) -> ProjectSpec:
        return cls(url_name=api_response["projectUrlName"], groups=api_response["groups"])


class TokenInspection(CogniteResponse):
    """Current login status

    Args:
        subject (str): Subject (sub claim) of JWT.
        projects (List[ProjectSpec]): Projects this token is valid for.
        capabilities (List[Dict]): Capabilities associated with this token.
    """

    def __init__(self, subject: str, projects: List[ProjectSpec], capabilities: List[Dict]):
        self.subject = subject
        self.projects = projects
        self.capabilities = capabilities

    @classmethod
    def _load(cls, api_response: Dict[str, Any]) -> TokenInspection:
        return cls(
            subject=api_response["subject"],
            projects=[ProjectSpec._load(p) for p in api_response["projects"]],
            capabilities=api_response["capabilities"],
        )

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dumped = {
            "subject": self.subject,
            "projects": [p.dump(camel_case=camel_case) for p in self.projects],
            "capabilities": self.capabilities,
        }
        if camel_case:
            dumped = convert_all_keys_to_camel_case(dumped)
        return dumped


class CreatedSession(CogniteResource):
    """Session creation related information

    Args:
        id (int): ID of the created session.
        type (str): Credentials kind used to create the session.
        status (str): Current status of the session.
        nonce (str): Nonce to be passed to the internal service that will bind the session
        client_id (str): Client ID in identity provider. Returned only if the session was created using client credentials
    """

    def __init__(
        self,
        id: Optional[int] = None,
        type: Optional[str] = None,
        status: Optional[str] = None,
        nonce: Optional[str] = None,
        client_id: Optional[str] = None,
        cognite_client: Optional[CogniteClient] = None,
    ):
        self.id = id
        self.type = type
        self.status = status
        self.nonce = nonce
        self.client_id = client_id


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
        id: Optional[int] = None,
        type: Optional[str] = None,
        status: Optional[str] = None,
        creation_time: Optional[int] = None,
        expiration_time: Optional[int] = None,
        client_id: Optional[str] = None,
        cognite_client: Optional[CogniteClient] = None,
    ):
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

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
