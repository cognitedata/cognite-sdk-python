from typing import *

from cognite.client._base import *


# GenClass: ServiceAccount
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
        name: str = None,
        groups: List[int] = None,
        id: int = None,
        is_deleted: bool = None,
        deleted_time: int = None,
        cognite_client=None,
    ):
        self.name = name
        self.groups = groups
        self.id = id
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self._cognite_client = cognite_client

    # GenStop


class ServiceAccountList(CogniteResourceList):
    _RESOURCE = ServiceAccount
    _ASSERT_CLASSES = False


# GenClass: NewApiKeyResponseDTO
class APIKey(CogniteResource):
    """No description.

    Args:
        id (int): Internal id for the api key
        service_account_id (int): id of the service account
        created_time (int): Time of creating in unix ms
        status (str): The status of the api key.
        value (str): The api key to be used against the API
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        service_account_id: int = None,
        created_time: int = None,
        status: str = None,
        value: str = None,
        cognite_client=None,
    ):
        self.id = id
        self.service_account_id = service_account_id
        self.created_time = created_time
        self.status = status
        self.value = value
        self._cognite_client = cognite_client

    # GenStop


class APIKeyList(CogniteResourceList):
    _RESOURCE = APIKey
    _ASSERT_CLASSES = False


# GenClass: Group
class Group(CogniteResource):
    """No description.

    Args:
        name (str): Name of the group
        source_id (str): ID of the group in the source. If this is the same ID as a group in the IDP, a user in that group will implicitly be a part of this group as well.
        capabilities (List[Dict[str, Any]]): No description.
        id (int): No description.
        is_deleted (bool): No description.
        deleted_time (int): No description.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        source_id: str = None,
        capabilities: List[Dict[str, Any]] = None,
        id: int = None,
        is_deleted: bool = None,
        deleted_time: int = None,
        cognite_client=None,
    ):
        self.name = name
        self.source_id = source_id
        self.capabilities = capabilities
        self.id = id
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self._cognite_client = cognite_client

    # GenStop


class GroupList(CogniteResourceList):
    _RESOURCE = Group
    _ASSERT_CLASSES = False


# GenClass: SecurityCategoryDTO
class SecurityCategory(CogniteResource):
    """No description.

    Args:
        name (str): Name of the security category
        id (int): Id of the security category
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, name: str = None, id: int = None, cognite_client=None):
        self.name = name
        self.id = id
        self._cognite_client = cognite_client

    # GenStop


class SecurityCategoryList(CogniteResourceList):
    _RESOURCE = SecurityCategory
    _ASSERT_CLASSES = False
