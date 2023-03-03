
from cognite.client import utils
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse
if TYPE_CHECKING:
    from cognite.client import CogniteClient

class ServiceAccount(CogniteResource):

    def __init__(self, name=None, groups=None, id=None, is_deleted=None, deleted_time=None, cognite_client=None):
        self.name = name
        self.groups = groups
        self.id = id
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self._cognite_client = cast('CogniteClient', cognite_client)

class ServiceAccountList(CogniteResourceList):
    _RESOURCE = ServiceAccount

class APIKey(CogniteResource):

    def __init__(self, id=None, service_account_id=None, created_time=None, status=None, value=None, cognite_client=None):
        self.id = id
        self.service_account_id = service_account_id
        self.created_time = created_time
        self.status = status
        self.value = value
        self._cognite_client = cast('CogniteClient', cognite_client)

class APIKeyList(CogniteResourceList):
    _RESOURCE = APIKey

class Group(CogniteResource):

    def __init__(self, name=None, source_id=None, capabilities=None, id=None, is_deleted=None, deleted_time=None, cognite_client=None):
        self.name = name
        self.source_id = source_id
        self.capabilities = capabilities
        self.id = id
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self._cognite_client = cast('CogniteClient', cognite_client)

class GroupList(CogniteResourceList):
    _RESOURCE = Group

class SecurityCategory(CogniteResource):

    def __init__(self, name=None, id=None, cognite_client=None):
        self.name = name
        self.id = id
        self._cognite_client = cast('CogniteClient', cognite_client)

class SecurityCategoryList(CogniteResourceList):
    _RESOURCE = SecurityCategory

class ProjectSpec(CogniteResponse):

    def __init__(self, url_name, groups):
        self.url_name = url_name
        self.groups = groups

    @classmethod
    def _load(cls, api_response):
        return cls(url_name=api_response['projectUrlName'], groups=api_response['groups'])

class TokenInspection(CogniteResponse):

    def __init__(self, subject, projects, capabilities):
        self.subject = subject
        self.projects = projects
        self.capabilities = capabilities

    @classmethod
    def _load(cls, api_response):
        return cls(subject=api_response['subject'], projects=[ProjectSpec._load(p) for p in api_response['projects']], capabilities=api_response['capabilities'])

    def dump(self, camel_case=False):
        dumped = {'subject': self.subject, 'projects': [p.dump(camel_case=camel_case) for p in self.projects], 'capabilities': self.capabilities}
        if camel_case:
            dumped = {utils._auxiliary.to_camel_case(key): value for (key, value) in dumped.items()}
        return dumped

class CreatedSession(CogniteResource):

    def __init__(self, id=None, type=None, status=None, nonce=None, client_id=None, cognite_client=None):
        self.id = id
        self.type = type
        self.status = status
        self.nonce = nonce
        self.client_id = client_id

class Session(CogniteResource):

    def __init__(self, id=None, type=None, status=None, creation_time=None, expiration_time=None, client_id=None, cognite_client=None):
        self.id = id
        self.type = type
        self.status = status
        self.creation_time = creation_time
        self.expiration_time = expiration_time
        self.client_id = client_id

class SessionList(CogniteResourceList):
    _RESOURCE = Session

class ClientCredentials(CogniteResource):

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
