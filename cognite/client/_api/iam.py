import numbers
import warnings

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    APIKey,
    APIKeyList,
    ClientCredentials,
    CreatedSession,
    Group,
    GroupList,
    SecurityCategory,
    SecurityCategoryList,
    ServiceAccount,
    ServiceAccountList,
    Session,
    SessionList,
)
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    pass


class IAMAPI(APIClient):
    def __init__(self, config, api_version=None, cognite_client=None):
        super().__init__(config, api_version=api_version, cognite_client=cognite_client)
        self.service_accounts = ServiceAccountsAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.api_keys = APIKeysAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.groups = GroupsAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.security_categories = SecurityCategoriesAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.sessions = SessionsAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.token = TokenAPI(config, cognite_client=cognite_client)


class ServiceAccountsAPI(APIClient):
    _RESOURCE_PATH = "/serviceaccounts"

    def list(self):
        return ServiceAccountList._load(self._get(url_path=self._RESOURCE_PATH).json()["items"])

    def create(self, service_account):
        return self._create_multiple(list_cls=ServiceAccountList, resource_cls=ServiceAccount, items=service_account)

    def delete(self, id):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)


class APIKeysAPI(APIClient):
    _RESOURCE_PATH = "/apikeys"

    def list(self, include_deleted=False, all=False, service_account_id=None):
        res = self._get(
            self._RESOURCE_PATH,
            params={"all": all, "serviceAccountId": service_account_id, "includeDeleted": include_deleted},
        )
        return APIKeyList._load(res.json()["items"])

    def create(self, service_account_id):
        if isinstance(service_account_id, numbers.Integral):
            items: Union[(list, dict)] = {"serviceAccountId": service_account_id}
        elif isinstance(service_account_id, Sequence):
            items = [{"serviceAccountId": sa_id} for sa_id in service_account_id]
        else:
            raise TypeError("service_account_id must be of type int or Sequence[int]")
        return self._create_multiple(list_cls=APIKeyList, resource_cls=APIKey, items=items)

    def delete(self, id):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)


class GroupsAPI(APIClient):
    _RESOURCE_PATH = "/groups"

    def list(self, all=False):
        res = self._get(self._RESOURCE_PATH, params={"all": all})
        return GroupList._load(res.json()["items"])

    def create(self, group):
        return self._create_multiple(list_cls=GroupList, resource_cls=Group, items=group)

    def delete(self, id):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)

    def list_service_accounts(self, id):
        resource_path = utils._auxiliary.interpolate_and_url_encode((self._RESOURCE_PATH + "/{}/serviceaccounts"), id)
        return ServiceAccountList._load(self._get(resource_path).json()["items"])

    def add_service_account(self, id, service_account_id):
        resource_path = utils._auxiliary.interpolate_and_url_encode((self._RESOURCE_PATH + "/{}/serviceaccounts"), id)
        all_ids = IdentifierSequence.load(ids=service_account_id, external_ids=None).as_primitives()
        self._post(resource_path, json={"items": all_ids})

    def remove_service_account(self, id, service_account_id):
        url_path = utils._auxiliary.interpolate_and_url_encode((self._RESOURCE_PATH + "/{}/serviceaccounts/remove"), id)
        all_ids = IdentifierSequence.load(ids=service_account_id).as_primitives()
        self._post(url_path, json={"items": all_ids})


class SecurityCategoriesAPI(APIClient):
    _RESOURCE_PATH = "/securitycategories"

    def list(self, limit=25):
        return self._list(list_cls=SecurityCategoryList, resource_cls=SecurityCategory, method="GET", limit=limit)

    def create(self, security_category):
        return self._create_multiple(
            list_cls=SecurityCategoryList, resource_cls=SecurityCategory, items=security_category
        )

    def delete(self, id):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)


class TokenAPI(APIClient):
    def inspect(self):
        if isinstance(self._config.credentials, APIKey):
            warnings.warn(
                "It seems you are trying to reach API endpoint `/token/inspect` which is not valid when authenticating using an API key. Try `client.login.status` instead",
                UserWarning,
                stacklevel=2,
            )
        return TokenInspection._load(self._get("/api/v1/token/inspect").json())


class SessionsAPI(APIClient):
    _LIST_CLASS = SessionList
    _RESOURCE_PATH = "/sessions"

    def __init__(self, config, api_version=None, cognite_client=None):
        super().__init__(config, api_version=api_version, cognite_client=cognite_client)
        self._LIST_LIMIT = 100

    def create(self, client_credentials=None):
        creds = self._config.credentials
        if (client_credentials is None) and isinstance(creds, OAuthClientCredentials):
            client_credentials = ClientCredentials(creds.client_id, creds.client_secret)
        items = {"tokenExchange": True} if (client_credentials is None) else client_credentials.dump(camel_case=True)
        return CreatedSession._load(self._post(self._RESOURCE_PATH, {"items": [items]}).json()["items"][0])

    def revoke(self, id):
        identifiers = IdentifierSequence.load(ids=id, external_ids=None)
        items = {"items": identifiers.as_dicts()}
        return self._LIST_CLASS._load(self._post((self._RESOURCE_PATH + "/revoke"), items).json()["items"])

    def list(self, status=None):
        filter = {"status": status} if status else None
        return self._list(list_cls=self._LIST_CLASS, resource_cls=Session, method="GET", filter=filter)
