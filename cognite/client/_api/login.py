
import warnings
from cognite.client._api_client import APIClient
from cognite.client.credentials import APIKey
from cognite.client.data_classes.login import LoginStatus

class LoginAPI(APIClient):
    _RESOURCE_PATH = '/login'

    def status(self):
        if (not isinstance(self._config.credentials, APIKey)):
            warnings.warn('It seems you are trying to reach API endpoint `/login/status` which is only valid when authenticating using an API key - without an API key. Try `client.iam.token.inspect` instead', UserWarning, stacklevel=2)
        return LoginStatus._load(self._get((self._RESOURCE_PATH + '/status')).json())
