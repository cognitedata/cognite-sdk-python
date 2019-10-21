from cognite.client._api_client import APIClient
from cognite.client.data_classes.login import LoginStatus


class LoginAPI(APIClient):
    _RESOURCE_PATH = "/login"

    def status(self) -> LoginStatus:
        """`Check login status <https://docs.cognite.com/api/v1/#operation/status>`_

        Returns:
            LoginStatus: The login status of the current api key.

        Examples:
            Check the current login status and get the project::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> login_status = c.login.status()
                >>> project = login_status.project

        """
        return LoginStatus._load(self._get(self._RESOURCE_PATH + "/status").json())
