from __future__ import annotations

import warnings

from cognite.client._api_client import APIClient
from cognite.client.credentials import APIKey
from cognite.client.data_classes.login import LoginStatus


class LoginAPI(APIClient):
    _RESOURCE_PATH = "/login"

    def status(self) -> LoginStatus:
        """`Check login status <https://docs.cognite.com/api/v1/#operation/status>`_

        Note:
            This endpoint is only applicable if the client has been authenticated with an api key. If not,
            use `client.iam.token.inspect` instead

        Returns:
            LoginStatus: The login status of the current api key.

        Examples:
            Check the current login status and get the project::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> login_status = c.login.status()
                >>> project = login_status.project

        """
        if not isinstance(self._config.credentials, APIKey):
            warnings.warn(
                "It seems you are trying to reach API endpoint `/login/status` which is only valid when "
                "authenticating using an API key - without an API key. Try `client.iam.token.inspect` instead",
                UserWarning,
                stacklevel=2,
            )
        return LoginStatus._load(self._get(self._RESOURCE_PATH + "/status").json())
